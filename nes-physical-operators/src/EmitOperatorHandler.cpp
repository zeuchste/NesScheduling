/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <EmitOperatorHandler.hpp>

#include <cstdint>
#include <optional>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

void EmitOperatorHandler::setChunkNumber(
    bool isEndOfIncomingChunk, ChunkNumber incomingChunkNumber, bool isIncomingBufferTheLastChunk, TupleBuffer& buffer)
{
    /// The SequenceState tracks how many chunks have been seen per Sequence/OriginId pair.
    /// This allows to reason about completeness of a SequenceNumber and assign unique ChunkNumbers to every produced buffer aswell
    /// as assigning exactly one LastChunk flag once we have seen all chunks.

    SequenceNumberForOriginId seqNumberOriginId(buffer.getSequenceNumber(), buffer.getOriginId());
    const auto lock = sequenceStates.wlock();

    /// Assign a unique chunk number to the buffer and increment the internal counter
    auto& [nextChunkNumberCounter, lastChunkNumber, seenChunks] = (*lock)[seqNumberOriginId];

#ifndef NO_ASSERT
    const auto completedSequencesLock = completedSequences.wlock();
    INVARIANT(
        !completedSequencesLock->contains(seqNumberOriginId),
        "Received chunk for sequence {} that was already completed",
        seqNumberOriginId);
#endif


    const auto nextChunkNumber = ChunkNumber(nextChunkNumberCounter);
    nextChunkNumberCounter += 1;

    buffer.setChunkNumber(nextChunkNumber);
    buffer.setLastChunk(false);

    /// Check for completeness of the sequence number. This is only relevant if this buffer is actually completing an incoming chunk.
    if (isEndOfIncomingChunk)
    {
        /// As soon as the incomingBufferLastChunk is received we know how many incoming chunks to expect.
        if (isIncomingBufferTheLastChunk)
        {
            INVARIANT(
                lastChunkNumber == INVALID<ChunkNumber>,
                "Received multiple last chunks for {}. Previous last chunk was {}",
                seqNumberOriginId,
                lastChunkNumber);
            lastChunkNumber = incomingChunkNumber;
        }
        seenChunks++;

        /// We have processed the expected number of chunks and this buffer is closing the input tuple buffer, so we assign the last flag
        /// and erase the sequenceState.
        if (lastChunkNumber != INVALID<ChunkNumber> && seenChunks - 1 == lastChunkNumber.getRawValue() - ChunkNumber::INITIAL)
        {
            lock->erase(seqNumberOriginId);
#ifndef NO_ASSERT
            completedSequencesLock->emplace(seqNumberOriginId);
#endif
            buffer.setLastChunk(true);
        }
    }
}

void EmitOperatorHandler::registerCarriedInputChunk(SequenceNumber sequenceNumber, OriginId originId)
{
    /// #1711 ReuseAcrossRuns: when a not-last input chunk is carried (its output is buffered, not emitted), the normal
    /// per-chunk completeness bump in setChunkNumber is skipped. Register the consumed input chunk here so the final
    /// flush -- which calls setChunkNumber exactly once for the whole sequence -- still completes the sequence: seenChunks
    /// reaches the input chunk count, matching (lastChunkNumber - INITIAL). We intentionally do NOT advance
    /// nextChunkNumberCounter, so the single emitted output buffer keeps chunk number INITIAL.
    const auto lock = sequenceStates.wlock();
    (*lock)[SequenceNumberForOriginId(sequenceNumber, originId)].seenChunks++;
}

void EmitOperatorHandler::start(PipelineExecutionContext&, uint32_t)
{
}

void EmitOperatorHandler::stop(QueryTerminationType, PipelineExecutionContext& pipelineExecutionContext)
{
    /// #1711 ReuseAcrossRuns: flush every partial output buffer that is still carried by a worker thread, so the records
    /// buffered across invocations are not stranded at query end. Each buffer already carries the metadata stamped by
    /// the close() that stashed it (origin/seq/watermark/chunk and lastChunk); we only need to publish numTuples and
    /// emit. For every other emit mode this map is empty, so this is a no-op.
    const auto lock = carriedBuffers.wlock();
    for (auto& [workerThreadId, carried] : *lock)
    {
        if (carried.numRecords == 0)
        {
            continue;
        }
        carried.buffer.setNumberOfTuples(carried.numRecords);
        pipelineExecutionContext.emitBuffer(carried.buffer, PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
    }
    lock->clear();
}

std::optional<CarriedOutputBuffer> EmitOperatorHandler::takeCarriedBuffer(const WorkerThreadId workerThreadId)
{
    const auto lock = carriedBuffers.wlock();
    const auto it = lock->find(workerThreadId);
    if (it == lock->end())
    {
        return std::nullopt;
    }
    auto carried = std::move(it->second);
    lock->erase(it);
    return carried;
}

void EmitOperatorHandler::storeCarriedBuffer(const WorkerThreadId workerThreadId, TupleBuffer&& buffer, const uint64_t numRecords)
{
    const auto lock = carriedBuffers.wlock();
    (*lock)[workerThreadId] = CarriedOutputBuffer{.buffer = std::move(buffer), .numRecords = numRecords};
}

}
