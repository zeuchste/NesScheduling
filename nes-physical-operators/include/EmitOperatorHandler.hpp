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

#pragma once
#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <ostream>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Formatter.hpp>
#include <folly/Synchronized.h>

#ifndef NO_ASSERT
    #include <set>
#endif

namespace NES
{

/// Stores a sequenceNumber and an OriginId
struct SequenceNumberForOriginId
{
    SequenceNumber sequenceNumber = INVALID_SEQ_NUMBER;
    OriginId originId = INVALID_ORIGIN_ID;

    auto operator<=>(const SequenceNumberForOriginId&) const = default;

    friend std::ostream& operator<<(std::ostream& os, const SequenceNumberForOriginId& obj)
    {
        return os << "{ seqNumber = " << obj.sequenceNumber << ", originId = " << obj.originId << "}";
    }
};

/// Container for storing information, related to the state of a sequence number
/// the SequenceState is only used inside 'folly::Synchronized' so its members do not need to be atomic themselves
struct SequenceState
{
    ChunkNumber::Underlying nextChunkNumberCounter = ChunkNumber::INITIAL;
    ChunkNumber lastChunkNumber = INVALID<ChunkNumber>;
    size_t seenChunks = 0;
};

/// #1711 ReuseAcrossRuns: a partially-filled output buffer carried by one worker thread from one pipeline invocation to
/// the next. Holding the TupleBuffer by value keeps it pinned/alive across invocations. `numRecords` is the number of
/// records written so far; the metadata (origin/seq/watermark/chunk) is already stamped on the buffer by the close()
/// that stashed it, so a flush at query end only needs to set numTuples and emit.
struct CarriedOutputBuffer
{
    TupleBuffer buffer;
    uint64_t numRecords = 0;
};

class EmitOperatorHandler final : public OperatorHandler
{
public:
    void setChunkNumber(bool isEndOfIncomingChunk, ChunkNumber incomingChunkNumber, bool isIncomingBufferTheLastChunk, TupleBuffer& buffer);

    /// #1711 ReuseAcrossRuns: record that one not-last input chunk of (sequenceNumber, originId) was consumed into a
    /// carried output buffer without emitting. Keeps the per-sequence chunk-completeness accounting correct so the
    /// single final flush still marks the sequence complete. No-op effect on chunk numbering of emitted buffers.
    void registerCarriedInputChunk(SequenceNumber sequenceNumber, OriginId originId);

    void start(PipelineExecutionContext& pipelineExecutionContext, uint32_t localStateVariableId) override;
    void stop(QueryTerminationType terminationType, PipelineExecutionContext& pipelineExecutionContext) override;

    /// #1711 ReuseAcrossRuns: claim (and remove) the partial output buffer carried by `workerThreadId`, if any. Called
    /// from the emit operator's open() so a worker resumes filling its previously-carried buffer instead of allocating
    /// a fresh one. The buffer is moved out so the handler no longer pins it while the pipeline owns it.
    [[nodiscard]] std::optional<CarriedOutputBuffer> takeCarriedBuffer(WorkerThreadId workerThreadId);

    /// #1711 ReuseAcrossRuns: stash a still-partial output buffer (with its current numRecords) for `workerThreadId` so
    /// the next invocation on that thread resumes into it. The caller must have stamped the buffer metadata already.
    void storeCarriedBuffer(WorkerThreadId workerThreadId, TupleBuffer&& buffer, uint64_t numRecords);

    folly::Synchronized<std::map<SequenceNumberForOriginId, SequenceState>> sequenceStates;

    /// #1711 ReuseAcrossRuns: one carried partial output buffer per worker thread (keyed by WorkerThreadId). Empty for
    /// every other emit mode. Flushed in stop() so no records are stranded at query end.
    folly::Synchronized<std::map<WorkerThreadId, CarriedOutputBuffer>> carriedBuffers;

#ifndef NO_ASSERT
    /// We assume that every tuple of (SequenceNumber, ChunkNumber, OriginId) is unique per query.
    /// In debug mode we track the completed sequence numbers to catch bugs related to bad sequence/chunk numbers
    folly::Synchronized<std::set<SequenceNumberForOriginId>> completedSequences;
#endif
};
}

FMT_OSTREAM(NES::SequenceNumberForOriginId);
