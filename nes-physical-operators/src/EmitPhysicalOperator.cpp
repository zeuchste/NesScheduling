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

#include <EmitPhysicalOperator.hpp>

#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/NESStrongTypeRef.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <nautilus/val.hpp>
#include <EmitOperatorHandler.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <OperatorState.hpp>
#include <PhysicalOperator.hpp>
#include <PipelineExecutionContext.hpp>
#include <function.hpp>
#include <val_ptr.hpp>

namespace NES
{

class EmitState : public OperatorState
{
public:
    EmitState(const RecordBuffer& resultBuffer, const nautilus::val<uint64_t>& targetBytes)
        : resultBuffer(resultBuffer), bufferMemoryArea(resultBuffer.getMemArea()), targetBytes(targetBytes)
    {
    }

    nautilus::val<uint64_t> outputIndex = 0;
    RecordBuffer resultBuffer;
    nautilus::val<int8_t*> bufferMemoryArea;
    /// #1711: byte size used when (re)allocating this pipeline's output buffer, derived from the input cardinality.
    nautilus::val<uint64_t> targetBytes;
};

namespace
{
/// #1711 ReuseAcrossRuns: resume into the partial output buffer this worker thread carried from the previous invocation.
/// Returns the carried buffer pinned for this pipeline invocation (so it lives like a freshly allocated one), or nullptr
/// when nothing was carried. The carried record count is stored as the buffer's numberOfTuples (set when stashing), so
/// the operator recovers the resume index via RecordBuffer::getNumRecords().
nautilus::val<TupleBuffer*> resumeCarriedBuffer(const ExecutionContext& ctx, OperatorHandlerId operatorHandlerId)
{
    return nautilus::invoke(
        +[](OperatorHandler* handler, PipelineExecutionContext* pec, WorkerThreadId workerThreadId) -> TupleBuffer*
        {
            PRECONDITION(handler != nullptr, "Expects a valid handler");
            PRECONDITION(pec != nullptr, "Expects a valid pipeline execution context");
            auto carried = dynamic_cast<EmitOperatorHandler&>(*handler).takeCarriedBuffer(workerThreadId);
            if (!carried.has_value())
            {
                return nullptr;
            }
            /// numberOfTuples already records the carried record count; pin so the buffer outlives the handler hand-off.
            carried->buffer.setNumberOfTuples(carried->numRecords);
            return std::addressof(pec->pinBuffer(std::move(carried->buffer)));
        },
        ctx.getGlobalOperatorHandler(operatorHandlerId),
        ctx.pipelineContext,
        ctx.workerThreadId);
}
}

void EmitPhysicalOperator::open(ExecutionContext& ctx, RecordBuffer& inputRecordBuffer) const
{
    /// #1711: the allocation strategy is fixed per operator (compile-time), so exactly one branch is specialised into
    /// the generated code. InputSized sizes the output buffer DOWN to the input cardinality (capped at the operator
    /// buffer size; with size classes enabled getBuffer() serves the smallest fitting class). ReuseAcrossRuns resumes
    /// into the partial buffer carried from the previous invocation (see below). EagerFull and StageAndCopy stage in a
    /// full-size buffer (StageAndCopy right-sizes it at the final flush in close()). Output buffer under-estimates are
    /// handled by the flush-on-full path in execute().
    if (mode == EmitBufferAllocationMode::InputSized)
    {
        nautilus::val<uint64_t> targetBytes = bufferRef->getBufferSize();
        const auto wanted = inputRecordBuffer.getNumRecords() * bufferRef->getTupleSize();
        if (wanted < targetBytes)
        {
            targetBytes = wanted;
        }
        const auto resultBufferRef = ctx.allocateBuffer(targetBytes);
        auto emitState = std::make_unique<EmitState>(RecordBuffer(resultBufferRef), targetBytes);
        ctx.setLocalOperatorState(id, std::move(emitState));
    }
    else if (mode == EmitBufferAllocationMode::ReuseAcrossRuns)
    {
        /// #1711 ReuseAcrossRuns: if this worker carried a partial buffer from a previous invocation, resume filling it;
        /// otherwise allocate a fresh full-size buffer. Carrying amortises allocation across the chunks of one input
        /// sequence. The carried buffer's numberOfTuples encodes the resume index. close() only carries within a
        /// sequence; it flushes (and clears the carry) at the sequence boundary, so a resumed buffer always shares the
        /// current input's sequence number -- this keeps the per-sequence completeness contract intact.
        const auto carriedRef = resumeCarriedBuffer(ctx, operatorHandlerId);
        if (carriedRef != nullptr)
        {
            const RecordBuffer carried(carriedRef);
            auto emitState = std::make_unique<EmitState>(carried, bufferRef->getBufferSize());
            emitState->outputIndex = carried.getNumRecords();
            ctx.setLocalOperatorState(id, std::move(emitState));
        }
        else
        {
            const auto resultBufferRef = ctx.allocateBuffer();
            auto emitState = std::make_unique<EmitState>(RecordBuffer(resultBufferRef), bufferRef->getBufferSize());
            ctx.setLocalOperatorState(id, std::move(emitState));
        }
    }
    else
    {
        const auto resultBufferRef = ctx.allocateBuffer();
        auto emitState = std::make_unique<EmitState>(RecordBuffer(resultBufferRef), bufferRef->getBufferSize());
        ctx.setLocalOperatorState(id, std::move(emitState));
    }
}

void EmitPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    auto* const emitState = dynamic_cast<EmitState*>(ctx.getLocalState(id));

    /// We need to first check if the buffer has to be emitted and then write to it. Otherwise, it can happen that we will
    /// emit a tuple twice. Once in the execute() and then again in close(). This happens only for buffers that are filled
    /// to the brim, i.e., have no more space left.
    auto writeResult
        = bufferRef->writeRecord(emitState->outputIndex, emitState->resultBuffer, record, ctx.pipelineMemoryProvider.bufferProvider);
    /// An unsuccessful writeResult means, that the current record buffer is filled up completely and needs to be emitted first.
    /// We emit and create a new record buffer
    if (!writeResult.successful)
    {
        emitRecordBuffer(ctx, emitState->resultBuffer, emitState->outputIndex, false);
        const auto sized = (mode == EmitBufferAllocationMode::InputSized);
        const auto resultBufferRef = sized ? ctx.allocateBuffer(emitState->targetBytes) : ctx.allocateBuffer();
        emitState->resultBuffer = RecordBuffer(resultBufferRef);
        emitState->bufferMemoryArea = emitState->resultBuffer.getMemArea();
        emitState->outputIndex = 0_u64;

        /// This write record call should succeed since a newly allocated tuple buffer should be able to store at least one record
        writeResult
            = bufferRef->writeRecord(emitState->outputIndex, emitState->resultBuffer, record, ctx.pipelineMemoryProvider.bufferProvider);
    }
    emitState->outputIndex = emitState->outputIndex + writeResult.writtenRecords;
}

namespace
{
/// #1711 ReuseAcrossRuns: hand the still-partial output buffer back to the handler, keyed by worker thread, so the next
/// invocation on this thread resumes into it instead of allocating. The buffer's metadata (origin/seq/watermark/chunk)
/// is stamped before this call so a query-end flush in stop() can emit it directly; numberOfTuples carries the index.
void stashCarriedBuffer(
    const ExecutionContext& ctx,
    OperatorHandlerId operatorHandlerId,
    const nautilus::val<TupleBuffer*>& buffer,
    const nautilus::val<uint64_t>& numRecords)
{
    nautilus::invoke(
        +[](OperatorHandler* handler, TupleBuffer* carried, WorkerThreadId workerThreadId, uint64_t records)
        {
            PRECONDITION(handler != nullptr, "Expects a valid handler");
            PRECONDITION(carried != nullptr, "Expects a valid buffer");
            auto& emitHandler = dynamic_cast<EmitOperatorHandler&>(*handler);
            /// Account for this consumed (not-last) input chunk so the eventual final flush completes the sequence.
            emitHandler.registerCarriedInputChunk(carried->getSequenceNumber(), carried->getOriginId());
            emitHandler.storeCarriedBuffer(workerThreadId, TupleBuffer(*carried), records);
        },
        ctx.getGlobalOperatorHandler(operatorHandlerId),
        buffer,
        ctx.workerThreadId,
        numRecords);
}
}

void EmitPhysicalOperator::close(ExecutionContext& ctx, RecordBuffer&) const
{
    auto* const emitState = dynamic_cast<EmitState*>(ctx.getLocalState(id));
    if (mode == EmitBufferAllocationMode::StageAndCopy)
    {
        /// #1711: the staging buffer is full-size; copy only the written records (and their var-sized children) into a
        /// right-sized buffer so a partially-filled final flush pins an exactly-sized buffer downstream.
        /// `outputIndex` is incremented by writeRecord's `writtenRecords`, whose unit depends on the buffer ref: for
        /// fixed-layout tuples it is a record count (so used main-buffer bytes = outputIndex * tupleSize), but for the
        /// output formatter (OutputFormatterBufferRef) tupleSize is a 0 placeholder and `outputIndex` already counts the
        /// bytes written into the main buffer (the formatted text). Picking `outputIndex` directly when tupleSize == 0
        /// avoids multiplying by 0, which previously copied 0 bytes and emitted a debug-filled right-sized buffer.
        const auto tupleSize = bufferRef->getTupleSize();
        const auto usedBytes = (tupleSize == 0) ? emitState->outputIndex : emitState->outputIndex * tupleSize;
        const auto rightSizedRef = ctx.copyToRightSizedBuffer(emitState->resultBuffer.getReference(), usedBytes);
        RecordBuffer rightSized(rightSizedRef);
        emitRecordBuffer(ctx, rightSized, emitState->outputIndex, true);
        return;
    }
    if (mode == EmitBufferAllocationMode::ReuseAcrossRuns && !ctx.lastChunk)
    {
        /// #1711 ReuseAcrossRuns: this input buffer is NOT the last chunk of its sequence, so more chunks of the same
        /// sequence (sharing this sequence number) are still to come. Rather than emit a (possibly tiny) partial buffer
        /// now, stamp the current metadata and carry the partial buffer to the next invocation on this worker thread,
        /// amortising the allocation. Flush still happens at the sequence boundary (the fall-through emit below) or, as a
        /// safety net for buffers outstanding at query end, in EmitOperatorHandler::stop(). We stamp the latest ctx
        /// metadata (watermark/seq/origin/chunk) onto the carried buffer; this is sound because the carry never crosses
        /// a sequence boundary, so the sequence number is invariant across the resumes that fill this buffer.
        emitState->resultBuffer.setNumRecords(emitState->outputIndex);
        emitState->resultBuffer.setWatermarkTs(ctx.watermarkTs);
        emitState->resultBuffer.setOriginId(ctx.originId);
        emitState->resultBuffer.setSequenceNumber(ctx.sequenceNumber);
        emitState->resultBuffer.setCreationTs(ctx.currentTs);
        stashCarriedBuffer(ctx, operatorHandlerId, emitState->resultBuffer.getReference(), emitState->outputIndex);
        return;
    }
    /// emit current buffer and set the metadata
    emitRecordBuffer(ctx, emitState->resultBuffer, emitState->outputIndex, true);
}

namespace
{
void setChunkNumber(
    const ExecutionContext& context,
    OperatorHandlerId operatorHandlerId,
    const nautilus::val<bool>& closesChunk,
    const nautilus::val<ChunkNumber>& currentChunkNumber,
    const nautilus::val<bool>& isCurrentBufferTheLastChunk,
    const nautilus::val<TupleBuffer*>& newBuffer)
{
    nautilus::invoke(
        +[](OperatorHandler* handler,
            bool closesChunk,
            ChunkNumber currentChunkNumber,
            bool isCurrentBufferTheLastChunk,
            TupleBuffer* newBuffer)
        {
            PRECONDITION(handler != nullptr, "Expects a valid handler");
            PRECONDITION(newBuffer != nullptr, "Expects a valid buffer");
            PRECONDITION(currentChunkNumber != INVALID<ChunkNumber>, "Expects a valid chunkNumber");

            dynamic_cast<EmitOperatorHandler&>(*handler).setChunkNumber(
                closesChunk, currentChunkNumber, isCurrentBufferTheLastChunk, *newBuffer);
        },
        context.getGlobalOperatorHandler(operatorHandlerId),
        closesChunk,
        currentChunkNumber,
        isCurrentBufferTheLastChunk,
        newBuffer);
}
}

void EmitPhysicalOperator::emitRecordBuffer(
    ExecutionContext& ctx,
    RecordBuffer& recordBuffer,
    const nautilus::val<uint64_t>& numRecords,
    const nautilus::val<bool>& potentialLastChunk) const
{
    recordBuffer.setNumRecords(numRecords);
    recordBuffer.setWatermarkTs(ctx.watermarkTs);
    recordBuffer.setOriginId(ctx.originId);
    recordBuffer.setSequenceNumber(ctx.sequenceNumber);
    recordBuffer.setCreationTs(ctx.currentTs);

    setChunkNumber(ctx, operatorHandlerId, potentialLastChunk, ctx.chunkNumber, ctx.lastChunk, recordBuffer.getReference());

    ctx.emitBuffer(recordBuffer);
}

namespace
{
void startHandlerProxy(OperatorHandler* handler, PipelineExecutionContext* pec)
{
    PRECONDITION(handler != nullptr, "Expects a valid handler");
    PRECONDITION(pec != nullptr, "Expects a valid pipeline execution context");
    handler->start(*pec, 0);
}

void stopHandlerProxy(OperatorHandler* handler, PipelineExecutionContext* pec)
{
    PRECONDITION(handler != nullptr, "Expects a valid handler");
    PRECONDITION(pec != nullptr, "Expects a valid pipeline execution context");
    /// #1711 ReuseAcrossRuns: this triggers EmitOperatorHandler::stop(), which flushes every partial output buffer still
    /// carried by a worker thread so no records are stranded at query end. A no-op for the other emit modes.
    handler->stop(QueryTerminationType::Graceful, *pec);
}
}

void EmitPhysicalOperator::setup(ExecutionContext& ctx, CompilationContext&) const
{
    nautilus::invoke(startHandlerProxy, ctx.getGlobalOperatorHandler(operatorHandlerId), ctx.pipelineContext);
}

void EmitPhysicalOperator::terminate(ExecutionContext& ctx) const
{
    nautilus::invoke(stopHandlerProxy, ctx.getGlobalOperatorHandler(operatorHandlerId), ctx.pipelineContext);
}

EmitPhysicalOperator::EmitPhysicalOperator(
    OperatorHandlerId operatorHandlerId, std::shared_ptr<TupleBufferRef> memoryProvider, EmitBufferAllocationMode mode)
    : bufferRef(std::move(memoryProvider)), operatorHandlerId(operatorHandlerId), mode(mode)
{
}

[[nodiscard]] uint64_t EmitPhysicalOperator::getMaxRecordsPerBuffer() const
{
    return bufferRef->getCapacity();
}

std::optional<PhysicalOperator> EmitPhysicalOperator::getChild() const
{
    return child;
}

void EmitPhysicalOperator::setChild(PhysicalOperator child)
{
    this->child = std::move(child);
}

}
