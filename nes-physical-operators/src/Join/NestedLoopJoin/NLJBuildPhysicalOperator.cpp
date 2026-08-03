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
#include <Join/NestedLoopJoin/NLJBuildPhysicalOperator.hpp>

#include <memory>
#include <optional>
#include <utility>
#include <vector>
#include <DataTypes/VarVal.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Join/NestedLoopJoin/NLJSlice.hpp>
#include <Join/StreamJoinBuildPhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/Slice.hpp>
#include <Time/Timestamp.hpp>
#include <Watermark/TimeFunction.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <WindowBuildPhysicalOperator.hpp>
#include <function.hpp>
#include <static.hpp>
#include <val_enum.hpp>
#include <val_ptr.hpp>

namespace NES
{
SliceStart getNLJSliceStartProxy(const NLJSlice* nljSlice)
{
    PRECONDITION(nljSlice != nullptr, "nlj slice pointer should not be null!");
    return nljSlice->getSliceStart();
}

SliceEnd getNLJSliceEndProxy(const NLJSlice* nljSlice)
{
    PRECONDITION(nljSlice != nullptr, "nlj slice pointer should not be null!");
    return nljSlice->getSliceEnd();
}

NLJBuildPhysicalOperator::NLJBuildPhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    const JoinBuildSideType joinBuildSide,
    std::unique_ptr<TimeFunction> timeFunction,
    std::shared_ptr<PagedVectorTupleLayout> tupleLayout,
    std::unique_ptr<SliceStoreRef> sliceStoreRef,
    std::optional<NLJEagerBuild> eager)
    : StreamJoinBuildPhysicalOperator{
          operatorHandlerId, joinBuildSide, std::move(timeFunction), std::move(tupleLayout), std::move(sliceStoreRef)}
    , eager(std::move(eager))
{
}

void NLJBuildPhysicalOperator::execute(ExecutionContext& executionCtx, Record& record) const
{
    /// Getting the operator handler from the local state
    auto* const localState = dynamic_cast<WindowOperatorBuildLocalState*>(executionCtx.getLocalState(id));
    auto operatorHandler = localState->getOperatorHandler();

    /// Get the current slice / pagedVector that we have to insert the tuple into
    const auto timestamp = timeFunction->getTs(executionCtx, record);
    auto nljPagedVectorBuffer = sliceStoreRef->getDataStructureRef(
        timestamp, executionCtx.workerThreadId, operatorHandler, executionCtx.pipelineMemoryProvider.bufferProvider);
    /// Write record to the pagedVector
    PagedVectorRef pagedVectorRef{BorrowedNautilusBuffer::from(nljPagedVectorBuffer.asArg()), tupleLayout};

    if (not eager)
    {
        pagedVectorRef.pushBack(record, executionCtx.pipelineMemoryProvider.bufferProvider);
        return;
    }

    /// Eager trigger (T2): the slice-level structures need the owning slice, resolved through the
    /// handler's per-worker cache. Positions are packed (worker, position-in-own-vector) and translate
    /// into combined-vector positions at trigger time.
    const auto sliceRef = nautilus::invoke(
        +[](OperatorHandler* handler, const Timestamp ts, const WorkerThreadId workerThreadId) -> NLJSlice*
        { return dynamic_cast<NLJOperatorHandler*>(handler)->sliceForEagerInsert(ts, workerThreadId); },
        executionCtx.getGlobalOperatorHandler(operatorHandlerId),
        timestamp,
        executionCtx.workerThreadId);
    const nautilus::val<uint64_t> ownSide{joinBuildSide == JoinBuildSideType::Right ? uint64_t{1} : uint64_t{0}};

    if (eager->mode == NLJEagerBuild::Mode::RHJ_RUNS)
    {
        /// Per-run trigger: append, hash into the shared run; full runs seal and probe inside the invoke.
        const auto position = pagedVectorRef.getNumberOfRecords();
        pagedVectorRef.pushBack(record, executionCtx.pipelineMemoryProvider.bufferProvider);
        std::vector<VarVal> keyValues;
        for (nautilus::static_val<uint64_t> i = 0; i < eager->keyFieldNames.size(); ++i)
        {
            keyValues.emplace_back(record.read(eager->keyFieldNames[i]));
        }
        const auto hash = eager->hashFunction->calculate(keyValues);
        nautilus::invoke(
            +[](NLJSlice* slice, const uint64_t side, const uint64_t hash, const uint64_t position, const WorkerThreadId worker) -> void
            { slice->eagerRunInsert(side, hash, position, worker); },
            sliceRef,
            ownSide,
            hash,
            position,
            executionCtx.workerThreadId);
        return;
    }

    /// NLJ_SCAN: under the slice-global lock, append the own tuple and scan the strictly-earlier
    /// opposite tuples; predicate-verified pairs are stored and drained at trigger time.
    nautilus::invoke(+[](NLJSlice* slice) -> void { slice->lockEager(); }, sliceRef);
    const auto ownPosition = pagedVectorRef.getNumberOfRecords();
    pagedVectorRef.pushBack(record, executionCtx.pipelineMemoryProvider.bufferProvider);
    const auto oppositeSide = joinBuildSide == JoinBuildSideType::Left ? JoinBuildSideType::Right : JoinBuildSideType::Left;
    const auto numWorkers = nautilus::invoke(+[](const NLJSlice* slice) -> uint64_t { return slice->workerCount(); }, sliceRef);
    for (nautilus::val<uint64_t> oppWorker = 0; oppWorker < numWorkers; ++oppWorker)
    {
        const auto oppBufferRef = nautilus::invoke(
            +[](const NLJSlice* slice, const uint64_t worker, const JoinBuildSideType side) -> const TupleBuffer*
            { return slice->getPagedVectorTupleBufferRef(WorkerThreadId(worker), side); },
            sliceRef,
            oppWorker,
            nautilus::val<JoinBuildSideType>(oppositeSide));
        const PagedVectorRef oppVector(BorrowedNautilusBuffer::from(oppBufferRef), eager->otherTupleLayout);
        nautilus::val<uint64_t> oppPosition = 0;
        for (auto it = oppVector.begin(); it != oppVector.end(); ++it)
        {
            const auto oppRecord = *it;
            Record keyRecord;
            for (const auto& fieldName : nautilus::static_iterable(eager->ownKeyFieldNames))
            {
                keyRecord.write(fieldName, record.read(fieldName));
            }
            for (const auto& fieldName : nautilus::static_iterable(eager->otherKeyFieldNames))
            {
                keyRecord.write(fieldName, oppRecord.read(fieldName));
            }
            if (eager->joinFunction->execute(keyRecord, executionCtx.pipelineMemoryProvider.arena))
            {
                nautilus::invoke(
                    +[](NLJSlice* slice,
                        const WorkerThreadId workerId,
                        const uint64_t side,
                        const uint64_t ownPos,
                        const uint64_t oppWorker,
                        const uint64_t oppPos) -> void { slice->appendEagerOriented(workerId, side, ownPos, oppWorker, oppPos); },
                    sliceRef,
                    executionCtx.workerThreadId,
                    ownSide,
                    ownPosition,
                    oppWorker,
                    oppPosition);
            }
            ++oppPosition;
        }
    }
    nautilus::invoke(+[](NLJSlice* slice) -> void { slice->unlockEager(); }, sliceRef);
}
}
