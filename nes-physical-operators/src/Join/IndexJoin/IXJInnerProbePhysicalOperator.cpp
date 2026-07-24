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

#include <Join/IndexJoin/IXJInnerProbePhysicalOperator.hpp>

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>
#include <DataTypes/VarVal.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/NESStrongTypeRef.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Interface/TimestampRef.hpp>
#include <Join/IndexJoin/IXJOperatorHandler.hpp>
#include <Join/IndexJoin/IXJSlice.hpp>
#include <Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Join/StreamJoinProbePhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Operators/Windows/WindowMetaData.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Time/Timestamp.hpp>
#include <nautilus/val_enum.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <function.hpp>
#include <static.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>

namespace NES
{

namespace
{
IXJSlice* getIXJSliceRefFromEndProxy(OperatorHandler* ptrOpHandler, const SliceEnd sliceEnd)
{
    PRECONDITION(ptrOpHandler != nullptr, "op handler context should not be null");
    const auto* opHandler = dynamic_cast<IXJOperatorHandler*>(ptrOpHandler);
    auto slice = opHandler->getSliceAndWindowStore().getSliceBySliceEnd(sliceEnd);
    INVARIANT(slice.has_value(), "Could not find a slice for slice end {}", sliceEnd);
    return dynamic_cast<IXJSlice*>(slice.value().get());
}

TupleBuffer* getVectorBufferProxy(IXJSlice* slice, const uint64_t worker, const JoinBuildSideType side)
{
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast): the probe only reads from the vector
    return const_cast<TupleBuffer*>(slice->getPagedVectorTupleBufferRef(WorkerThreadId(worker), side));
}
}

IXJInnerProbePhysicalOperator::IXJInnerProbePhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    PhysicalFunction joinFunction,
    WindowMetaData windowMetaData,
    const JoinSchema& joinSchema,
    std::shared_ptr<PagedVectorTupleLayout> leftTupleLayout,
    std::shared_ptr<PagedVectorTupleLayout> rightTupleLayout,
    std::vector<Record::RecordFieldIdentifier> leftKeyFieldNames,
    std::vector<Record::RecordFieldIdentifier> rightKeyFieldNames,
    std::shared_ptr<HashFunction> hashFunction)
    : StreamJoinProbePhysicalOperator(operatorHandlerId, std::move(joinFunction), std::move(windowMetaData), joinSchema)
    , leftTupleLayout(std::move(leftTupleLayout))
    , rightTupleLayout(std::move(rightTupleLayout))
    , leftKeyFieldNames(std::move(leftKeyFieldNames))
    , rightKeyFieldNames(std::move(rightKeyFieldNames))
    , hashFunction(std::move(hashFunction))
{
}

void IXJInnerProbePhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    StreamJoinProbePhysicalOperator::open(executionCtx, recordBuffer);

    /// Parse trigger buffer — for inner join, always 1 left + 1 right slice end
    const auto triggerRef = static_cast<nautilus::val<EmittedNLJWindowTrigger*>>(recordBuffer.getMemArea());
    const auto windowInfoRef = getMemberRef(triggerRef, &EmittedNLJWindowTrigger::windowInfo);
    const auto windowStart = nautilus::val<Timestamp>{readValueFromMemRef<uint64_t>(getMemberRef(windowInfoRef, &WindowInfo::windowStart))};
    const auto windowEnd = nautilus::val<Timestamp>{readValueFromMemRef<uint64_t>(getMemberRef(windowInfoRef, &WindowInfo::windowEnd))};

    auto leftSliceEndsPtr = readValueFromMemRef<SliceEnd::Underlying*>(getMemberRef(triggerRef, &EmittedNLJWindowTrigger::leftSliceEnds));
    auto rightSliceEndsPtr = readValueFromMemRef<SliceEnd::Underlying*>(getMemberRef(triggerRef, &EmittedNLJWindowTrigger::rightSliceEnds));
    const nautilus::val<SliceEnd> sliceIdLeft{leftSliceEndsPtr[0]};
    const nautilus::val<SliceEnd> sliceIdRight{rightSliceEndsPtr[0]};
    const auto rangeIndex = readValueFromMemRef<uint64_t>(getMemberRef(triggerRef, &EmittedNLJWindowTrigger::rangeIndex));
    const auto rangeCount = readValueFromMemRef<uint64_t>(getMemberRef(triggerRef, &EmittedNLJWindowTrigger::rangeCount));

    const auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    const auto leftSliceRef = invoke(getIXJSliceRefFromEndProxy, operatorHandlerMemRef, sliceIdLeft);
    const auto rightSliceRef = invoke(getIXJSliceRefFromEndProxy, operatorHandlerMemRef, sliceIdRight);

    const auto leftFields = getOrderedFieldNames(leftTupleLayout->getSchema());
    const auto rightFields = getOrderedFieldNames(rightTupleLayout->getSchema());

    /// Iterate the right side's per-worker vectors; per tuple, one logarithmic lookup in the left shared index.
    const auto numRightVectors = invoke(+[](const IXJSlice* slice) { return slice->getNumberOfVectorsPerSide(); }, rightSliceRef);
    for (nautilus::val<uint64_t> rightWorker = 0; rightWorker < numRightVectors; ++rightWorker)
    {
        /// Range-parallel probing: task i iterates the right worker-vectors with index ≡ i (mod rangeCount).
        if (rangeCount > 1 and rightWorker % rangeCount != rangeIndex)
        {
            continue;
        }
        const auto rightBufferRef
            = invoke(getVectorBufferProxy, rightSliceRef, rightWorker, nautilus::val<JoinBuildSideType>(JoinBuildSideType::Right));
        const PagedVectorRef rightPagedVector(BorrowedNautilusBuffer::from(rightBufferRef), rightTupleLayout);
        for (auto rightIt = rightPagedVector.begin(); rightIt != rightPagedVector.end(); ++rightIt)
        {
            const auto rightRecord = *rightIt;
            std::vector<VarVal> keyValues;
            for (nautilus::static_val<uint64_t> i = 0; i < rightKeyFieldNames.size(); ++i)
            {
                keyValues.emplace_back(rightRecord.read(rightKeyFieldNames[i]));
            }
            const auto hash = hashFunction->calculate(keyValues);

            auto lookupState = invoke(
                +[](const IXJSlice* slice, const uint64_t keyHash)
                { return slice->startLookup(JoinBuildSideType::Left, keyHash); },
                leftSliceRef,
                hash);
            if (lookupState != nullptr)
            {
                nautilus::val<uint64_t> packed = invoke(
                    +[](IXJSlice::LookupState* state) -> uint64_t
                    {
                        if (state->pos >= state->matches.size())
                        {
                            delete state; /// self-frees on exhaustion; the caller stops on LOOKUP_END
                            return IXJSlice::LOOKUP_END;
                        }
                        return state->matches[state->pos++];
                    },
                    lookupState);
                while (packed != IXJSlice::LOOKUP_END)
                {
                    const auto leftWorker = packed >> IXJSlice::WORKER_SHIFT;
                    const auto leftPosition = packed & IXJSlice::POSITION_MASK;
                    const auto leftBufferRef = invoke(
                        getVectorBufferProxy, leftSliceRef, leftWorker, nautilus::val<JoinBuildSideType>(JoinBuildSideType::Left));
                    const PagedVectorRef leftPagedVector(BorrowedNautilusBuffer::from(leftBufferRef), leftTupleLayout);
                    const auto leftRecord = leftPagedVector.at(leftPosition);

                    /// Candidates are already hash-pruned, so evaluate the join predicate on the full joined
                    /// record directly (the predicate reads the original fields, not the casted key fields).
                    auto joinedRecord = createJoinedRecord(leftRecord, rightRecord, windowStart, windowEnd, leftFields, rightFields);
                    if (joinFunction.execute(joinedRecord, executionCtx.pipelineMemoryProvider.arena))
                    {
                        executeChild(executionCtx, joinedRecord);
                    }

                    packed = invoke(
                        +[](IXJSlice::LookupState* state) -> uint64_t
                        {
                            if (state->pos >= state->matches.size())
                            {
                                delete state;
                                return IXJSlice::LOOKUP_END;
                            }
                            return state->matches[state->pos++];
                        },
                        lookupState);
                }
            }
        }
    }
}

}
