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

#include <Join/NestedLoopJoin/NLJInnerProbePhysicalOperator.hpp>

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>
#include <DataTypes/DataTypesUtil.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/NESStrongTypeRef.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Interface/TimestampRef.hpp>
#include <Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Join/NestedLoopJoin/NLJProbePhysicalOperatorBase.hpp>
#include <Join/NestedLoopJoin/NLJSlice.hpp>
#include <Join/StreamJoinProbePhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Operators/Windows/WindowMetaData.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Time/Timestamp.hpp>
#include <ExecutionContext.hpp>
#include <function.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{

namespace
{
NLJSlice* nljEagerSliceFromEnd(OperatorHandler* ptrOpHandler, const SliceEnd sliceEnd)
{
    PRECONDITION(ptrOpHandler != nullptr, "op handler context should not be null");
    const auto* opHandler = dynamic_cast<NLJOperatorHandler*>(ptrOpHandler);
    auto slice = opHandler->getSliceAndWindowStore().getSliceBySliceEnd(sliceEnd);
    INVARIANT(slice.has_value(), "Could not find a slice for slice end {}", sliceEnd);
    return dynamic_cast<NLJSlice*>(slice.value().get());
}
}

NLJInnerProbePhysicalOperator::NLJInnerProbePhysicalOperator(
    OperatorHandlerId operatorHandlerId,
    PhysicalFunction joinFunction,
    WindowMetaData windowMetaData,
    const JoinSchema& joinSchema,
    std::shared_ptr<PagedVectorTupleLayout> leftTupleLayout,
    std::shared_ptr<PagedVectorTupleLayout> rightTupleLayout,
    std::vector<Record::RecordFieldIdentifier> leftKeyFieldNames,
    std::vector<Record::RecordFieldIdentifier> rightKeyFieldNames,
    const bool eager)
    : NLJProbePhysicalOperatorBase(
          operatorHandlerId,
          std::move(joinFunction),
          std::move(windowMetaData),
          joinSchema,
          std::move(leftTupleLayout),
          std::move(rightTupleLayout),
          std::move(leftKeyFieldNames),
          std::move(rightKeyFieldNames))
    , eager(eager)
{
}

void NLJInnerProbePhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
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

    /// Getting the left and right paged vector
    const auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    const auto leftPagedVectorRef = getPagedVectorBufferRef(operatorHandlerMemRef, sliceIdLeft, JoinBuildSideType::Left);
    const auto rightPagedVectorRef = getPagedVectorBufferRef(operatorHandlerMemRef, sliceIdRight, JoinBuildSideType::Right);

    const PagedVectorRef leftPagedVector(BorrowedNautilusBuffer::from(leftPagedVectorRef), leftTupleLayout);
    const PagedVectorRef rightPagedVector(BorrowedNautilusBuffer::from(rightPagedVectorRef), rightTupleLayout);
    if (eager)
    {
        /// Eager trigger (T2): every pair was found and predicate-verified at insert time; the trigger
        /// only drains them. Tumbling windows only (the lowering falls back to lazy for sliding), so the
        /// left and right slice are the same slice and pairs cover the whole window.
        const auto leftFields = getOrderedFieldNames(leftTupleLayout->getSchema());
        const auto rightFields = getOrderedFieldNames(rightTupleLayout->getSchema());
        const auto sliceRef = invoke(nljEagerSliceFromEnd, operatorHandlerMemRef, sliceIdLeft);
        const auto pairCount = invoke(+[](NLJSlice* slice) -> uint64_t { return slice->eagerPairFlattenCount(); }, sliceRef);
        for (nautilus::val<uint64_t> i = 0; i < pairCount; ++i)
        {
            const auto leftPosition
                = invoke(+[](const NLJSlice* slice, const uint64_t idx) -> uint64_t { return slice->eagerPairLeftAt(idx); }, sliceRef, i);
            const auto rightPosition
                = invoke(+[](const NLJSlice* slice, const uint64_t idx) -> uint64_t { return slice->eagerPairRightAt(idx); }, sliceRef, i);
            const auto leftRecord = leftPagedVector.at(leftPosition);
            const auto rightRecord = rightPagedVector.at(rightPosition);
            auto joinedRecord = createJoinedRecord(leftRecord, rightRecord, windowStart, windowEnd, leftFields, rightFields);
            executeChild(executionCtx, joinedRecord);
        }
        return;
    }

    const auto numberOfTuplesLeft = leftPagedVector.getNumberOfRecords();
    const auto numberOfTuplesRight = rightPagedVector.getNumberOfRecords();

    /// Outer loop should have more no. tuples
    if (numberOfTuplesLeft < numberOfTuplesRight)
    {
        performNLJ(
            leftPagedVector,
            rightPagedVector,
            *leftTupleLayout,
            *rightTupleLayout,
            leftKeyFieldNames,
            rightKeyFieldNames,
            executionCtx,
            windowStart,
            windowEnd);
    }
    else
    {
        performNLJ(
            rightPagedVector,
            leftPagedVector,
            *rightTupleLayout,
            *leftTupleLayout,
            rightKeyFieldNames,
            leftKeyFieldNames,
            executionCtx,
            windowStart,
            windowEnd);
    }
}

}
