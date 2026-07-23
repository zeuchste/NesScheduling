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

#include <Join/SortMergeJoin/SMJInnerProbePhysicalOperator.hpp>

#include <algorithm>
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
#include <Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Join/StreamJoinProbePhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Operators/Windows/WindowMetaData.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Time/Timestamp.hpp>
#include <ExecutionContext.hpp>
#include <function.hpp>
#include <static.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>

namespace NES
{

namespace
{
/// Trigger-time sort state: one (hash, position) run per side, sorted by hash so that equal keys are adjacent,
/// plus the candidate pairs produced by the merge pass over the sorted runs.
struct SMJSortState
{
    std::vector<std::pair<uint64_t, uint64_t>> sides[2];
    std::vector<std::pair<uint64_t, uint64_t>> candidatePairs;
};

SMJSortState* smjCreateState()
{
    return new SMJSortState();
}

void smjAppend(SMJSortState* state, const uint64_t side, const uint64_t hash, const uint64_t position)
{
    state->sides[side].emplace_back(hash, position);
}

/// Sorts both sides by hash and merges the sorted runs into the candidate-pair list. The merge is plain C++
/// over the (hash, position) index pairs; only the per-pair record reads and the predicate run in Nautilus.
uint64_t smjSortAndMerge(SMJSortState* state)
{
    std::ranges::sort(state->sides[0]);
    std::ranges::sort(state->sides[1]);
    const auto& left = state->sides[0];
    const auto& right = state->sides[1];
    size_t leftPos = 0;
    size_t rightPos = 0;
    while (leftPos < left.size() and rightPos < right.size())
    {
        if (left[leftPos].first < right[rightPos].first)
        {
            ++leftPos;
        }
        else if (right[rightPos].first < left[leftPos].first)
        {
            ++rightPos;
        }
        else
        {
            auto leftRunEnd = leftPos;
            while (leftRunEnd < left.size() and left[leftRunEnd].first == left[leftPos].first)
            {
                ++leftRunEnd;
            }
            auto rightRunEnd = rightPos;
            while (rightRunEnd < right.size() and right[rightRunEnd].first == right[rightPos].first)
            {
                ++rightRunEnd;
            }
            for (auto i = leftPos; i < leftRunEnd; ++i)
            {
                for (auto j = rightPos; j < rightRunEnd; ++j)
                {
                    state->candidatePairs.emplace_back(left[i].second, right[j].second);
                }
            }
            leftPos = leftRunEnd;
            rightPos = rightRunEnd;
        }
    }
    return state->candidatePairs.size();
}

uint64_t smjPairLeft(const SMJSortState* state, const uint64_t pair)
{
    return state->candidatePairs[pair].first;
}

uint64_t smjPairRight(const SMJSortState* state, const uint64_t pair)
{
    return state->candidatePairs[pair].second;
}

void smjFreeState(const SMJSortState* state)
{
    delete state;
}
}

SMJInnerProbePhysicalOperator::SMJInnerProbePhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    PhysicalFunction joinFunction,
    WindowMetaData windowMetaData,
    const JoinSchema& joinSchema,
    std::shared_ptr<PagedVectorTupleLayout> leftTupleLayout,
    std::shared_ptr<PagedVectorTupleLayout> rightTupleLayout,
    std::vector<Record::RecordFieldIdentifier> leftKeyFieldNames,
    std::vector<Record::RecordFieldIdentifier> rightKeyFieldNames,
    std::shared_ptr<HashFunction> hashFunction)
    : NLJProbePhysicalOperatorBase(
          operatorHandlerId,
          std::move(joinFunction),
          std::move(windowMetaData),
          joinSchema,
          std::move(leftTupleLayout),
          std::move(rightTupleLayout),
          std::move(leftKeyFieldNames),
          std::move(rightKeyFieldNames))
    , hashFunction(std::move(hashFunction))
{
}

void SMJInnerProbePhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    StreamJoinProbePhysicalOperator::open(executionCtx, recordBuffer);

    /// Parse trigger buffer — for inner join, always 1 left + 1 right slice end (same format as the NLJ)
    const auto triggerRef = static_cast<nautilus::val<EmittedNLJWindowTrigger*>>(recordBuffer.getMemArea());
    const auto windowInfoRef = getMemberRef(triggerRef, &EmittedNLJWindowTrigger::windowInfo);
    const auto windowStart = nautilus::val<Timestamp>{readValueFromMemRef<uint64_t>(getMemberRef(windowInfoRef, &WindowInfo::windowStart))};
    const auto windowEnd = nautilus::val<Timestamp>{readValueFromMemRef<uint64_t>(getMemberRef(windowInfoRef, &WindowInfo::windowEnd))};

    auto leftSliceEndsPtr = readValueFromMemRef<SliceEnd::Underlying*>(getMemberRef(triggerRef, &EmittedNLJWindowTrigger::leftSliceEnds));
    auto rightSliceEndsPtr = readValueFromMemRef<SliceEnd::Underlying*>(getMemberRef(triggerRef, &EmittedNLJWindowTrigger::rightSliceEnds));
    const nautilus::val<SliceEnd> sliceIdLeft{leftSliceEndsPtr[0]};
    const nautilus::val<SliceEnd> sliceIdRight{rightSliceEndsPtr[0]};

    const auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    const auto leftPagedVectorRef = getPagedVectorBufferRef(operatorHandlerMemRef, sliceIdLeft, JoinBuildSideType::Left);
    const auto rightPagedVectorRef = getPagedVectorBufferRef(operatorHandlerMemRef, sliceIdRight, JoinBuildSideType::Right);

    const PagedVectorRef leftPagedVector(BorrowedNautilusBuffer::from(leftPagedVectorRef), leftTupleLayout);
    const PagedVectorRef rightPagedVector(BorrowedNautilusBuffer::from(rightPagedVectorRef), rightTupleLayout);

    performSortMergeJoin(leftPagedVector, rightPagedVector, executionCtx, windowStart, windowEnd);
}

void SMJInnerProbePhysicalOperator::performSortMergeJoin(
    const PagedVectorRef& leftPagedVector,
    const PagedVectorRef& rightPagedVector,
    ExecutionContext& executionCtx,
    const nautilus::val<Timestamp>& windowStart,
    const nautilus::val<Timestamp>& windowEnd) const
{
    const auto leftFields = getOrderedFieldNames(leftTupleLayout->getSchema());
    const auto rightFields = getOrderedFieldNames(rightTupleLayout->getSchema());

    const auto state = nautilus::invoke(smjCreateState);

    /// Phase 1: build the (hash, position) runs of both sides.
    const auto appendSide = [&](const PagedVectorRef& pagedVector,
                                const std::vector<Record::RecordFieldIdentifier>& keyFieldNames,
                                const uint64_t side)
    {
        nautilus::val<uint64_t> position = 0;
        for (auto it = pagedVector.begin(); it != pagedVector.end(); ++it)
        {
            const auto record = *it;
            std::vector<VarVal> keyValues;
            for (nautilus::static_val<uint64_t> i = 0; i < keyFieldNames.size(); ++i)
            {
                keyValues.emplace_back(record.read(keyFieldNames[i]));
            }
            const auto hash = hashFunction->calculate(keyValues);
            nautilus::invoke(smjAppend, state, nautilus::val<uint64_t>(side), hash, position);
            ++position;
        }
    };
    appendSide(leftPagedVector, leftKeyFieldNames, 0);
    appendSide(rightPagedVector, rightKeyFieldNames, 1);

    /// Phase 2: sort both runs by hash and merge them into candidate pairs — the trigger-time sort cost of A2.
    const auto numberOfCandidatePairs = nautilus::invoke(smjSortAndMerge, state);

    /// Phase 3: one flat loop over the candidate pairs (equal keys are adjacent in the sorted runs, so this
    /// scans matches sequentially); the join predicate is evaluated per pair, so hash collisions cannot
    /// produce wrong results.
    for (nautilus::val<uint64_t> pair = 0; pair < numberOfCandidatePairs; ++pair)
    {
        const auto leftRecord = leftPagedVector.at(nautilus::invoke(smjPairLeft, state, pair));
        const auto rightRecord = rightPagedVector.at(nautilus::invoke(smjPairRight, state, pair));
        /// The predicate reads the original fields, not the casted key fields, so it runs on the full record.
        auto joinedRecord = createJoinedRecord(leftRecord, rightRecord, windowStart, windowEnd, leftFields, rightFields);
        if (joinFunction.execute(joinedRecord, executionCtx.pipelineMemoryProvider.arena))
        {
            executeChild(executionCtx, joinedRecord);
        }
    }

    nautilus::invoke(smjFreeState, state);
}

}
