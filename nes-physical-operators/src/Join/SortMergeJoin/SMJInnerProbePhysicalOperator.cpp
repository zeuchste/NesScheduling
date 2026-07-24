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
#include <atomic>
#include <cstdint>
#include <map>
#include <mutex>
#include <thread>
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
/// With range-parallel probing (rangeCount > 1) the state is shared by all range tasks of a window: the first
/// task builds and sorts the runs (the others wait), every task merges its own disjoint hash range into a
/// task-local pair list, and the last finishing task frees the state.
struct SMJSortState
{
    std::vector<std::pair<uint64_t, uint64_t>> sides[2];
    std::vector<std::pair<uint64_t, uint64_t>> candidatePairs; /// single-task mode only
    std::atomic<int> phase{0}; /// 0 = unclaimed, 1 = building, 2 = sorted & ready
    std::atomic<uint64_t> tasksRemaining{1};
};

/// Registry of shared states, keyed by (handler, window start): range tasks of the same window meet here.
/// ponytail: global map + mutex; per-handler storage if this ever contends.
std::mutex smjRegistryMutex;
std::map<std::pair<const void*, uint64_t>, SMJSortState*> smjRegistry;

SMJSortState* smjCreateState()
{
    return new SMJSortState();
}

/// Returns the shared state for this window and claims the builder role for exactly one task:
/// out-param semantics via the low bit — (state | isBuilder) is impossible on pointers, so the claim result
/// is returned by smjTryClaimBuild instead.
SMJSortState* smjAcquireShared(const void* handler, const uint64_t windowStart, const uint64_t rangeCount)
{
    const std::lock_guard lock(smjRegistryMutex);
    const auto key = std::make_pair(handler, windowStart);
    if (const auto it = smjRegistry.find(key); it != smjRegistry.end())
    {
        return it->second;
    }
    auto* state = new SMJSortState();
    state->tasksRemaining.store(rangeCount);
    smjRegistry.emplace(key, state);
    return state;
}

uint64_t smjTryClaimBuild(SMJSortState* state)
{
    int expected = 0;
    return state->phase.compare_exchange_strong(expected, 1) ? 1 : 0;
}

void smjMarkReady(SMJSortState* state)
{
    std::ranges::sort(state->sides[0]);
    std::ranges::sort(state->sides[1]);
    state->phase.store(2, std::memory_order_release);
}

void smjWaitReady(const SMJSortState* state)
{
    while (state->phase.load(std::memory_order_acquire) != 2)
    {
        std::this_thread::yield();
    }
}

void smjReleaseShared(const void* handler, const uint64_t windowStart, SMJSortState* state)
{
    if (state->tasksRemaining.fetch_sub(1) == 1)
    {
        {
            const std::lock_guard lock(smjRegistryMutex);
            smjRegistry.erase(std::make_pair(handler, windowStart));
        }
        delete state;
    }
}

void smjAppend(SMJSortState* state, const uint64_t side, const uint64_t hash, const uint64_t position)
{
    state->sides[side].emplace_back(hash, position);
}

/// Merges the hash range [lo, hi] of the sorted runs into a task-local candidate-pair list.
std::vector<std::pair<uint64_t, uint64_t>>*
smjMergeRange(const SMJSortState* state, const uint64_t lo, const uint64_t hi)
{
    auto* pairs = new std::vector<std::pair<uint64_t, uint64_t>>();
    const auto& left = state->sides[0];
    const auto& right = state->sides[1];
    const auto cmp = [](const std::pair<uint64_t, uint64_t>& a, const uint64_t v) { return a.first < v; };
    size_t leftPos = std::lower_bound(left.begin(), left.end(), lo, cmp) - left.begin();
    size_t rightPos = std::lower_bound(right.begin(), right.end(), lo, cmp) - right.begin();
    while (leftPos < left.size() and rightPos < right.size() and left[leftPos].first <= hi and right[rightPos].first <= hi)
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
                    pairs->emplace_back(left[i].second, right[j].second);
                }
            }
            leftPos = leftRunEnd;
            rightPos = rightRunEnd;
        }
    }
    return pairs;
}

uint64_t smjRangePairCount(const std::vector<std::pair<uint64_t, uint64_t>>* pairs)
{
    return pairs->size();
}

uint64_t smjRangePairLeft(const std::vector<std::pair<uint64_t, uint64_t>>* pairs, const uint64_t pair)
{
    return (*pairs)[pair].first;
}

uint64_t smjRangePairRight(const std::vector<std::pair<uint64_t, uint64_t>>* pairs, const uint64_t pair)
{
    return (*pairs)[pair].second;
}

void smjFreeRangePairs(const std::vector<std::pair<uint64_t, uint64_t>>* pairs)
{
    delete pairs;
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
    const auto rangeIndex = readValueFromMemRef<uint64_t>(getMemberRef(triggerRef, &EmittedNLJWindowTrigger::rangeIndex));
    const auto rangeCount = readValueFromMemRef<uint64_t>(getMemberRef(triggerRef, &EmittedNLJWindowTrigger::rangeCount));

    const auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    const auto leftPagedVectorRef = getPagedVectorBufferRef(operatorHandlerMemRef, sliceIdLeft, JoinBuildSideType::Left);
    const auto rightPagedVectorRef = getPagedVectorBufferRef(operatorHandlerMemRef, sliceIdRight, JoinBuildSideType::Right);

    const PagedVectorRef leftPagedVector(BorrowedNautilusBuffer::from(leftPagedVectorRef), leftTupleLayout);
    const PagedVectorRef rightPagedVector(BorrowedNautilusBuffer::from(rightPagedVectorRef), rightTupleLayout);

    performSortMergeJoin(leftPagedVector, rightPagedVector, executionCtx, windowStart, windowEnd, rangeIndex, rangeCount);
}

void SMJInnerProbePhysicalOperator::performSortMergeJoin(
    const PagedVectorRef& leftPagedVector,
    const PagedVectorRef& rightPagedVector,
    ExecutionContext& executionCtx,
    const nautilus::val<Timestamp>& windowStart,
    const nautilus::val<Timestamp>& windowEnd,
    const nautilus::val<uint64_t>& rangeIndex,
    const nautilus::val<uint64_t>& rangeCount) const
{
    const auto leftFields = getOrderedFieldNames(leftTupleLayout->getSchema());
    const auto rightFields = getOrderedFieldNames(rightTupleLayout->getSchema());

    if (rangeCount > 1)
    {
        /// Range-parallel mode: the range tasks of this window share one sort state. The first task builds and
        /// sorts the runs, every task merges its own disjoint hash range, the last task frees the state.
        const auto handlerRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
        const auto state = nautilus::invoke(
            +[](OperatorHandler* handler, const Timestamp windowStart, const uint64_t ranges) -> SMJSortState*
            { return smjAcquireShared(handler, windowStart.getRawValue(), ranges); },
            handlerRef,
            windowStart,
            rangeCount);
        const auto isBuilder = nautilus::invoke(smjTryClaimBuild, state);
        if (isBuilder == 1)
        {
            const auto appendSideShared = [&](const PagedVectorRef& pagedVector,
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
            appendSideShared(leftPagedVector, leftKeyFieldNames, 0);
            appendSideShared(rightPagedVector, rightKeyFieldNames, 1);
            nautilus::invoke(smjMarkReady, state);
        }
        else
        {
            nautilus::invoke(smjWaitReady, state);
        }

        /// Disjoint hash range of this task: [idx * step, (idx + 1) * step - 1], last range ends at UINT64_MAX.
        const auto pairs = nautilus::invoke(
            +[](const SMJSortState* st, const uint64_t idx, const uint64_t ranges) -> std::vector<std::pair<uint64_t, uint64_t>>*
            {
                const uint64_t step = UINT64_MAX / ranges;
                const uint64_t lo = idx * step;
                const uint64_t hi = idx == ranges - 1 ? UINT64_MAX : ((idx + 1) * step - 1);
                return smjMergeRange(st, lo, hi);
            },
            state,
            rangeIndex,
            rangeCount);
        const auto numberOfPairs = nautilus::invoke(smjRangePairCount, pairs);
        for (nautilus::val<uint64_t> pair = 0; pair < numberOfPairs; ++pair)
        {
            const auto leftRecord = leftPagedVector.at(nautilus::invoke(smjRangePairLeft, pairs, pair));
            const auto rightRecord = rightPagedVector.at(nautilus::invoke(smjRangePairRight, pairs, pair));
            auto joinedRecord = createJoinedRecord(leftRecord, rightRecord, windowStart, windowEnd, leftFields, rightFields);
            if (joinFunction.execute(joinedRecord, executionCtx.pipelineMemoryProvider.arena))
            {
                executeChild(executionCtx, joinedRecord);
            }
        }
        nautilus::invoke(smjFreeRangePairs, pairs);
        nautilus::invoke(
            +[](OperatorHandler* handler, const Timestamp windowStart, SMJSortState* st) -> void
            { smjReleaseShared(handler, windowStart.getRawValue(), st); },
            handlerRef,
            windowStart,
            state);
        return;
    }

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
