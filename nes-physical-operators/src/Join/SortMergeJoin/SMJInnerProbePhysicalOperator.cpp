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
#include <Join/NestedLoopJoin/NLJSlice.hpp>
#include <Join/StreamJoinProbePhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Operators/Windows/WindowMetaData.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Time/Timestamp.hpp>
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
/// Resolves a slice end to its NLJSlice via the handler (mirror of the NLJ probe's internal proxy).
NLJSlice* smjSliceFromEnd(OperatorHandler* ptrOpHandler, const SliceEnd sliceEnd)
{
    PRECONDITION(ptrOpHandler != nullptr, "op handler context should not be null");
    const auto* opHandler = dynamic_cast<NLJOperatorHandler*>(ptrOpHandler);
    auto slice = opHandler->getSliceAndWindowStore().getSliceBySliceEnd(sliceEnd);
    INVARIANT(slice.has_value(), "Could not find a slice for slice end {}", sliceEnd);
    return dynamic_cast<NLJSlice*>(slice.value().get());
}
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
    /// CompactHashJoin (hash-grouping kernel): per-side bucket-contiguous runs + prefix offsets, shared mask.
    std::vector<uint64_t> offsets[2];
    uint64_t buckets{0};
    /// join_prefilter=BLOOM (one-sided kernels only): negative directory over the left side. Two probes
    /// derived from the stored 64-bit hash, ~8 bits per key; a right entry whose bits miss skips the
    /// directory probe entirely — the win scales with the miss rate.
    std::vector<uint64_t> bloom;
    uint64_t bloomMask{0};
};

void bloomBuild(SMJSortState* state)
{
    uint64_t words = 64; /// 8 bits/key target, pow2 words of 64 bits
    while (words * 64 < state->sides[0].size() * 8)
    {
        words <<= 1;
    }
    state->bloom.assign(words, 0);
    state->bloomMask = words * 64 - 1;
    for (const auto& [hash, position] : state->sides[0])
    {
        const uint64_t b1 = hash & state->bloomMask;
        const uint64_t b2 = (hash >> 32 ^ hash * 0x9e3779b97f4a7c15ULL) & state->bloomMask;
        state->bloom[b1 >> 6] |= 1ULL << (b1 & 63);
        state->bloom[b2 >> 6] |= 1ULL << (b2 & 63);
    }
}

bool bloomMayContain(const SMJSortState* state, const uint64_t hash)
{
    const uint64_t b1 = hash & state->bloomMask;
    const uint64_t b2 = (hash >> 32 ^ hash * 0x9e3779b97f4a7c15ULL) & state->bloomMask;
    return (state->bloom[b1 >> 6] >> (b1 & 63) & 1) and (state->bloom[b2 >> 6] >> (b2 & 63) & 1);
}

/// CompactHashJoin trigger kernel, phase A: group both (hash, position) runs by hash bucket via
/// histogram + prefix sum + scatter — O(n), two passes, no comparison sort. Bucket count is the next
/// power of two >= the larger side, so buckets hold ~1 entry per side on unique keys.
void chjSizeBuckets(SMJSortState* state)
{
    const uint64_t n = std::max(state->sides[0].size(), state->sides[1].size());
    uint64_t buckets = 64;
    while (buckets < n)
    {
        buckets <<= 1;
    }
    state->buckets = buckets;
}

void chjGroupSide(SMJSortState* state, const int side)
{
    const uint64_t mask = state->buckets - 1;
    const auto& in = state->sides[side];
    auto& off = state->offsets[side];
    off.assign(state->buckets + 1, 0);
    for (const auto& [hash, position] : in)
    {
        ++off[(hash & mask) + 1];
    }
    for (uint64_t b = 1; b <= state->buckets; ++b)
    {
        off[b] += off[b - 1];
    }
    std::vector<std::pair<uint64_t, uint64_t>> out(in.size());
    std::vector<uint64_t> cursor(off.begin(), off.end() - 1);
    for (const auto& entry : in)
    {
        out[cursor[entry.first & mask]++] = entry;
    }
    state->sides[side] = std::move(out); /// side now bucket-contiguous; offsets index the regions
}

void chjGroup(SMJSortState* state)
{
    chjSizeBuckets(state);
    chjGroupSide(state, 0);
    chjGroupSide(state, 1);
}

/// CompactHashJoin trigger kernel, phase B: per-bucket merge of the region [bucketLo, bucketHi).
/// Buckets are tiny (~1 entry/side), so the in-bucket nested loop is O(1) amortized; full-hash equality
/// filters bucket collisions, the join predicate later verifies the actual keys.
void chjMergeBuckets(
    const SMJSortState* state, const uint64_t bucketLo, const uint64_t bucketHi, std::vector<std::pair<uint64_t, uint64_t>>& pairs)
{
    const auto& left = state->sides[0];
    const auto& right = state->sides[1];
    const auto& leftOff = state->offsets[0];
    const auto& rightOff = state->offsets[1];
    for (uint64_t b = bucketLo; b < bucketHi; ++b)
    {
        for (uint64_t i = leftOff[b]; i < leftOff[b + 1]; ++i)
        {
            for (uint64_t j = rightOff[b]; j < rightOff[b + 1]; ++j)
            {
                if (left[i].first == right[j].first)
                {
                    pairs.emplace_back(left[i].second, right[j].second);
                }
            }
        }
    }
}

uint64_t chjGroupAndMerge(SMJSortState* state)
{
    chjGroup(state);
    chjMergeBuckets(state, 0, state->buckets, state->candidatePairs);
    return state->candidatePairs.size();
}

/// RunMergeJoin trigger kernel: bottom-up mergesort with cache-sized initial runs — sort each run of
/// RMJ_RUN_SIZE entries, then log(k) inplace_merge passes. Same result as std::sort; the run structure is
/// what stage B moves into the build phase (runs sealed and sorted during ingestion, only the merge left here).
constexpr uint64_t RMJ_RUN_SIZE = 65536; /// ponytail: fixed ~1 MiB runs; make it a knob if run size ever matters
void rmjSortRuns(std::vector<std::pair<uint64_t, uint64_t>>& entries)
{
    const size_t n = entries.size();
    for (size_t lo = 0; lo < n; lo += RMJ_RUN_SIZE)
    {
        std::sort(entries.begin() + lo, entries.begin() + std::min(lo + RMJ_RUN_SIZE, n));
    }
    for (size_t width = RMJ_RUN_SIZE; width < n; width <<= 1)
    {
        for (size_t lo = 0; lo + width < n; lo += width * 2)
        {
            std::inplace_merge(entries.begin() + lo, entries.begin() + lo + width, entries.begin() + std::min(lo + width * 2, n));
        }
    }
}

void rmjMarkReady(SMJSortState* state)
{
    rmjSortRuns(state->sides[0]);
    rmjSortRuns(state->sides[1]);
    state->phase.store(2, std::memory_order_release);
}

/// ONE_SIDED kernels: directory over the left side only, right side streamed against it.
/// Sorted-directory variant (SORT and RUN_MERGE kernels): binary search per right entry.
uint64_t oneSidedSortedMerge(SMJSortState* state, const uint64_t useBloom)
{
    if (useBloom != 0)
    {
        bloomBuild(state);
    }
    const auto& left = state->sides[0];
    const auto cmp = [](const std::pair<uint64_t, uint64_t>& a, const uint64_t v) { return a.first < v; };
    for (const auto& [hash, rightPosition] : state->sides[1])
    {
        if (useBloom != 0 and not bloomMayContain(state, hash))
        {
            continue;
        }
        for (auto it = std::lower_bound(left.begin(), left.end(), hash, cmp); it != left.end() and it->first == hash; ++it)
        {
            state->candidatePairs.emplace_back(it->second, rightPosition);
        }
    }
    return state->candidatePairs.size();
}

uint64_t smjOneSidedSortAndMerge(SMJSortState* state, const uint64_t useBloom)
{
    std::ranges::sort(state->sides[0]);
    return oneSidedSortedMerge(state, useBloom);
}

uint64_t rmjOneSidedSortAndMerge(SMJSortState* state, const uint64_t useBloom)
{
    rmjSortRuns(state->sides[0]);
    return oneSidedSortedMerge(state, useBloom);
}

/// Hash-grouping one-sided variant: bucket the left side only; each right entry scans its left bucket region.
uint64_t chjOneSidedGroupAndMerge(SMJSortState* state, const uint64_t useBloom)
{
    chjSizeBuckets(state);
    chjGroupSide(state, 0);
    if (useBloom != 0)
    {
        bloomBuild(state);
    }
    const uint64_t mask = state->buckets - 1;
    const auto& left = state->sides[0];
    const auto& leftOff = state->offsets[0];
    for (const auto& [hash, rightPosition] : state->sides[1])
    {
        if (useBloom != 0 and not bloomMayContain(state, hash))
        {
            continue;
        }
        const uint64_t b = hash & mask;
        for (uint64_t i = leftOff[b]; i < leftOff[b + 1]; ++i)
        {
            if (left[i].first == hash)
            {
                state->candidatePairs.emplace_back(left[i].second, rightPosition);
            }
        }
    }
    return state->candidatePairs.size();
}

void chjMarkReady(SMJSortState* state)
{
    chjGroup(state);
    state->phase.store(2, std::memory_order_release);
}

/// Range-parallel CompactHashJoin: task idx merges its disjoint bucket range.
std::vector<std::pair<uint64_t, uint64_t>>* chjMergeBucketRange(const SMJSortState* state, const uint64_t idx, const uint64_t ranges)
{
    auto* pairs = new std::vector<std::pair<uint64_t, uint64_t>>();
    const uint64_t step = state->buckets / ranges;
    const uint64_t lo = idx * step;
    const uint64_t hi = idx == ranges - 1 ? state->buckets : (idx + 1) * step;
    chjMergeBuckets(state, lo, hi, *pairs);
    return pairs;
}

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

/// Merges two fully sorted runs into a candidate-pair list. The merge is plain C++ over the
/// (hash, position) index pairs; only the per-pair record reads and the predicate run in Nautilus.
void mergeRuns(
    const std::vector<std::pair<uint64_t, uint64_t>>& left,
    const std::vector<std::pair<uint64_t, uint64_t>>& right,
    std::vector<std::pair<uint64_t, uint64_t>>& out)
{
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
                    out.emplace_back(left[i].second, right[j].second);
                }
            }
            leftPos = leftRunEnd;
            rightPos = rightRunEnd;
        }
    }
}

uint64_t smjSortAndMerge(SMJSortState* state)
{
    std::ranges::sort(state->sides[0]);
    std::ranges::sort(state->sides[1]);
    mergeRuns(state->sides[0], state->sides[1], state->candidatePairs);
    return state->candidatePairs.size();
}

uint64_t rmjSortAndMerge(SMJSortState* state)
{
    rmjSortRuns(state->sides[0]);
    rmjSortRuns(state->sides[1]);
    mergeRuns(state->sides[0], state->sides[1], state->candidatePairs);
    return state->candidatePairs.size();
}

/// join_state_scope=PER_SLICE proxies: claim/append/seal/wait on the slice-owned sorted runs, then merge
/// the left run of the left slice with the right run of the right slice into a task-local pair list.
uint64_t sliceTryClaimRun(NLJSlice* slice, const uint64_t side)
{
    return slice->tryClaimSortedRun(side) ? 1 : 0;
}

void sliceAppendRunEntry(NLJSlice* slice, const uint64_t side, const uint64_t hash, const uint64_t position)
{
    slice->appendSortedRunEntry(side, hash, position);
}

void sliceSealRun(NLJSlice* slice, const uint64_t side)
{
    slice->sealSortedRun(side);
}

void sliceWaitRun(const NLJSlice* slice, const uint64_t side)
{
    slice->waitSortedRunReady(side);
}

std::vector<std::pair<uint64_t, uint64_t>>* sliceMergeRuns(const NLJSlice* leftSlice, const NLJSlice* rightSlice)
{
    auto* pairs = new std::vector<std::pair<uint64_t, uint64_t>>();
    mergeRuns(leftSlice->getSortedRun(0), rightSlice->getSortedRun(1), *pairs);
    return pairs;
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
    std::shared_ptr<HashFunction> hashFunction,
    const SMJKernel kernel,
    const bool oneSided,
    const bool perSliceRuns,
    const bool bloomFilter)
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
    , kernel(kernel)
    , oneSided(oneSided)
    , perSliceRuns(perSliceRuns)
    , bloomFilter(bloomFilter)
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

    if (perSliceRuns)
    {
        performPerSliceJoin(leftPagedVector, rightPagedVector, executionCtx, windowStart, windowEnd, sliceIdLeft, sliceIdRight);
        return;
    }
    performSortMergeJoin(leftPagedVector, rightPagedVector, executionCtx, windowStart, windowEnd, rangeIndex, rangeCount);
}

/// join_state_scope=PER_SLICE: the sorted (hash, position) run of each (slice, side) is built exactly once
/// — by the first probe task that touches it — and cached on the slice, so every slice-pair task of every
/// overlapping window merges cached runs instead of re-extracting and re-sorting. All three kernels share
/// this path (the run is the directory); single-task probe only (the lowering forces rangeCount = 1).
void SMJInnerProbePhysicalOperator::performPerSliceJoin(
    const PagedVectorRef& leftPagedVector,
    const PagedVectorRef& rightPagedVector,
    ExecutionContext& executionCtx,
    const nautilus::val<Timestamp>& windowStart,
    const nautilus::val<Timestamp>& windowEnd,
    const nautilus::val<SliceEnd>& sliceIdLeft,
    const nautilus::val<SliceEnd>& sliceIdRight) const
{
    const auto leftFields = getOrderedFieldNames(leftTupleLayout->getSchema());
    const auto rightFields = getOrderedFieldNames(rightTupleLayout->getSchema());
    const auto handlerRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    const auto leftSliceRef = nautilus::invoke(smjSliceFromEnd, handlerRef, sliceIdLeft);
    const auto rightSliceRef = nautilus::invoke(smjSliceFromEnd, handlerRef, sliceIdRight);

    const auto buildOrWaitRun = [&](const nautilus::val<NLJSlice*>& sliceRef,
                                    const PagedVectorRef& pagedVector,
                                    const std::vector<Record::RecordFieldIdentifier>& keyFieldNames,
                                    const uint64_t side)
    {
        const auto claimed = nautilus::invoke(sliceTryClaimRun, sliceRef, nautilus::val<uint64_t>(side));
        if (claimed == 1)
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
                nautilus::invoke(sliceAppendRunEntry, sliceRef, nautilus::val<uint64_t>(side), hash, position);
                ++position;
            }
            nautilus::invoke(sliceSealRun, sliceRef, nautilus::val<uint64_t>(side));
        }
        else
        {
            nautilus::invoke(sliceWaitRun, sliceRef, nautilus::val<uint64_t>(side));
        }
    };
    buildOrWaitRun(leftSliceRef, leftPagedVector, leftKeyFieldNames, 0);
    buildOrWaitRun(rightSliceRef, rightPagedVector, rightKeyFieldNames, 1);

    const auto pairs = nautilus::invoke(sliceMergeRuns, leftSliceRef, rightSliceRef);
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
            switch (kernel)
            {
                case SMJKernel::HASH_GROUP:
                    nautilus::invoke(chjMarkReady, state);
                    break;
                case SMJKernel::RUN_MERGE:
                    nautilus::invoke(rmjMarkReady, state);
                    break;
                case SMJKernel::SORT:
                    nautilus::invoke(smjMarkReady, state);
                    break;
            }
        }
        else
        {
            nautilus::invoke(smjWaitReady, state);
        }

        /// Disjoint range of this task: hash-domain ranges for the sorted kernels, bucket ranges for hash
        /// grouping. SORT and RUN_MERGE both leave fully sorted sides, so they share the hash-domain path.
        const auto pairs = kernel == SMJKernel::HASH_GROUP
            ? nautilus::invoke(chjMergeBucketRange, state, rangeIndex, rangeCount)
            : nautilus::invoke(
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

    /// Phase 2: build the trigger-time directory and produce candidate pairs. The kernel decides the cost:
    /// SORT = O(n log n) comparison sort, HASH_GROUP = O(n) bucket grouping, RUN_MERGE = cache-sized runs +
    /// k-way merge; oneSided variants build the directory over the left side only and stream the right side.
    const auto numberOfCandidatePairs = [&]
    {
        switch (kernel)
        {
            case SMJKernel::HASH_GROUP:
                return oneSided ? nautilus::invoke(chjOneSidedGroupAndMerge, state, nautilus::val<uint64_t>(bloomFilter ? 1 : 0))
                                : nautilus::invoke(chjGroupAndMerge, state);
            case SMJKernel::RUN_MERGE:
                return oneSided ? nautilus::invoke(rmjOneSidedSortAndMerge, state, nautilus::val<uint64_t>(bloomFilter ? 1 : 0))
                                : nautilus::invoke(rmjSortAndMerge, state);
            case SMJKernel::SORT:
            default:
                return oneSided ? nautilus::invoke(smjOneSidedSortAndMerge, state, nautilus::val<uint64_t>(bloomFilter ? 1 : 0))
                                : nautilus::invoke(smjSortAndMerge, state);
        }
    }();

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
