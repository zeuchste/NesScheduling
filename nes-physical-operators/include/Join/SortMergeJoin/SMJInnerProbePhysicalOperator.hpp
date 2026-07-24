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

#include <memory>
#include <vector>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Hash/HashFunction.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Join/NestedLoopJoin/NLJProbePhysicalOperatorBase.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Operators/Windows/WindowMetaData.hpp>
#include <Time/Timestamp.hpp>
#include <ExecutionContext.hpp>

namespace NES
{

/// Probe phase of the sort-merge join (A2): at trigger time, both sides' runs are sorted by join-key hash
/// (equal keys become adjacent) and a merge pass emits the pairs of equal-hash runs. The join predicate is
/// evaluated per candidate pair, so hash collisions cannot produce wrong results — they only cost extra
/// comparisons. Build side and slices are shared with the nested-loop join (append-only paged vectors).
/// ponytail: sorts (hash, position) index pairs via std::sort in a proxy call; materialized, order-preserving
/// key-encoded runs (SIMD-sortable) are the upgrade path.
/// The trigger-time kernel of the append-only join family sharing this operator:
/// SORT = SortMergeJoin (comparison sort, O(n log n)); HASH_GROUP = CompactHashJoin (histogram + prefix sum +
/// scatter into bucket-contiguous runs, O(n)); RUN_MERGE = RunMergeJoin (cache-sized sorted runs + k-way
/// merge, O(n log C + n log k) — the amortizable middle ground of the build-time axis).
enum class SMJKernel : uint8_t
{
    SORT,
    HASH_GROUP,
    RUN_MERGE
};

class SMJInnerProbePhysicalOperator final : public NLJProbePhysicalOperatorBase
{
public:
    SMJInnerProbePhysicalOperator(
        OperatorHandlerId operatorHandlerId,
        PhysicalFunction joinFunction,
        WindowMetaData windowMetaData,
        const JoinSchema& joinSchema,
        std::shared_ptr<PagedVectorTupleLayout> leftTupleLayout,
        std::shared_ptr<PagedVectorTupleLayout> rightTupleLayout,
        std::vector<Record::RecordFieldIdentifier> leftKeyFieldNames,
        std::vector<Record::RecordFieldIdentifier> rightKeyFieldNames,
        std::shared_ptr<HashFunction> hashFunction,
        SMJKernel kernel = SMJKernel::SORT,
        bool oneSided = false,
        bool perSliceRuns = false);

    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

    static constexpr bool supportsJoinType(JoinLogicalOperator::JoinType joinType) noexcept
    {
        return joinType == JoinLogicalOperator::JoinType::INNER_JOIN;
    }

private:
    void performSortMergeJoin(
        const PagedVectorRef& leftPagedVector,
        const PagedVectorRef& rightPagedVector,
        ExecutionContext& executionCtx,
        const nautilus::val<Timestamp>& windowStart,
        const nautilus::val<Timestamp>& windowEnd,
        const nautilus::val<uint64_t>& rangeIndex,
        const nautilus::val<uint64_t>& rangeCount) const;

    std::shared_ptr<HashFunction> hashFunction;
    SMJKernel kernel;
    /// ONE_SIDED directory-sides knob: build the directory over the left side only and stream the right
    /// side against it. Single-task probe only (the lowering forces rangeCount = 1 for one-sided kernels).
    bool oneSided;
    /// join_state_scope=PER_SLICE: sorted runs cached on the NLJSlice, shared across overlapping windows.
    /// All three kernels use the sorted-run mechanism under this scope; single-task probe only.
    bool perSliceRuns;

    void performPerSliceJoin(
        const PagedVectorRef& leftPagedVector,
        const PagedVectorRef& rightPagedVector,
        ExecutionContext& executionCtx,
        const nautilus::val<Timestamp>& windowStart,
        const nautilus::val<Timestamp>& windowEnd,
        const nautilus::val<SliceEnd>& sliceIdLeft,
        const nautilus::val<SliceEnd>& sliceIdRight) const;
};

}
