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

#include <atomic>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Interface/PagedVector/PagedVector.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SliceStore/Slice.hpp>

namespace NES
{

struct CreateNewNLJSliceArgs final : CreateNewSlicesArguments
{
    CreateNewNLJSliceArgs(AbstractBufferProvider& bufferProvider, uint64_t tupleSizeLeft, uint64_t tupleSizeRight)
        : bufferProvider(&bufferProvider)
        , tupleSizeLeft(tupleSizeLeft)
        , tupleSizeRight(tupleSizeRight) /// NOLINT(clang-analyzer-optin.cplusplus.UninitializedObject)
    {
    }

    ~CreateNewNLJSliceArgs() override = default;

    AbstractBufferProvider* bufferProvider;
    uint64_t tupleSizeLeft;
    uint64_t tupleSizeRight;
};

/// This class represents a single slice for the NestedLoopJoin. It stores all tuples for the left and right stream.
/// Also serves as the tuple storage of the index join (IXJSlice extends it with a shared index).
class NLJSlice : public Slice
{
public:
    NLJSlice(
        AbstractBufferProvider& bufferProvider,
        SliceStart sliceStart,
        SliceEnd sliceEnd,
        uint64_t numberOfWorkerThreads,
        uint64_t tupleSizeLeft,
        uint64_t tupleSizeRight);

    /// Returns the number of tuples in this slice on either side.
    [[nodiscard]] uint64_t getNumberOfTuplesLeft() const;
    [[nodiscard]] uint64_t getNumberOfTuplesRight() const;

    /// Returns the pointer to the PagedVector on either side.
    [[nodiscard]] const TupleBuffer* getPagedVectorRefLeft(WorkerThreadId workerThreadId) const;
    [[nodiscard]] const TupleBuffer* getPagedVectorRefRight(WorkerThreadId workerThreadId) const;
    [[nodiscard]] const TupleBuffer* getPagedVectorTupleBufferRef(WorkerThreadId workerThreadId, JoinBuildSideType joinBuildSide) const;

    /// Moves all tuples in this slice to the PagedVector at 0th index on both sides.
    void combinePagedVectors();

    /// Per-slice sorted (hash, position) runs for join_state_scope=PER_SLICE: built exactly once by the
    /// first probe task that touches this (slice, side), reused by every slice-pair task of every
    /// overlapping window, freed with the slice. Claim/seal follow the CAS + spin pattern of the range
    /// probes: tryClaim returns true for the single builder, everyone else waits for the seal.
    bool tryClaimSortedRun(uint64_t side);
    void appendSortedRunEntry(uint64_t side, uint64_t hash, uint64_t position);
    void sealSortedRun(uint64_t side);
    void waitSortedRunReady(uint64_t side) const;
    [[nodiscard]] const std::vector<std::pair<uint64_t, uint64_t>>& getSortedRun(uint64_t side) const;

protected:
    /// This does not really follow our compact-buffer data structure logic
    std::vector<TupleBuffer> leftPagedVectorBuffers;
    std::vector<TupleBuffer> rightPagedVectorBuffers;
    std::mutex combinePagedVectorsMutex;
    std::atomic<int> sortedRunPhase[2]{}; /// 0 = unbuilt, 1 = building, 2 = sealed
    std::vector<std::pair<uint64_t, uint64_t>> sortedRuns[2];
};
}
