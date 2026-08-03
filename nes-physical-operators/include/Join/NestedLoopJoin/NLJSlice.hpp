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

#include <array>
#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
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

    /// ===== Eager trigger (T2) support =====
    /// Positions found at insert time are packed (worker, position-in-worker-vector) references; they
    /// stay valid across combinePagedVectors(), which records the per-worker prefix offsets needed to
    /// translate them into combined-vector positions at trigger time.
    static constexpr uint64_t EAGER_WORKER_SHIFT = 40;
    static constexpr uint64_t EAGER_POS_MASK = (uint64_t{1} << EAGER_WORKER_SHIFT) - 1;
    [[nodiscard]] uint64_t combinedPosition(uint64_t side, uint64_t packed) const;
    [[nodiscard]] uint64_t workerCount() const { return numWorkers; }

    /// Eager NestedLoopJoin (handshake/SplitJoin-style): one slice-global mutex serializes every insert
    /// of both sides, so each tuple scans exactly the strictly-earlier opposite tuples -- exactly-once
    /// without per-tuple sequence state.
    /// ponytail: global lock; core-to-core flow (handshake) or broadcast (SplitJoin) if throughput matters.
    void lockEager();
    void unlockEager();
    /// Appends a pair found by `workerThreadId` (single writer per list). Oriented: ownSide 0 = left.
    void appendEagerOriented(WorkerThreadId workerThreadId, uint64_t ownSide, uint64_t ownPosition, uint64_t oppWorker, uint64_t oppPosition);
    /// Trigger-time: flattens all per-worker pair lists into combined-vector positions; idempotent.
    uint64_t eagerPairFlattenCount();
    [[nodiscard]] uint64_t eagerPairLeftAt(const uint64_t i) const { return eagerPairsFlat[i][0]; }
    [[nodiscard]] uint64_t eagerPairRightAt(const uint64_t i) const { return eagerPairsFlat[i][1]; }

    /// Eager RunHashJoin ("per-run trigger"): tuples append (hash, packedPos) to an open per-side run; a
    /// full run seals (sorts), draws a global seal sequence, and immediately probes every opposite run
    /// with a smaller sequence -- each (runA, runB) combination is probed exactly once, by whichever run
    /// seals later. The unsealed tails complete at trigger time.
    struct EagerRunSealed
    {
        std::vector<std::pair<uint64_t, uint64_t>> entries; /// (hash, packedPos), sorted by hash
        uint64_t seq{0};
    };
    struct EagerRuns
    {
        std::mutex sideMutex[2];
        std::mutex sealMutex;
        uint64_t sealSeq{0};
        std::vector<std::shared_ptr<EagerRunSealed>> sealed[2];
        std::vector<std::pair<uint64_t, uint64_t>> open[2];
    };
    void eagerRunInsert(uint64_t side, uint64_t hash, uint64_t position, WorkerThreadId workerThreadId);
    [[nodiscard]] const EagerRuns* getEagerRuns() const { return eagerRuns.get(); }

protected:
    /// This does not really follow our compact-buffer data structure logic
    std::vector<TupleBuffer> leftPagedVectorBuffers;
    std::vector<TupleBuffer> rightPagedVectorBuffers;
    std::mutex combinePagedVectorsMutex;
    std::atomic<int> sortedRunPhase[2]{}; /// 0 = unbuilt, 1 = building, 2 = sealed
    std::vector<std::pair<uint64_t, uint64_t>> sortedRuns[2];

    /// Eager trigger (T2) state; allocated lazily, zero cost for lazy queries.
    uint64_t numWorkers;
    std::mutex eagerMutex;
    std::once_flag eagerPairsOnce;
    std::vector<std::vector<std::array<uint64_t, 2>>> eagerPairs;
    std::vector<std::array<uint64_t, 2>> eagerPairsFlat;
    bool eagerFlattened{false};
    std::vector<uint64_t> combinedOffsets[2];
    std::once_flag eagerRunsOnce;
    std::unique_ptr<EagerRuns> eagerRuns;

    void appendEagerPair(uint64_t workerIdx, uint64_t leftPacked, uint64_t rightPacked);
};
}
