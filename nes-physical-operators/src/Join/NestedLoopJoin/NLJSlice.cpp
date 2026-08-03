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

#include <Join/NestedLoopJoin/NLJSlice.hpp>

#include <algorithm>
#include <thread>

#include <cstdint>
#include <memory>
#include <mutex>
#include <numeric>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Interface/PagedVector/PagedVector.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <SliceStore/Slice.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

NLJSlice::NLJSlice(
    AbstractBufferProvider& bufferProvider,
    const SliceStart sliceStart,
    const SliceEnd sliceEnd,
    const uint64_t numberOfWorkerThreads,
    const uint64_t tupleSizeLeft,
    const uint64_t tupleSizeRight)
    : Slice(sliceStart, sliceEnd)
    , numWorkers(numberOfWorkerThreads)
{
    const uint64_t pvMainBufferSize = PagedVector::getMainBufferSize();
    const uint64_t pvPageBufferSize = bufferProvider.getBufferSize();
    for (uint64_t i = 0; i < numberOfWorkerThreads; ++i)
    {
        if (auto pagedVectorBuffer = bufferProvider.getUnpooledBuffer(pvMainBufferSize))
        {
            /// initialize the paged vector tuple buffer
            PagedVector::init(pagedVectorBuffer.value(), pvPageBufferSize, tupleSizeLeft);
            leftPagedVectorBuffers.emplace_back(pagedVectorBuffer.value());
        }
        else
        {
            throw BufferAllocationFailure("No unpooled TupleBuffer available for NLJ left paged vector main buffer");
        }
    }

    for (uint64_t i = 0; i < numberOfWorkerThreads; ++i)
    {
        if (auto pagedVectorBuffer = bufferProvider.getUnpooledBuffer(pvMainBufferSize))
        {
            /// initialize the paged vector tuple buffer
            PagedVector::init(pagedVectorBuffer.value(), pvPageBufferSize, tupleSizeRight);
            rightPagedVectorBuffers.emplace_back(pagedVectorBuffer.value());
        }
        else
        {
            throw BufferAllocationFailure("No unpooled TupleBuffer available for NLJ right paged vector main buffer");
        }
    }
}

uint64_t NLJSlice::getNumberOfTuplesLeft() const
{
    return std::accumulate(
        leftPagedVectorBuffers.begin(),
        leftPagedVectorBuffers.end(),
        0,
        [](uint64_t sum, const TupleBuffer& buf)
        {
            auto pagedVector = PagedVector::load(buf);
            return sum + pagedVector.getTotalNumberOfRecords();
        });
}

uint64_t NLJSlice::getNumberOfTuplesRight() const
{
    return std::accumulate(
        rightPagedVectorBuffers.begin(),
        rightPagedVectorBuffers.end(),
        0,
        [](uint64_t sum, const TupleBuffer& buf)
        {
            auto pagedVector = PagedVector::load(buf);
            return sum + pagedVector.getTotalNumberOfRecords();
        });
}

const TupleBuffer* NLJSlice::getPagedVectorRefLeft(const WorkerThreadId workerThreadId) const
{
    const auto pos = workerThreadId % leftPagedVectorBuffers.size();
    return &leftPagedVectorBuffers[pos];
}

const TupleBuffer* NLJSlice::getPagedVectorRefRight(const WorkerThreadId workerThreadId) const
{
    const auto pos = workerThreadId % rightPagedVectorBuffers.size();
    return &rightPagedVectorBuffers[pos];
}

const TupleBuffer* NLJSlice::getPagedVectorTupleBufferRef(const WorkerThreadId workerThreadId, const JoinBuildSideType joinBuildSide) const
{
    switch (joinBuildSide)
    {
        case JoinBuildSideType::Right:
            return getPagedVectorRefRight(workerThreadId);
        case JoinBuildSideType::Left:
            return getPagedVectorRefLeft(workerThreadId);
    }
    std::unreachable();
}

void NLJSlice::combinePagedVectors()
{
    /// Due to the out-of-order nature of our execution engine, it might happen that we call this code here from multiple worker threads.
    /// For example, if different worker threads are emitting the same slice for different windows.
    /// To ensure correctness, we use a lock here
    const std::scoped_lock lock(combinePagedVectorsMutex);

    /// Record the per-worker prefix offsets before moving any pages: the packed (worker, position)
    /// references of the eager paths translate through these into combined-vector positions.
    if (combinedOffsets[0].empty())
    {
        for (int side = 0; side < 2; ++side)
        {
            const auto& buffers = side == 0 ? leftPagedVectorBuffers : rightPagedVectorBuffers;
            uint64_t offset = 0;
            for (const auto& buf : buffers)
            {
                combinedOffsets[side].push_back(offset);
                offset += PagedVector::load(buf).getTotalNumberOfRecords();
            }
        }
    }

    /// Append all PagedVectors on the left join side and erase all items except for the first one
    /// We do this to ensure that we have only one PagedVector for each side during the probing phase
    if (leftPagedVectorBuffers.size() > 1)
    {
        for (uint64_t i = 1; i < leftPagedVectorBuffers.size(); ++i)
        {
            auto firstLeftPagedVector = PagedVector::load(leftPagedVectorBuffers[0]);
            auto currLeftPagedVector = PagedVector::load(leftPagedVectorBuffers[i]);
            firstLeftPagedVector.movePagesFrom(currLeftPagedVector);
        }
        leftPagedVectorBuffers.erase(leftPagedVectorBuffers.begin() + 1, leftPagedVectorBuffers.end());
    }

    /// Append all PagedVectors on the right join side and remove all items except for the first one
    if (rightPagedVectorBuffers.size() > 1)
    {
        for (uint64_t i = 1; i < rightPagedVectorBuffers.size(); ++i)
        {
            auto firstRightPagedVector = PagedVector::load(rightPagedVectorBuffers[0]);
            auto currRightPagedVector = PagedVector::load(rightPagedVectorBuffers[i]);
            firstRightPagedVector.movePagesFrom(currRightPagedVector);
        }
        rightPagedVectorBuffers.erase(rightPagedVectorBuffers.begin() + 1, rightPagedVectorBuffers.end());
    }
}

bool NLJSlice::tryClaimSortedRun(const uint64_t side)
{
    int expected = 0;
    return sortedRunPhase[side].compare_exchange_strong(expected, 1);
}

void NLJSlice::appendSortedRunEntry(const uint64_t side, const uint64_t hash, const uint64_t position)
{
    sortedRuns[side].emplace_back(hash, position);
}

void NLJSlice::sealSortedRun(const uint64_t side)
{
    std::ranges::sort(sortedRuns[side]);
    sortedRunPhase[side].store(2, std::memory_order_release);
}

void NLJSlice::waitSortedRunReady(const uint64_t side) const
{
    while (sortedRunPhase[side].load(std::memory_order_acquire) != 2)
    {
        std::this_thread::yield();
    }
}

const std::vector<std::pair<uint64_t, uint64_t>>& NLJSlice::getSortedRun(const uint64_t side) const
{
    return sortedRuns[side];
}

uint64_t NLJSlice::combinedPosition(const uint64_t side, const uint64_t packed) const
{
    return combinedOffsets[side][packed >> EAGER_WORKER_SHIFT] + (packed & EAGER_POS_MASK);
}

void NLJSlice::lockEager()
{
    eagerMutex.lock();
}

void NLJSlice::unlockEager()
{
    eagerMutex.unlock();
}

void NLJSlice::appendEagerPair(const uint64_t workerIdx, const uint64_t leftPacked, const uint64_t rightPacked)
{
    std::call_once(eagerPairsOnce, [this] { eagerPairs.resize(numWorkers); });
    eagerPairs[workerIdx].push_back({leftPacked, rightPacked});
}

void NLJSlice::appendEagerOriented(
    const WorkerThreadId workerThreadId,
    const uint64_t ownSide,
    const uint64_t ownPosition,
    const uint64_t oppWorker,
    const uint64_t oppPosition)
{
    const auto w = static_cast<uint64_t>(workerThreadId % numWorkers);
    const auto ownPacked = (w << EAGER_WORKER_SHIFT) | ownPosition;
    const auto oppPacked = (oppWorker << EAGER_WORKER_SHIFT) | oppPosition;
    if (ownSide == 0)
    {
        appendEagerPair(w, ownPacked, oppPacked);
    }
    else
    {
        appendEagerPair(w, oppPacked, ownPacked);
    }
}

uint64_t NLJSlice::eagerPairFlattenCount()
{
    const std::scoped_lock lock(eagerMutex);
    if (not eagerFlattened)
    {
        for (const auto& list : eagerPairs)
        {
            for (const auto& [l, r] : list)
            {
                eagerPairsFlat.push_back({combinedPosition(0, l), combinedPosition(1, r)});
            }
        }
        eagerFlattened = true;
    }
    return eagerPairsFlat.size();
}

namespace
{
constexpr uint64_t EAGER_RUN_SIZE = 65536;

/// Merge-scan two hash-sorted (hash, packedPos) runs; calls emit(entryOfA, entryOfB) per hash match.
template <typename Emit>
void mergeByHash(
    const std::vector<std::pair<uint64_t, uint64_t>>& a, const std::vector<std::pair<uint64_t, uint64_t>>& b, Emit emit)
{
    size_t i = 0;
    size_t j = 0;
    while (i < a.size() and j < b.size())
    {
        if (a[i].first < b[j].first)
        {
            ++i;
        }
        else if (b[j].first < a[i].first)
        {
            ++j;
        }
        else
        {
            const auto hash = a[i].first;
            const size_t i0 = i;
            const size_t j0 = j;
            while (i < a.size() and a[i].first == hash)
            {
                ++i;
            }
            while (j < b.size() and b[j].first == hash)
            {
                ++j;
            }
            for (size_t x = i0; x < i; ++x)
            {
                for (size_t y = j0; y < j; ++y)
                {
                    emit(a[x].second, b[y].second);
                }
            }
        }
    }
}
}

void NLJSlice::eagerRunInsert(const uint64_t side, const uint64_t hash, const uint64_t position, const WorkerThreadId workerThreadId)
{
    std::call_once(eagerRunsOnce, [this] { eagerRuns = std::make_unique<EagerRuns>(); });
    auto& runs = *eagerRuns;
    const auto w = static_cast<uint64_t>(workerThreadId % numWorkers);
    const auto packed = (w << EAGER_WORKER_SHIFT) | position;
    std::vector<std::pair<uint64_t, uint64_t>> full;
    {
        const std::scoped_lock lock(runs.sideMutex[side]);
        runs.open[side].emplace_back(hash, packed);
        if (runs.open[side].size() >= EAGER_RUN_SIZE)
        {
            full.swap(runs.open[side]);
        }
    }
    if (full.empty())
    {
        return;
    }
    /// Seal: sequence draw and publication are one critical section, so a snapshot that misses a run
    /// implies that run carries a larger sequence and will probe this one instead (exactly-once).
    std::ranges::sort(full);
    auto run = std::make_shared<EagerRunSealed>();
    run->entries = std::move(full);
    std::vector<std::shared_ptr<EagerRunSealed>> opposite;
    {
        const std::scoped_lock lock(runs.sealMutex);
        run->seq = ++runs.sealSeq;
        runs.sealed[side].push_back(run);
        opposite = runs.sealed[1 - side];
    }
    for (const auto& opp : opposite)
    {
        if (opp->seq >= run->seq)
        {
            continue;
        }
        mergeByHash(
            run->entries,
            opp->entries,
            [&](const uint64_t mine, const uint64_t theirs)
            {
                if (side == 0)
                {
                    appendEagerPair(w, mine, theirs);
                }
                else
                {
                    appendEagerPair(w, theirs, mine);
                }
            });
    }
}

}
