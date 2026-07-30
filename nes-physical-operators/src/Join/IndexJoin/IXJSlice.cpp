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

#include <Join/IndexJoin/IXJSlice.hpp>

#include <array>
#include <cstdint>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Interface/PagedVector/PagedVector.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <SliceStore/Slice.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

IXJSlice::IXJSlice(
    AbstractBufferProvider& bufferProvider,
    const SliceStart sliceStart,
    const SliceEnd sliceEnd,
    const uint64_t numberOfWorkerThreads,
    const uint64_t tupleSizeLeft,
    const uint64_t tupleSizeRight,
    const bool sharedIndex,
    const bool eager)
    : NLJSlice(bufferProvider, sliceStart, sliceEnd, numberOfWorkerThreads, tupleSizeLeft, tupleSizeRight)
    , numberOfWorkerThreads(numberOfWorkerThreads)
    , sharedIndex(sharedIndex or eager) /// the eager probe reads the opposite side's index concurrently
{
    if (eager)
    {
        eagerPairs.resize(numberOfWorkerThreads);
    }
    if (not this->sharedIndex)
    {
        localIndexes[0].resize(numberOfWorkerThreads);
        localIndexes[1].resize(numberOfWorkerThreads);
    }
}

void IXJSlice::insertIndexEntry(const JoinBuildSideType side, const WorkerThreadId workerThreadId, const uint64_t keyHash)
{
    /// Position of the tuple that is about to be appended = current count of the caller's own vector.
    const auto* vectorBuffer = getPagedVectorTupleBufferRef(workerThreadId, side);
    const auto position = PagedVector::load(*vectorBuffer).getTotalNumberOfRecords();
    const auto worker = workerThreadId % numberOfWorkerThreads;
    INVARIANT(position <= POSITION_MASK, "IXJ index position overflow");
    const auto packed = (static_cast<uint64_t>(worker) << WORKER_SHIFT) | position;
    if (sharedIndex)
    {
        indexFor(side).wlock()->emplace(keyHash, std::pair{packed, uint64_t{0}});
    }
    else
    {
        /// Single writer per (side, worker) during the build phase — no synchronization needed.
        localIndexes[side == JoinBuildSideType::Right][worker].emplace(keyHash, packed);
    }
}

void IXJSlice::insertAndProbeEager(const JoinBuildSideType side, const WorkerThreadId workerThreadId, const uint64_t keyHash)
{
    const auto* vectorBuffer = getPagedVectorTupleBufferRef(workerThreadId, side);
    const auto position = PagedVector::load(*vectorBuffer).getTotalNumberOfRecords();
    const auto worker = workerThreadId % numberOfWorkerThreads;
    INVARIANT(position <= POSITION_MASK, "IXJ index position overflow");
    const auto packed = (static_cast<uint64_t>(worker) << WORKER_SHIFT) | position;

    /// Sequence draw and own-side insert are one critical section: a probe that misses this entry then
    /// provably carries a smaller sequence, so the missed pair is emitted by the other insert instead.
    uint64_t seq = 0;
    {
        const auto locked = indexFor(side).wlock();
        seq = eagerSeq.fetch_add(1, std::memory_order_relaxed);
        locked->emplace(keyHash, std::pair{packed, seq});
    }

    /// Probe the opposite side's index; accept only entries inserted earlier (entrySeq < ownSeq).
    const auto opposite = side == JoinBuildSideType::Left ? JoinBuildSideType::Right : JoinBuildSideType::Left;
    auto& pairs = eagerPairs[worker];
    const auto locked = indexFor(opposite).rlock();
    const auto [first, last] = locked->equal_range(keyHash);
    for (auto it = first; it != last; ++it)
    {
        if (it->second.second < seq)
        {
            const auto other = it->second.first;
            pairs.push_back(
                side == JoinBuildSideType::Left ? std::array<uint64_t, 2>{packed, other} : std::array<uint64_t, 2>{other, packed});
        }
    }
}

IXJSlice::LookupState* IXJSlice::startLookup(const JoinBuildSideType side, const uint64_t keyHash) const
{
    /// Called at probe time only: the slice is closed, no concurrent inserts remain, but we still take the
    /// read lock to be safe against stragglers.
    if (not sharedIndex)
    {
        /// Local mode: the indexes are immutable at probe time; merge the matches of all worker indexes.
        LookupState* state = nullptr;
        for (const auto& index : localIndexes[side == JoinBuildSideType::Right])
        {
            const auto [f, l] = index.equal_range(keyHash);
            for (auto it = f; it != l; ++it)
            {
                if (state == nullptr)
                {
                    state = new LookupState();
                }
                state->matches.push_back(it->second);
            }
        }
        return state;
    }
    const auto locked = indexFor(side).rlock();
    const auto [first, last] = locked->equal_range(keyHash);
    if (first == last)
    {
        return nullptr;
    }
    auto* state = new LookupState();
    for (auto it = first; it != last; ++it)
    {
        state->matches.push_back(it->second.first);
    }
    return state;
}

}
