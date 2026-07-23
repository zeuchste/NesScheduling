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

#include <cstdint>
#include <map>
#include <vector>
#include <folly/Synchronized.h>
#include <Identifiers/Identifiers.hpp>
#include <Join/NestedLoopJoin/NLJSlice.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <SliceStore/Slice.hpp>

namespace NES
{

/// Slice of the index join (A4): NLJ-style per-worker tuple storage plus one shared, ordered index per side
/// that is maintained INCREMENTALLY at insert time and shared by all worker threads. Probes are logarithmic
/// lookups. The per-worker paged vectors are never combined, so an index entry (worker, position) stays valid.
/// ponytail: std::multimap over the key hash with a coarse lock; a B+-tree with CAS inserts and true key order
/// (for range predicates) is the upgrade path.
class IXJSlice final : public NLJSlice
{
public:
    /// Sentinel returned by LookupState::next() once all matches are consumed.
    static constexpr uint64_t LOOKUP_END = UINT64_MAX;

    IXJSlice(
        AbstractBufferProvider& bufferProvider,
        SliceStart sliceStart,
        SliceEnd sliceEnd,
        uint64_t numberOfWorkerThreads,
        uint64_t tupleSizeLeft,
        uint64_t tupleSizeRight);

    [[nodiscard]] uint64_t getNumberOfVectorsPerSide() const { return numberOfWorkerThreads; }

    /// Registers the NEXT tuple that workerThreadId will append to its paged vector of the given side under
    /// the given key hash. Must be called immediately before the corresponding pushBack, from the owning
    /// worker thread (the vector count read here is only stable for the caller's own vector).
    void insertIndexEntry(JoinBuildSideType side, WorkerThreadId workerThreadId, uint64_t keyHash);

    /// Snapshot of all index matches for a key hash on one side. Values pack (worker << WORKER_SHIFT) | position.
    /// Returns nullptr if there is no match. The returned state deletes itself when next() returns LOOKUP_END.
    struct LookupState
    {
        std::vector<uint64_t> matches;
        size_t pos{0};
    };
    [[nodiscard]] LookupState* startLookup(JoinBuildSideType side, uint64_t keyHash) const;

    static constexpr uint64_t WORKER_SHIFT = 40; /// supports 2^40 tuples per worker vector and 2^24 workers
    static constexpr uint64_t POSITION_MASK = (uint64_t{1} << WORKER_SHIFT) - 1;

private:
    using SideIndex = folly::Synchronized<std::multimap<uint64_t, uint64_t>>;
    [[nodiscard]] const SideIndex& indexFor(JoinBuildSideType side) const
    {
        return side == JoinBuildSideType::Left ? leftIndex : rightIndex;
    }
    SideIndex& indexFor(JoinBuildSideType side) { return side == JoinBuildSideType::Left ? leftIndex : rightIndex; }

    uint64_t numberOfWorkerThreads;
    SideIndex leftIndex;
    SideIndex rightIndex;
};

}
