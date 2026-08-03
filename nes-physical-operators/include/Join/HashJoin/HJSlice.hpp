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
#include <cstdint>
#include <mutex>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SliceStore/Slice.hpp>
#include <HashMapSlice.hpp>

namespace NES
{

struct CreateNewHJSliceArgs final : CreateNewHashMapSliceArgs
{
    /// The base keySize/valueSize describe the LEFT side; rightValueSize the RIGHT side. The sides always share the
    /// same key layout (the lowering casts both sides' keys to identical types), but their value sizes differ as soon
    /// as the storage variant stores the tuples inline (TUPLE_CHAINED): each side then needs its own entry size.
    CreateNewHJSliceArgs(
        const uint64_t keySize,
        const uint64_t valueSize,
        const uint64_t rightValueSize,
        const uint64_t pageSize,
        const uint64_t numberOfBuckets,
        AbstractBufferProvider* bufferProvider,
        const JoinBuildSideType joinBuildSide)
        : CreateNewHashMapSliceArgs{keySize, valueSize, pageSize, numberOfBuckets, bufferProvider}
        , rightValueSize(rightValueSize)
        , joinBuildSide(joinBuildSide)
    {
    }

    ~CreateNewHJSliceArgs() override = default;
    uint64_t rightValueSize;
    JoinBuildSideType joinBuildSide;
};

/// As a hash join has left and right side, we need to handle the left and right side of the join with one slice
/// Thus, we use a HashMapSlice and set the number of input streams to 2 in its constructor
class HJSlice final : public HashMapSlice
{
public:
    /// All hash maps are created eagerly by the HashMapSlice base constructor, so the SHARED_TABLE build variant
    /// needs no extra pre-creation: routing every worker to map 0 is race-free by construction.
    /// Right-side maps are re-initialized with rightValueSize when it differs from the left value size.
    HJSlice(
        AbstractBufferProvider& bufferProvider,
        SliceStart sliceStart,
        SliceEnd sliceEnd,
        const CreateNewHJSliceArgs& createNewHashMapSliceArgs,
        uint64_t numberOfHashMaps);
    [[nodiscard]] const TupleBuffer* getHashMapBufferRefForSide(WorkerThreadId workerThreadId, const JoinBuildSideType& buildSide) const;
    [[nodiscard]] uint64_t getNumberOfHashMapsForSide() const;

    /// Eager trigger (T2, symmetric hash join): every {insert own + probe opposite} runs under this single
    /// per-slice mutex. The strict total order over both sides' inserts makes pairing exactly-once without
    /// sequence numbers: the later insert always sees the earlier, completed one and never itself.
    /// ponytail: one global eager lock per slice -- the classic symmetric-hash-join price; per-bucket
    /// latches are the upgrade path if eager throughput ever matters.
    void eagerLock() { eagerMutex.lock(); }
    void eagerUnlock() { eagerMutex.unlock(); }
    /// Records one matched (left entry, right entry) pointer pair; caller holds the eager mutex.
    void eagerRecordPair(const uint64_t leftEntry, const uint64_t rightEntry) { eagerPairs.push_back({leftEntry, rightEntry}); }
    /// Drain accessors, called at trigger time only (no concurrent inserts remain).
    [[nodiscard]] uint64_t eagerPairCount() const { return eagerPairs.size(); }
    [[nodiscard]] uint64_t eagerPairLeft(const uint64_t i) const { return eagerPairs[i][0]; }
    [[nodiscard]] uint64_t eagerPairRight(const uint64_t i) const { return eagerPairs[i][1]; }

private:
    /// Value size for right-side maps; the base createNewHashMapSliceArgs (stored sliced in HashMapSlice) keeps the
    /// left side's sizes.
    uint64_t rightValueSize;
    /// Eager state; see eagerLock().
    std::mutex eagerMutex;
    std::vector<std::array<uint64_t, 2>> eagerPairs;
};

}
