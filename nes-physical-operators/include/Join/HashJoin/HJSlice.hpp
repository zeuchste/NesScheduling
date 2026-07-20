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
#include <Identifiers/Identifiers.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <SliceStore/Slice.hpp>
#include <HashMapSlice.hpp>

namespace NES
{

struct CreateNewHJSliceArgs final : CreateNewHashMapSliceArgs
{
    /// The base keySize/valueSize describe the LEFT side; rightValueSize the RIGHT side. The sides always share the
    /// same key layout (the lowering casts both sides' keys to identical types), but their value sizes differ as soon
    /// as the storage variant stores the tuples inline (SHARED_CHAINS): each side then needs its own entry size.
    CreateNewHJSliceArgs(
        std::vector<std::shared_ptr<NautilusCleanupExec>> nautilusCleanup,
        const uint64_t keySize,
        const uint64_t valueSize,
        const uint64_t rightValueSize,
        const uint64_t pageSize,
        const uint64_t numberOfBuckets,
        const JoinBuildSideType joinBuildSide)
        : CreateNewHashMapSliceArgs{std::move(nautilusCleanup), keySize, valueSize, pageSize, numberOfBuckets}
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
    /// If preCreateHashMaps is set, all hash maps are created eagerly in the constructor. This is required for the
    /// SHARED_TABLE processing variant (one map per side shared by all worker threads), where lazy creation would
    /// race between the building threads.
    HJSlice(
        SliceStart sliceStart,
        SliceEnd sliceEnd,
        const CreateNewHJSliceArgs& createNewHashMapSliceArgs,
        uint64_t numberOfHashMaps,
        bool preCreateHashMaps = false);
    [[nodiscard]] HashMap* getHashMapPtr(WorkerThreadId workerThreadId, const JoinBuildSideType& buildSide) const;
    [[nodiscard]] HashMap* getHashMapPtrOrCreate(WorkerThreadId workerThreadId, const JoinBuildSideType& buildSide);
    [[nodiscard]] uint64_t getNumberOfHashMapsForSide() const;

private:
    /// Value size for right-side maps; the base createNewHashMapSliceArgs (stored sliced in HashMapSlice) keeps the
    /// left side's sizes.
    uint64_t rightValueSize;
};

}
