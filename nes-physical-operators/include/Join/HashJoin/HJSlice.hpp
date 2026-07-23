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
    /// as the storage variant stores the tuples inline (SHARED_CHAINS): each side then needs its own entry size.
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

private:
    /// Value size for right-side maps; the base createNewHashMapSliceArgs (stored sliced in HashMapSlice) keeps the
    /// left side's sizes.
    uint64_t rightValueSize;
};

}
