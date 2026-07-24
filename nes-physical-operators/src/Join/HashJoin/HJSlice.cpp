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
#include <Join/HashJoin/HJSlice.hpp>

#include <cstdint>
#include <memory>
#include <tuple>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>
#include <SliceStore/Slice.hpp>
#include <ErrorHandling.hpp>
#include <HashMapSlice.hpp>

namespace NES
{
HJSlice::HJSlice(
    AbstractBufferProvider& bufferProvider,
    SliceStart sliceStart,
    SliceEnd sliceEnd,
    const CreateNewHJSliceArgs& createNewHashMapSliceArgs,
    const uint64_t numberOfHashMaps)
    : HashMapSlice(bufferProvider, std::move(sliceStart), std::move(sliceEnd), createNewHashMapSliceArgs, numberOfHashMaps, 2)
    , rightValueSize(createNewHashMapSliceArgs.rightValueSize)
{
    /// The base constructor eagerly initialized every map with the LEFT side's value size. The two sides may have
    /// different value sizes (e.g., for the TUPLE_CHAINED storage variant, which stores the tuples inline), so
    /// re-initialize the right half (indices [perStream, 2*perStream)) with the right side's entry size.
    if (rightValueSize != createNewHashMapSliceArgs.valueSize)
    {
        const auto perStream = getNumHashMapsPerInputStream();
        for (uint64_t i = perStream; i < 2 * perStream; ++i)
        {
            ChainedHashMap::init(
                hashMapBuffers[i],
                createNewHashMapSliceArgs.keySize,
                rightValueSize,
                createNewHashMapSliceArgs.numberOfBuckets,
                createNewHashMapSliceArgs.pageSize);
        }
    }
}

[[nodiscard]] const TupleBuffer*
HJSlice::getHashMapBufferRefForSide(WorkerThreadId workerThreadId, const JoinBuildSideType& buildSide) const
{
    /// Hashmaps of the left build side come before right
    auto pos = (workerThreadId % getNumHashMapsPerInputStream())
        + ((static_cast<uint64_t>(buildSide == JoinBuildSideType::Right) * getNumHashMapsPerInputStream()));
    const auto numHashMaps = getNumberOfHashMaps();
    INVARIANT(
        numHashMaps > 0 and pos < numHashMaps,
        "No hashmap found for workerThreadId {} at pos {} for {} hashmaps",
        workerThreadId,
        pos,
        numHashMaps);
    const VariableSizedAccess::Index bufferIndex(pos);
    return getHashMapBufferRef(bufferIndex);
}

uint64_t HJSlice::getNumberOfHashMapsForSide() const
{
    return getNumHashMapsPerInputStream();
}

}
