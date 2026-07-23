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

#include <cstdint>
#include <unordered_map>
#include <folly/Synchronized.h>
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
    const uint64_t tupleSizeRight)
    : NLJSlice(bufferProvider, sliceStart, sliceEnd, numberOfWorkerThreads, tupleSizeLeft, tupleSizeRight)
    , numberOfWorkerThreads(numberOfWorkerThreads)
{
    auto reg = vectorBufferRegistry().wlock();
    for (uint64_t worker = 0; worker < numberOfWorkerThreads; ++worker)
    {
        for (const auto side : {JoinBuildSideType::Left, JoinBuildSideType::Right})
        {
            if (const auto* buffer = getPagedVectorTupleBufferRef(WorkerThreadId(worker), side))
            {
                (*reg)[buffer->getAvailableMemoryArea().data()] = this;
            }
        }
    }
}

IXJSlice::~IXJSlice()
{
    auto reg = vectorBufferRegistry().wlock();
    std::erase_if(*reg, [this](const auto& kv) { return kv.second == this; });
}

folly::Synchronized<std::unordered_map<const void*, IXJSlice*>>& IXJSlice::vectorBufferRegistry()
{
    static folly::Synchronized<std::unordered_map<const void*, IXJSlice*>> registry;
    return registry;
}

IXJSlice* IXJSlice::fromVectorBuffer(const void* vectorMemArea)
{
    const auto reg = vectorBufferRegistry().rlock();
    const auto it = reg->find(vectorMemArea);
    INVARIANT(it != reg->end(), "No IXJSlice registered for vector buffer {}", vectorMemArea);
    return it->second;
}

void IXJSlice::insertIndexEntry(const JoinBuildSideType side, const WorkerThreadId workerThreadId, const uint64_t keyHash)
{
    /// Position of the tuple that is about to be appended = current count of the caller's own vector.
    const auto* vectorBuffer = getPagedVectorTupleBufferRef(workerThreadId, side);
    const auto position = PagedVector::load(*vectorBuffer).getTotalNumberOfRecords();
    const auto worker = workerThreadId % numberOfWorkerThreads;
    INVARIANT(position <= POSITION_MASK, "IXJ index position overflow");
    const auto packed = (static_cast<uint64_t>(worker) << WORKER_SHIFT) | position;
    indexFor(side).wlock()->emplace(keyHash, packed);
}

IXJSlice::LookupState* IXJSlice::startLookup(const JoinBuildSideType side, const uint64_t keyHash) const
{
    /// Called at probe time only: the slice is closed, no concurrent inserts remain, but we still take the
    /// read lock to be safe against stragglers.
    const auto locked = indexFor(side).rlock();
    const auto [first, last] = locked->equal_range(keyHash);
    if (first == last)
    {
        return nullptr;
    }
    auto* state = new LookupState();
    for (auto it = first; it != last; ++it)
    {
        state->matches.push_back(it->second);
    }
    return state;
}

}
