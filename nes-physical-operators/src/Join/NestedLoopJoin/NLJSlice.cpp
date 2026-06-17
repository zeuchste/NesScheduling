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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <numeric>
#include <string>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Interface/PagedVector/PagedVector.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Spill/ArenaMemoryResource.hpp>
#include <Runtime/Spill/SpillManager.hpp>
#include <SliceStore/Slice.hpp>

namespace NES
{

namespace
{
std::atomic<uint64_t> nljSpillSliceCounter{0};
/// The per-slice BufferManager's pooled area is unused (PagedVector allocates via getUnpooledBuffer, which routes
/// through the arena), so it is kept tiny; this page size only sizes that unused pooled area.
constexpr uint32_t NLJ_SLICE_POOL_PAGE_SIZE = 4096;
constexpr uint32_t NLJ_SLICE_POOL_BUFFERS = 2;
}

NLJSlice::NLJSlice(
    const SliceStart sliceStart, const SliceEnd sliceEnd, const uint64_t numberOfWorkerThreads, std::shared_ptr<SpillManager> spillManager)
    : Slice(sliceStart, sliceEnd)
    , spillManager(std::move(spillManager))
    , spillEnabled(this->spillManager != nullptr && this->spillManager->configuration().enabled)
{
    /// When spilling is enabled, create the slice's arena up front so every PagedVector can be pinned to it.
    AbstractBufferProvider* sliceProvider = nullptr;
    if (spillEnabled)
    {
        const auto& config = this->spillManager->configuration();
        const auto backingFile = config.spillDirectory + "/nes-nljslice-" + std::to_string(nljSpillSliceCounter.fetch_add(1)) + ".spill";
        spillArena = std::make_shared<ArenaMemoryResource>(config.arenaMode, backingFile);
        spillBufferManager = BufferManager::create(NLJ_SLICE_POOL_PAGE_SIZE, NLJ_SLICE_POOL_BUFFERS, spillArena);
        sliceProvider = spillBufferManager.get();
    }

    for (uint64_t i = 0; i < numberOfWorkerThreads; ++i)
    {
        auto pagedVector = std::make_unique<PagedVector>();
        if (sliceProvider != nullptr)
        {
            pagedVector->setOwnBufferProvider(sliceProvider);
        }
        leftPagedVectors.emplace_back(std::move(pagedVector));
    }

    for (uint64_t i = 0; i < numberOfWorkerThreads; ++i)
    {
        auto pagedVector = std::make_unique<PagedVector>();
        if (sliceProvider != nullptr)
        {
            pagedVector->setOwnBufferProvider(sliceProvider);
        }
        rightPagedVectors.emplace_back(std::move(pagedVector));
    }
}

NLJSlice::~NLJSlice()
{
    if (spillManager != nullptr)
    {
        spillManager->unregisterState(this);
    }
    /// Releasing the PagedVectors' TupleBuffers touches buffer control blocks that live in the arena; reload first so
    /// that memory is valid rather than discarded (zero-fill) pages.
    if (spillEvicted.load() && spillArena != nullptr)
    {
        spillArena->reload();
        spillEvicted.store(false);
    }
}

size_t NLJSlice::residentBytes() const
{
    if (spillEvicted.load() || spillArena == nullptr)
    {
        return 0;
    }
    return spillArena->liveBytes();
}

bool NLJSlice::isEvicted() const
{
    return spillEvicted.load();
}

void NLJSlice::evictState()
{
    if (spillArena != nullptr && !spillEvicted.load())
    {
        spillArena->evict();
        spillEvicted.store(true);
    }
}

void NLJSlice::reloadState()
{
    if (spillArena != nullptr && spillEvicted.load())
    {
        spillArena->reload();
        spillEvicted.store(false);
    }
}

uint64_t NLJSlice::coldnessKey() const
{
    return sliceEnd.getRawValue();
}

uint64_t NLJSlice::getNumberOfTuplesLeft() const
{
    return std::accumulate(
        leftPagedVectors.begin(),
        leftPagedVectors.end(),
        0,
        [](uint64_t sum, const auto& pagedVector) { return sum + pagedVector->getTotalNumberOfEntries(); });
}

uint64_t NLJSlice::getNumberOfTuplesRight() const
{
    return std::accumulate(
        rightPagedVectors.begin(),
        rightPagedVectors.end(),
        0,
        [](uint64_t sum, const auto& pagedVector) { return sum + pagedVector->getTotalNumberOfEntries(); });
}

PagedVector* NLJSlice::getPagedVectorRefLeft(const WorkerThreadId workerThreadId) const
{
    const auto pos = workerThreadId % leftPagedVectors.size();
    return leftPagedVectors[pos].get();
}

PagedVector* NLJSlice::getPagedVectorRefRight(const WorkerThreadId workerThreadId) const
{
    const auto pos = workerThreadId % rightPagedVectors.size();
    return rightPagedVectors[pos].get();
}

PagedVector* NLJSlice::getPagedVectorRef(const WorkerThreadId workerThreadId, const JoinBuildSideType joinBuildSide) const
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

    /// Append all PagedVectors on the left join side and erase all items except for the first one
    /// We do this to ensure that we have only one PagedVector for each side during the probing phase
    if (leftPagedVectors.size() > 1)
    {
        for (uint64_t i = 1; i < leftPagedVectors.size(); ++i)
        {
            leftPagedVectors[0]->moveAllPages(*leftPagedVectors[i]);
        }
        leftPagedVectors.erase(leftPagedVectors.begin() + 1, leftPagedVectors.end());
    }

    /// Append all PagedVectors on the right join side and remove all items except for the first one
    if (rightPagedVectors.size() > 1)
    {
        for (uint64_t i = 1; i < rightPagedVectors.size(); ++i)
        {
            rightPagedVectors[0]->moveAllPages(*rightPagedVectors[i]);
        }
        rightPagedVectors.erase(rightPagedVectors.begin() + 1, rightPagedVectors.end());
    }
}
}
