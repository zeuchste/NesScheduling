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


#include <Join/HashJoin/HJOperatorHandler.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <tuple>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Join/HashJoin/HJSlice.hpp>
#include <Join/StreamJoinOperatorHandler.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sequencing/SequenceData.hpp>
#include <Time/Timestamp.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

namespace
{
/// Collects non-empty hash map buffers for one build side from a single slice
std::vector<TupleBuffer> getHashMapsFromSlice(const Slice& slice, JoinBuildSideType side)
{
    std::vector<TupleBuffer> buffers;
    const auto* hjSlice = dynamic_cast<const HJSlice*>(&slice);
    INVARIANT(hjSlice != nullptr, "Slice must be of type HJSlice!");
    for (uint64_t i = 0; i < hjSlice->getNumberOfHashMapsForSide(); ++i)
    {
        const auto* hashMapBuffer = hjSlice->getHashMapBufferRefForSide(WorkerThreadId(i), side);
        if (const auto hashMap = ChainedHashMap::load(*hashMapBuffer); hashMap.getTotalNumberOfRecords() > 0)
        {
            buffers.emplace_back(*hashMapBuffer);
        }
    }
    return buffers;
}

/// Collects non-empty hash map buffers from multiple slices for one build side
std::vector<TupleBuffer> getHashMapsFromSlices(const std::vector<std::shared_ptr<Slice>>& slices, JoinBuildSideType side)
{
    std::vector<TupleBuffer> allBuffers;
    for (const auto& slice : slices)
    {
        for (auto& buffer : getHashMapsFromSlice(*slice, side))
        {
            allBuffers.emplace_back(buffer);
        }
    }
    return allBuffers;
}
}

HJOperatorHandler::HJOperatorHandler(
    const std::vector<OriginId>& inputOrigins,
    const OriginId outputOriginId,
    std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore,
    const uint64_t maxNumberOfBuckets,
    JoinTriggerStrategy triggerStrategy,
    const JoinBuildVariant buildVariant,
    const JoinProbeVariant probeVariant,
    const uint64_t probeRanges,
    const std::optional<uint64_t> fixedNumberOfBuckets)
    : StreamJoinOperatorHandler(inputOrigins, outputOriginId, std::move(sliceAndWindowStore), std::move(triggerStrategy))
    , setupAlreadyCalledLeft(false)
    , setupAlreadyCalledRight(false)
    , leftRollingAverageNumberOfKeys(RollingAverage<uint64_t>{100})
    , rightRollingAverageNumberOfKeys(RollingAverage<uint64_t>{100})
    , maxNumberOfBuckets(maxNumberOfBuckets)
    , buildVariant(buildVariant)
    , probeVariant(probeVariant)
    , probeRanges(probeRanges)
    , fixedNumberOfBuckets(fixedNumberOfBuckets)
{
}

std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>
HJOperatorHandler::getCreateNewSlicesFunction(const CreateNewSlicesArguments& newSlicesArguments) const
{
    PRECONDITION(
        numberOfWorkerThreads > 0, "Number of worker threads not set for window based operator. Has setWorkerThreads() being called?");

    auto newHashMapArgs = dynamic_cast<const CreateNewHJSliceArgs&>(newSlicesArguments);
    if (fixedNumberOfBuckets.has_value())
    {
        /// S3 (FIXED_ARRAY): bucket array sized from the estimated key cardinality, no adaptive resizing.
        newHashMapArgs.numberOfBuckets = *fixedNumberOfBuckets;
    }
    else
    {
        switch (newHashMapArgs.joinBuildSide)
        {
            case JoinBuildSideType::Left:
                newHashMapArgs.numberOfBuckets = std::clamp(leftRollingAverageNumberOfKeys.rlock()->getAverage(), 1UL, maxNumberOfBuckets);
                break;
            case JoinBuildSideType::Right:
                newHashMapArgs.numberOfBuckets
                    = std::clamp(rightRollingAverageNumberOfKeys.rlock()->getAverage(), 1UL, maxNumberOfBuckets);
                break;
        }
    }

    /// B2 (SHARED_TABLE): one hash map per side shared by all worker threads. The map is created eagerly in the
    /// slice constructor so that no two threads race on its lazy creation.
    const auto sharedTable = buildVariant == JoinBuildVariant::SHARED_TABLE;
    const auto numberOfHashMaps = sharedTable ? 1UL : numberOfWorkerThreads;
    return std::function(
        [bufferProvider = newHashMapArgs.bufferProvider,
         outputOriginId = outputOriginId,
         numberOfHashMaps,
         copyOfNewHashMapArgs = newHashMapArgs](SliceStart sliceStart, SliceEnd sliceEnd) -> std::vector<std::shared_ptr<Slice>>
        {
            NES_TRACE("Creating new hash-join slice for slice {}-{} for output origin {}", sliceStart, sliceEnd, outputOriginId);
            return {std::make_shared<HJSlice>(*bufferProvider, sliceStart, sliceEnd, copyOfNewHashMapArgs, numberOfHashMaps)};
        });
}

HJSlice* HJOperatorHandler::sliceForEagerInsert(const Timestamp ts, const WorkerThreadId workerThreadId)
{
    auto& cache = eagerInsertCaches[workerThreadId % MAX_CACHED_WORKERS];
    const auto raw = ts.getRawValue();
    if (cache.slice != nullptr and cache.start <= raw and raw < cache.end)
    {
        return cache.slice;
    }
    const auto slices = getSliceAndWindowStore().getSlicesOrCreate(
        ts,
        [](SliceStart, SliceEnd) -> std::vector<std::shared_ptr<Slice>>
        {
            INVARIANT(false, "HJ eager insert requires the slice to exist (the hash-map extractor creates it first)");
            return {};
        });
    INVARIANT(not slices.empty(), "No slice found for timestamp {}", ts);
    auto* hjSlice = dynamic_cast<HJSlice*>(slices.front().get());
    INVARIANT(hjSlice != nullptr, "Slice for timestamp {} is not an HJSlice", ts);
    cache = {hjSlice->getSliceStart().getRawValue(), hjSlice->getSliceEnd().getRawValue(), hjSlice};
    return hjSlice;
}

bool HJOperatorHandler::wasSetupCalled(const JoinBuildSideType& buildSide)
{
    switch (buildSide)
    {
        case JoinBuildSideType::Right: {
            bool expectedValue = false;
            return not setupAlreadyCalledRight.compare_exchange_strong(expectedValue, true);
        }
        case JoinBuildSideType::Left: {
            bool expectedValue = false;
            return not setupAlreadyCalledLeft.compare_exchange_strong(expectedValue, true);
        }
    }

    std::unreachable();
}

void HJOperatorHandler::createProbeTasks(
    const ProbeWorkItem& workItem,
    const WindowInfo& windowInfo,
    PipelineExecutionContext* pipelineCtx,
    std::vector<TupleBuffer>& probeTasks)
{
    const auto probeTaskType = workItem.probeTaskType;
    const auto leftHashMapBuffers = getHashMapsFromSlices(workItem.leftSlices, JoinBuildSideType::Left);
    const auto rightHashMapBuffers = getHashMapsFromSlices(workItem.rightSlices, JoinBuildSideType::Right);

    /// Update rolling average (accumulate locally, single lock acquisition)
    {
        uint64_t totalTuples = 0;
        uint64_t mapCount = 0;
        for (const auto& buffer : leftHashMapBuffers)
        {
            totalTuples += ChainedHashMap::load(buffer).getTotalNumberOfRecords();
            ++mapCount;
        }
        if (mapCount > 0)
        {
            leftRollingAverageNumberOfKeys.wlock()->add(totalTuples / mapCount);
        }

        /// Resetting before updating the right side
        totalTuples = 0;
        mapCount = 0;
        for (const auto& buffer : rightHashMapBuffers)
        {
            totalTuples += ChainedHashMap::load(buffer).getTotalNumberOfRecords();
            ++mapCount;
        }
        if (mapCount > 0)
        {
            rightRollingAverageNumberOfKeys.wlock()->add(totalTuples / mapCount);
        }
    }

    /// Creates one probe task buffer over the given hash-map buffer subsets; sequence/chunk data is stamped
    /// centrally in StreamJoinOperatorHandler::triggerSlices. The hash map buffers are stored as child buffers of
    /// the task buffer: left at indices [0, left.size()), right following at [left.size(), left.size() + right.size()).
    const auto createTask = [&](const std::vector<TupleBuffer>& left,
                                const std::vector<TupleBuffer>& right,
                                const uint64_t rightPageStart = 0,
                                const uint64_t rightPageEnd = EmittedHJWindowTrigger::FULL_RANGE)
    {
        uint64_t totalNumberOfTuples = 0;
        for (const auto& buffer : left)
        {
            totalNumberOfTuples += ChainedHashMap::load(buffer).getTotalNumberOfRecords();
        }
        for (const auto& buffer : right)
        {
            totalNumberOfTuples += ChainedHashMap::load(buffer).getTotalNumberOfRecords();
        }

        /// Hash map buffers are stored as child buffers, not inline in the main buffer.
        constexpr auto neededBufferSize = sizeof(EmittedHJWindowTrigger);
        const auto tupleBufferVal = pipelineCtx->getBufferManager()->getUnpooledBuffer(neededBufferSize);
        if (not tupleBufferVal.has_value())
        {
            throw CannotAllocateBuffer("{}B for the hash join window trigger were requested", neededBufferSize);
        }

        auto tupleBuffer = tupleBufferVal.value();
        tupleBuffer.setOriginId(outputOriginId);
        tupleBuffer.setWatermark(windowInfo.windowStart);
        tupleBuffer.setNumberOfTuples(totalNumberOfTuples);
        tupleBuffer.setCreationTimestampInMS(Timestamp(
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count()));

        /// Writing all necessary information for the probe to the buffer via the placement constructor
        new (tupleBuffer.getAvailableMemoryArea().data())
            EmittedHJWindowTrigger{windowInfo, left.size(), right.size(), probeTaskType, rightPageStart, rightPageEnd};

        /// Store left hash map buffers as children first (indices 0..left-1), then right (indices left..left+right-1)
        for (auto leftBuffer : left)
        {
            std::ignore = tupleBuffer.storeChildBuffer(leftBuffer);
        }
        for (auto rightBuffer : right)
        {
            std::ignore = tupleBuffer.storeChildBuffer(rightBuffer);
        }
        probeTasks.emplace_back(std::move(tupleBuffer));
    };

    /// Null-fill tasks are never split: deciding that a tuple has no match requires seeing ALL opposite maps.
    const auto splittable
        = probeTaskType == ProbeTaskType::MATCH_PAIRS and not leftHashMapBuffers.empty() and not rightHashMapBuffers.empty();
    switch (splittable ? probeVariant : JoinProbeVariant::SINGLE_TASK)
    {
        case JoinProbeVariant::SINGLE_TASK:
            createTask(leftHashMapBuffers, rightHashMapBuffers);
            break;
        case JoinProbeVariant::TABLE_BROADCAST:
            /// One probe task per left table, each carrying the full right side - content-insensitive work
            /// division, immune to key skew, at a memory-bandwidth premium.
            for (const auto& leftBuffer : leftHashMapBuffers)
            {
                createTask({leftBuffer}, rightHashMapBuffers);
            }
            break;
        case JoinProbeVariant::TASK_PER_PAIR:
            /// One probe task per (left table, right table) pair - the thread-local build tables provide the
            /// partitioning for free and each pair is probed without shared state.
            for (const auto& leftBuffer : leftHashMapBuffers)
            {
                for (const auto& rightBuffer : rightHashMapBuffers)
                {
                    createTask({leftBuffer}, {rightBuffer});
                }
            }
            break;
        case JoinProbeVariant::BUCKET_RANGES: {
            /// One probe task per (pair, storage-page range of the right table) - the finest granularity,
            /// and the only one that still parallelizes the probe under the SHARED_TABLE build.
            const auto ranges = std::max<uint64_t>(1, probeRanges != 0 ? probeRanges : numberOfWorkerThreads);
            for (const auto& leftBuffer : leftHashMapBuffers)
            {
                for (const auto& rightBuffer : rightHashMapBuffers)
                {
                    const auto pages = ChainedHashMap::load(rightBuffer).getNumberOfPages();
                    if (pages == 0)
                    {
                        createTask({leftBuffer}, {rightBuffer});
                        continue;
                    }
                    const auto pagesPerRange = (pages + ranges - 1) / ranges;
                    for (uint64_t start = 0; start < pages; start += pagesPerRange)
                    {
                        createTask({leftBuffer}, {rightBuffer}, start, std::min(start + pagesPerRange, pages));
                    }
                }
            }
            break;
        }
    }
}

}
