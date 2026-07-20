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
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Join/HashJoin/HJSlice.hpp>
#include <Join/StreamJoinOperatorHandler.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Sequencing/SequenceData.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

namespace
{
/// Collects non-empty hash maps for one build side from a single slice
std::vector<HashMap*> getHashMapsFromSlice(const Slice& slice, JoinBuildSideType side)
{
    std::vector<HashMap*> maps;
    const auto* hjSlice = dynamic_cast<const HJSlice*>(&slice);
    INVARIANT(hjSlice != nullptr, "Slice must be of type HJSlice!");
    for (uint64_t i = 0; i < hjSlice->getNumberOfHashMapsForSide(); ++i)
    {
        if (auto* map = hjSlice->getHashMapPtr(WorkerThreadId(i), side); map != nullptr && map->getNumberOfTuples() > 0)
        {
            maps.emplace_back(map);
        }
    }
    return maps;
}

/// Collects non-empty hash maps from multiple slices for one build side
std::vector<HashMap*> getHashMapsFromSlices(const std::vector<std::shared_ptr<Slice>>& slices, JoinBuildSideType side)
{
    std::vector<HashMap*> allMaps;
    for (const auto& slice : slices)
    {
        for (auto* map : getHashMapsFromSlice(*slice, side))
        {
            allMaps.emplace_back(map);
        }
    }
    return allMaps;
}
}

HJOperatorHandler::HJOperatorHandler(
    const std::vector<OriginId>& inputOrigins,
    const OriginId outputOriginId,
    std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore,
    const uint64_t maxNumberOfBuckets,
    JoinTriggerStrategy triggerStrategy,
    const JoinProcessingVariant processingVariant,
    const std::optional<uint64_t> fixedNumberOfBuckets)
    : StreamJoinOperatorHandler(inputOrigins, outputOriginId, std::move(sliceAndWindowStore), std::move(triggerStrategy))
    , setupAlreadyCalledLeft(false)
    , setupAlreadyCalledRight(false)
    , leftRollingAverageNumberOfKeys(RollingAverage<uint64_t>{100})
    , rightRollingAverageNumberOfKeys(RollingAverage<uint64_t>{100})
    , maxNumberOfBuckets(maxNumberOfBuckets)
    , processingVariant(processingVariant)
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

    /// P3 (SHARED_TABLE): one hash map per side shared by all worker threads. The map is created eagerly in the
    /// slice constructor so that no two threads race on its lazy creation.
    const auto sharedTable = processingVariant == JoinProcessingVariant::SHARED_TABLE;
    const auto numberOfHashMaps = sharedTable ? 1UL : numberOfWorkerThreads;
    return std::function(
        [outputOriginId = outputOriginId, numberOfHashMaps, sharedTable, copyOfNewHashMapArgs = newHashMapArgs](
            SliceStart sliceStart, SliceEnd sliceEnd) -> std::vector<std::shared_ptr<Slice>>
        {
            NES_TRACE("Creating new hash-join slice for slice {}-{} for output origin {}", sliceStart, sliceEnd, outputOriginId);
            return {std::make_shared<HJSlice>(sliceStart, sliceEnd, copyOfNewHashMapArgs, numberOfHashMaps, sharedTable)};
        });
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

void HJOperatorHandler::setNautilusCleanupExec(
    std::shared_ptr<CreateNewHashMapSliceArgs::NautilusCleanupExec> nautilusCleanupExec, const JoinBuildSideType& buildSide)
{
    switch (buildSide)
    {
        case JoinBuildSideType::Right:
            rightCleanupStateNautilusFunction = std::move(nautilusCleanupExec);
            break;
        case JoinBuildSideType::Left:
            leftCleanupStateNautilusFunction = std::move(nautilusCleanupExec);
            break;
            std::unreachable();
    }
}

std::vector<std::shared_ptr<CreateNewHashMapSliceArgs::NautilusCleanupExec>> HJOperatorHandler::getNautilusCleanupExec() const
{
    return {leftCleanupStateNautilusFunction, rightCleanupStateNautilusFunction};
}

void HJOperatorHandler::createProbeTasks(
    const ProbeWorkItem& workItem,
    const WindowInfo& windowInfo,
    PipelineExecutionContext* pipelineCtx,
    std::vector<TupleBuffer>& probeTasks)
{
    const auto probeTaskType = workItem.probeTaskType;
    const auto leftHashMaps = getHashMapsFromSlices(workItem.leftSlices, JoinBuildSideType::Left);
    const auto rightHashMaps = getHashMapsFromSlices(workItem.rightSlices, JoinBuildSideType::Right);

    /// Update rolling average (accumulate locally, single lock acquisition)
    {
        uint64_t totalTuples = 0;
        uint64_t mapCount = 0;
        for (const auto* map : leftHashMaps)
        {
            totalTuples += map->getNumberOfTuples();
            ++mapCount;
        }
        if (mapCount > 0)
        {
            leftRollingAverageNumberOfKeys.wlock()->add(totalTuples / mapCount);
        }

        /// Resetting before updating the right side
        totalTuples = 0;
        mapCount = 0;
        for (const auto* map : rightHashMaps)
        {
            totalTuples += map->getNumberOfTuples();
            ++mapCount;
        }
        if (mapCount > 0)
        {
            rightRollingAverageNumberOfKeys.wlock()->add(totalTuples / mapCount);
        }
    }

    /// Creates one probe task buffer over the given hash-map subsets; sequence/chunk data is stamped centrally
    /// in StreamJoinOperatorHandler::triggerSlices.
    const auto createTask = [&](const std::vector<HashMap*>& left, const std::vector<HashMap*>& right)
    {
        uint64_t totalNumberOfTuples = 0;
        for (const auto* map : left)
        {
            totalNumberOfTuples += map->getNumberOfTuples();
        }
        for (const auto* map : right)
        {
            totalNumberOfTuples += map->getNumberOfTuples();
        }

        const auto neededBufferSize = sizeof(EmittedHJWindowTrigger) + ((left.size() + right.size()) * sizeof(HashMap*));
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

        new (tupleBuffer.getAvailableMemoryArea().data()) EmittedHJWindowTrigger{windowInfo, left, right, probeTaskType};
        probeTasks.emplace_back(std::move(tupleBuffer));
    };

    /// Null-fill tasks are never split: deciding that a tuple has no match requires seeing ALL opposite maps.
    /// P1 (SINGLE_TASK) and P3 (SHARED_TABLE, one map per side anyway) also emit exactly one task per work item.
    const auto splittable = probeTaskType == ProbeTaskType::MATCH_PAIRS and not leftHashMaps.empty() and not rightHashMaps.empty();
    switch (splittable ? processingVariant : JoinProcessingVariant::SINGLE_TASK)
    {
        case JoinProcessingVariant::SINGLE_TASK:
        case JoinProcessingVariant::SHARED_TABLE:
            createTask(leftHashMaps, rightHashMaps);
            break;
        case JoinProcessingVariant::TASK_PER_PAIR:
            /// P2: one probe task per (left map, right map) pair — the thread-local build maps provide the
            /// partitioning for free and each pair is probed without shared state.
            for (auto* leftMap : leftHashMaps)
            {
                for (auto* rightMap : rightHashMaps)
                {
                    createTask({leftMap}, {rightMap});
                }
            }
            break;
        case JoinProcessingVariant::BROADCAST:
            /// P4: one probe task per left map, each carrying the full right side — content-insensitive work
            /// division, immune to key skew, at a memory-bandwidth premium.
            for (auto* leftMap : leftHashMaps)
            {
                createTask({leftMap}, rightHashMaps);
            }
            break;
    }
}

}
