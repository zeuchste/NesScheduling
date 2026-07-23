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
#include <algorithm>
#include <bit>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <utility>
#include <vector>
#include <Util/StreamJoinKnobs.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Join/StreamJoinOperatorHandler.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Sequencing/SequenceData.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Util/RollingAverage.hpp>
#include <HashMapSlice.hpp>

namespace NES
{

/// This task models the information for a hash join based window trigger
struct EmittedHJWindowTrigger
{
    /// Full-range sentinel: beginRange() clamps to the number of pages, so [0, FULL_RANGE) is the whole table.
    static constexpr uint64_t FULL_RANGE = UINT64_MAX;

    EmittedHJWindowTrigger(
        const WindowInfo windowInfo,
        const uint64_t leftNumberOfHashMaps,
        const uint64_t rightNumberOfHashMaps,
        ProbeTaskType probeTaskType,
        const uint64_t rightPageStart = 0,
        const uint64_t rightPageEnd = FULL_RANGE)
        : windowInfo(windowInfo)
        , leftNumberOfHashMaps(leftNumberOfHashMaps)
        , rightNumberOfHashMaps(rightNumberOfHashMaps)
        , probeTaskType(probeTaskType)
        , rightPageStart(rightPageStart)
        , rightPageEnd(rightPageEnd)
    {
    }

    WindowInfo windowInfo;
    uint64_t leftNumberOfHashMaps;
    uint64_t rightNumberOfHashMaps;
    ProbeTaskType probeTaskType;
    /// Storage-page range of the RIGHT tables that this task probes (BUCKET_RANGES); [0, FULL_RANGE) otherwise.
    uint64_t rightPageStart;
    uint64_t rightPageEnd;
};

class HJOperatorHandler final : public StreamJoinOperatorHandler
{
public:
    HJOperatorHandler(
        const std::vector<OriginId>& inputOrigins,
        OriginId outputOriginId,
        std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore,
        uint64_t maxNumberOfBuckets,
        JoinTriggerStrategy triggerStrategy,
        JoinBuildVariant buildVariant = JoinBuildVariant::LOCAL_TABLES,
        JoinProbeVariant probeVariant = JoinProbeVariant::SINGLE_TASK,
        uint64_t probeRanges = 0,
        std::optional<uint64_t> fixedNumberOfBuckets = std::nullopt);

    [[nodiscard]] std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>
    getCreateNewSlicesFunction(const CreateNewSlicesArguments& newSlicesArguments) const override;

    bool wasSetupCalled(const JoinBuildSideType& buildSide);

private:
    /// Is required to not perform the setup again and resolving a race condition to the cleanup state function
    std::atomic<bool> setupAlreadyCalledLeft;
    std::atomic<bool> setupAlreadyCalledRight;

protected:
    void createProbeTasks(
        const ProbeWorkItem& workItem,
        const WindowInfo& windowInfo,
        PipelineExecutionContext* pipelineCtx,
        std::vector<TupleBuffer>& probeTasks) override;

    folly::Synchronized<RollingAverage<uint64_t>> leftRollingAverageNumberOfKeys;
    folly::Synchronized<RollingAverage<uint64_t>> rightRollingAverageNumberOfKeys;
    uint64_t maxNumberOfBuckets;
    /// B1/B2: one build table per worker thread and side, or one shared per side.
    JoinBuildVariant buildVariant;
    /// P1-P4: granularity of the probe tasks created per work item.
    JoinProbeVariant probeVariant;
    /// BUCKET_RANGES: ranges per table pair (0 = number of worker threads).
    uint64_t probeRanges;
    /// S3 (FIXED_ARRAY): fixed bucket-array size from the estimated key cardinality; disables the
    /// rolling-average-based adaptive sizing.
    std::optional<uint64_t> fixedNumberOfBuckets;
};

}
