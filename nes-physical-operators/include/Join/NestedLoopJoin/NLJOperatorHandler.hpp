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
#include <memory>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Join/StreamJoinOperatorHandler.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>

namespace NES
{

/// Variable-sized trigger buffer for NLJ. Stores arrays of slice ends for both sides,
/// a ProbeTaskType, and window info. The slice end arrays are packed after the struct.
struct EmittedNLJWindowTrigger
{
    EmittedNLJWindowTrigger(
        const WindowInfo& windowInfo,
        const std::vector<SliceEnd>& leftSliceEnds,
        const std::vector<SliceEnd>& rightSliceEnds,
        ProbeTaskType probeTaskType,
        const uint64_t rangeIndex = 0,
        const uint64_t rangeCount = 1)
        : windowInfo(windowInfo)
        , leftNumberOfSliceEnds(leftSliceEnds.size())
        , rightNumberOfSliceEnds(rightSliceEnds.size())
        , probeTaskType(probeTaskType)
        , rangeIndex(rangeIndex)
        , rangeCount(rangeCount)
    {
        auto* base = std::bit_cast<int8_t*>(this + 1);
        this->leftSliceEnds = std::bit_cast<SliceEnd*>(base);
        this->rightSliceEnds = std::bit_cast<SliceEnd*>(base + (leftSliceEnds.size() * sizeof(SliceEnd)));
        std::ranges::copy(leftSliceEnds, this->leftSliceEnds);
        std::ranges::copy(rightSliceEnds, this->rightSliceEnds);
    }

    WindowInfo windowInfo;
    uint64_t leftNumberOfSliceEnds;
    uint64_t rightNumberOfSliceEnds;
    ProbeTaskType probeTaskType;
    /// Range-parallel probing (SMJ: hash-domain range i of rangeCount; IXJ: iterated-vector subset i of
    /// rangeCount). Plain NLJ probes ignore these and always receive (0, 1).
    uint64_t rangeIndex;
    uint64_t rangeCount;
    SliceEnd* leftSliceEnds;
    SliceEnd* rightSliceEnds;
};

class NLJOperatorHandler final : public StreamJoinOperatorHandler
{
public:
    NLJOperatorHandler(
        const std::vector<OriginId>& inputOrigins,
        OriginId outputOriginId,
        std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore,
        JoinTriggerStrategy triggerStrategy,
        uint64_t probeRangeTasks = 1);

    [[nodiscard]] std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>
    getCreateNewSlicesFunction(const CreateNewSlicesArguments&) const override;

private:
    void createProbeTasks(
        const ProbeWorkItem& workItem,
        const WindowInfo& windowInfo,
        PipelineExecutionContext* pipelineCtx,
        std::vector<TupleBuffer>& probeTasks) override;

    /// Number of range probe tasks per window (SMJ hash ranges / IXJ vector subsets); 0 = worker-thread count.
    uint64_t probeRangeTasks;
};
}
