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

#include <Join/StreamJoinOperatorHandler.hpp>

#include <map>
#include <memory>
#include <utility>
#include <variant>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <PipelineExecutionContext.hpp>
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{
StreamJoinOperatorHandler::StreamJoinOperatorHandler(
    const std::vector<OriginId>& inputOrigins,
    const OriginId outputOriginId,
    std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore,
    JoinTriggerStrategy triggerStrategy)
    : WindowBasedOperatorHandler(inputOrigins, outputOriginId, std::move(sliceAndWindowStore)), triggerStrategy(std::move(triggerStrategy))
{
}

void StreamJoinOperatorHandler::triggerSlices(
    const std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>& slicesAndWindowInfo,
    PipelineExecutionContext* pipelineCtx)
{
    for (const auto& [windowInfo, allSlices] : slicesAndWindowInfo)
    {
        const auto workItems
            = std::visit([&](const auto& strategy) { return strategy.collectProbeWorkItems(allSlices); }, triggerStrategy);

        /// Expand each work item into one or more probe task buffers (granularity depends on the join
        /// implementation and its configured processing variant).
        std::vector<TupleBuffer> probeTasks;
        for (const auto& workItem : workItems)
        {
            createProbeTasks(workItem, windowInfo.windowInfo, pipelineCtx, probeTasks);
        }

        /// Stamp sequence/chunk numbers over all tasks of this window and emit them.
        const auto totalChunks = probeTasks.size();
        ChunkNumber::Underlying chunkNumber = ChunkNumber::INITIAL;
        for (auto& probeTask : probeTasks)
        {
            probeTask.setSequenceNumber(SequenceNumber(windowInfo.sequenceNumber));
            probeTask.setChunkNumber(ChunkNumber(chunkNumber));
            probeTask.setLastChunk(chunkNumber == totalChunks);
            pipelineCtx->emitBuffer(probeTask);
            ++chunkNumber;
        }
    }
}

}
