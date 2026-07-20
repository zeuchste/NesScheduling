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

#include <Join/IndexJoin/IXJOperatorHandler.hpp>

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Join/IndexJoin/IXJSlice.hpp>
#include <Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Join/NestedLoopJoin/NLJSlice.hpp>
#include <Join/StreamJoinOperatorHandler.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Time/Timestamp.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

IXJOperatorHandler::IXJOperatorHandler(
    const std::vector<OriginId>& inputOrigins,
    const OriginId outputOriginId,
    std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore,
    JoinTriggerStrategy triggerStrategy)
    : StreamJoinOperatorHandler(inputOrigins, outputOriginId, std::move(sliceAndWindowStore), std::move(triggerStrategy))
{
}

std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>
IXJOperatorHandler::getCreateNewSlicesFunction(const CreateNewSlicesArguments& args) const
{
    PRECONDITION(
        numberOfWorkerThreads > 0, "Number of worker threads not set for window based operator. Was setWorkerThreads() being called?");
    const auto& nljArgs = dynamic_cast<const CreateNewNLJSliceArgs&>(args);
    return std::function(
        [numberOfWorkerThreads = numberOfWorkerThreads,
         bufferProvider = nljArgs.bufferProvider,
         tupleSizeLeft = nljArgs.tupleSizeLeft,
         tupleSizeRight = nljArgs.tupleSizeRight](SliceStart start, SliceEnd end) -> std::vector<std::shared_ptr<Slice>>
        { return {std::make_shared<IXJSlice>(*bufferProvider, start, end, numberOfWorkerThreads, tupleSizeLeft, tupleSizeRight)}; });
}

void IXJOperatorHandler::createProbeTasks(
    const ProbeWorkItem& workItem,
    const WindowInfo& windowInfo,
    PipelineExecutionContext* pipelineCtx,
    std::vector<TupleBuffer>& probeTasks)
{
    /// In contrast to the NLJ handler, the per-worker paged vectors are deliberately NOT combined here.
    uint64_t totalNumberOfTuples = 0;
    std::vector<SliceEnd> leftSliceEnds;
    leftSliceEnds.reserve(workItem.leftSlices.size());
    for (const auto& slice : workItem.leftSlices)
    {
        totalNumberOfTuples += dynamic_cast<NLJSlice&>(*slice).getNumberOfTuplesLeft();
        leftSliceEnds.emplace_back(slice->getSliceEnd());
    }
    std::vector<SliceEnd> rightSliceEnds;
    rightSliceEnds.reserve(workItem.rightSlices.size());
    for (const auto& slice : workItem.rightSlices)
    {
        totalNumberOfTuples += dynamic_cast<NLJSlice&>(*slice).getNumberOfTuplesRight();
        rightSliceEnds.emplace_back(slice->getSliceEnd());
    }

    const auto neededBufferSize = sizeof(EmittedNLJWindowTrigger) + ((leftSliceEnds.size() + rightSliceEnds.size()) * sizeof(SliceEnd));
    const auto tupleBufferVal = pipelineCtx->getBufferManager()->getUnpooledBuffer(neededBufferSize);
    if (not tupleBufferVal.has_value())
    {
        throw CannotAllocateBuffer("{}B for the IXJ window trigger were requested", neededBufferSize);
    }

    auto tupleBuffer = tupleBufferVal.value();
    tupleBuffer.setOriginId(outputOriginId);
    tupleBuffer.setWatermark(windowInfo.windowStart);
    tupleBuffer.setNumberOfTuples(totalNumberOfTuples);
    tupleBuffer.setCreationTimestampInMS(Timestamp(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count()));
    new (tupleBuffer.getAvailableMemoryArea().data())
        EmittedNLJWindowTrigger{windowInfo, leftSliceEnds, rightSliceEnds, workItem.probeTaskType};

    probeTasks.emplace_back(std::move(tupleBuffer));
}

}
