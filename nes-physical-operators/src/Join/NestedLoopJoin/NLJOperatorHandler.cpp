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

#include <Join/NestedLoopJoin/NLJOperatorHandler.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Interface/PagedVector/PagedVector.hpp>
#include <Join/NestedLoopJoin/NLJSlice.hpp>
#include <Join/StreamJoinOperatorHandler.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Sequencing/SequenceData.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{
NLJOperatorHandler::NLJOperatorHandler(
    const std::vector<OriginId>& inputOrigins,
    OriginId outputOriginId,
    std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore,
    JoinTriggerStrategy triggerStrategy,
    const uint64_t probeRangeTasks)
    : StreamJoinOperatorHandler(inputOrigins, outputOriginId, std::move(sliceAndWindowStore), std::move(triggerStrategy))
    , probeRangeTasks(probeRangeTasks)
{
}

NLJSlice* NLJOperatorHandler::sliceForEagerInsert(const Timestamp ts, const WorkerThreadId workerThreadId)
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
            INVARIANT(false, "The eager insert requires the slice to exist (the vector extractor creates it first)");
            return {};
        });
    INVARIANT(not slices.empty(), "No slice found for timestamp {}", ts);
    auto* nljSlice = dynamic_cast<NLJSlice*>(slices.front().get());
    INVARIANT(nljSlice != nullptr, "Slice for timestamp {} is not an NLJSlice", ts);
    cache = {nljSlice->getSliceStart().getRawValue(), nljSlice->getSliceEnd().getRawValue(), nljSlice};
    return nljSlice;
}

std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>
NLJOperatorHandler::getCreateNewSlicesFunction(const CreateNewSlicesArguments& args) const
{
    PRECONDITION(
        numberOfWorkerThreads > 0, "Number of worker threads not set for window based operator. Was setWorkerThreads() being called?");
    const auto& nljArgs = dynamic_cast<const CreateNewNLJSliceArgs&>(args);
    return std::function(
        [numberOfWorkerThreads = numberOfWorkerThreads,
         bufferProvider = nljArgs.bufferProvider,
         tupleSizeLeft = nljArgs.tupleSizeLeft,
         tupleSizeRight = nljArgs.tupleSizeRight](SliceStart start, SliceEnd end) -> std::vector<std::shared_ptr<Slice>>
        { return {std::make_shared<NLJSlice>(*bufferProvider, start, end, numberOfWorkerThreads, tupleSizeLeft, tupleSizeRight)}; });
}

void NLJOperatorHandler::createProbeTasks(
    const ProbeWorkItem& workItem,
    const WindowInfo& windowInfo,
    PipelineExecutionContext* pipelineCtx,
    std::vector<TupleBuffer>& probeTasks)
{
    /// Combine paged vectors for all slices on both sides
    uint64_t totalNumberOfTuples = 0;
    for (const auto& slice : workItem.leftSlices)
    {
        auto& nljSlice = dynamic_cast<NLJSlice&>(*slice);
        nljSlice.combinePagedVectors();
        totalNumberOfTuples += nljSlice.getNumberOfTuplesLeft();
    }
    for (const auto& slice : workItem.rightSlices)
    {
        auto& nljSlice = dynamic_cast<NLJSlice&>(*slice);
        nljSlice.combinePagedVectors();
        totalNumberOfTuples += nljSlice.getNumberOfTuplesRight();
    }

    /// Collect slice ends
    std::vector<SliceEnd> leftSliceEnds;
    leftSliceEnds.reserve(workItem.leftSlices.size());
    for (const auto& slice : workItem.leftSlices)
    {
        leftSliceEnds.emplace_back(slice->getSliceEnd());
    }
    std::vector<SliceEnd> rightSliceEnds;
    rightSliceEnds.reserve(workItem.rightSlices.size());
    for (const auto& slice : workItem.rightSlices)
    {
        rightSliceEnds.emplace_back(slice->getSliceEnd());
    }

    /// Range-parallel probing: k tasks per window (match-pairs only; null-fill tasks must see everything).
    const auto rangeTasks = workItem.probeTaskType == ProbeTaskType::MATCH_PAIRS
        ? std::max<uint64_t>(1, probeRangeTasks != 0 ? probeRangeTasks : numberOfWorkerThreads)
        : 1;
    const auto neededBufferSize = sizeof(EmittedNLJWindowTrigger) + ((leftSliceEnds.size() + rightSliceEnds.size()) * sizeof(SliceEnd));
    for (uint64_t rangeIndex = 0; rangeIndex < rangeTasks; ++rangeIndex)
    {
        const auto tupleBufferVal = pipelineCtx->getBufferManager()->getUnpooledBuffer(neededBufferSize);
        if (not tupleBufferVal.has_value())
        {
            throw CannotAllocateBuffer("{}B for the NLJ window trigger were requested", neededBufferSize);
        }

        auto tupleBuffer = tupleBufferVal.value();
        tupleBuffer.setOriginId(outputOriginId);
        tupleBuffer.setWatermark(windowInfo.windowStart);
        tupleBuffer.setNumberOfTuples(totalNumberOfTuples);
        tupleBuffer.setCreationTimestampInMS(Timestamp(
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count()));
        new (tupleBuffer.getAvailableMemoryArea().data())
            EmittedNLJWindowTrigger{windowInfo, leftSliceEnds, rightSliceEnds, workItem.probeTaskType, rangeIndex, rangeTasks};

        probeTasks.emplace_back(std::move(tupleBuffer));
    }
}

}
