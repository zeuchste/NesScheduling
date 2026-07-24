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
#include <array>

#include <functional>
#include <memory>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Join/StreamJoinOperatorHandler.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>

namespace NES
{

/// Operator handler of the index join (A4). Reuses the NLJ trigger-buffer format (EmittedNLJWindowTrigger:
/// window info + slice ends), but creates IXJSlices and does NOT combine the per-worker paged vectors at
/// trigger time — the shared index references (worker, position) pairs that must stay valid.
class IXJSlice;

class IXJOperatorHandler final : public StreamJoinOperatorHandler
{
public:
    IXJOperatorHandler(
        const std::vector<OriginId>& inputOrigins,
        OriginId outputOriginId,
        std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore,
        JoinTriggerStrategy triggerStrategy);

    [[nodiscard]] std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>
    getCreateNewSlicesFunction(const CreateNewSlicesArguments& args) const override;

    /// Resolves the IXJSlice covering `ts` for the index insert of `workerThreadId`, via a per-worker one-entry
    /// cache (single-writer per entry, so lock-free on the hot path; the store lookup only runs on slice change).
    /// The slice is guaranteed to exist: the vector extractor for the same tuple ran first and created it.
    /// ponytail: fixed 256-worker cache array; sized dynamically if we ever run more workers.
    [[nodiscard]] IXJSlice* sliceForIndexInsert(Timestamp ts, WorkerThreadId workerThreadId);

private:
    struct WorkerSliceCache
    {
        uint64_t start = 1; /// empty interval [1, 0) == always miss initially
        uint64_t end = 0;
        IXJSlice* slice = nullptr;
    };
    static constexpr uint64_t MAX_CACHED_WORKERS = 256;
    std::array<WorkerSliceCache, MAX_CACHED_WORKERS> indexInsertCaches{};

    void createProbeTasks(
        const ProbeWorkItem& workItem,
        const WindowInfo& windowInfo,
        PipelineExecutionContext* pipelineCtx,
        std::vector<TupleBuffer>& probeTasks) override;
};

}
