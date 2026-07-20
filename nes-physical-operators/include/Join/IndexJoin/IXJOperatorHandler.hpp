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

private:
    void createProbeTasks(
        const ProbeWorkItem& workItem,
        const WindowInfo& windowInfo,
        PipelineExecutionContext* pipelineCtx,
        std::vector<TupleBuffer>& probeTasks) override;
};

}
