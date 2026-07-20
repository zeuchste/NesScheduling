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

#include <map>
#include <memory>
#include <variant>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Join/JoinTriggerStrategy.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{

/// All possible trigger strategies, selected at lowering time based on the join type.
/// std::variant avoids virtual dispatch and heap allocation — the strategy is stored inline.
using JoinTriggerStrategy = std::variant<
    InnerJoinTriggerStrategy,
    OuterJoinTriggerStrategy<true, false>,
    OuterJoinTriggerStrategy<false, true>,
    OuterJoinTriggerStrategy<true, true>>;

/// This operator is the general join operator handler. It is expected that all StreamJoinOperatorHandlers inherit from this class.
/// It delegates window triggering to a JoinTriggerStrategy configured at lowering time. The strategy yields probe work
/// items; the specific join implementation expands each work item into one or more probe task buffers via
/// createProbeTasks() (e.g., the hash join splits one work item into one task per hash-map pair for the TASK_PER_PAIR
/// processing variant). Sequence and chunk numbers are assigned centrally here, over all tasks of a window.
class StreamJoinOperatorHandler : public WindowBasedOperatorHandler
{
public:
    StreamJoinOperatorHandler(
        const std::vector<OriginId>& inputOrigins,
        OriginId outputOriginId,
        std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore,
        JoinTriggerStrategy triggerStrategy);

protected:
    /// Delegates to the configured JoinTriggerStrategy for each triggered window, expands the resulting work items
    /// into probe task buffers, stamps sequence/chunk numbers, and emits the buffers.
    void triggerSlices(
        const std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>& slicesAndWindowInfo,
        PipelineExecutionContext* pipelineCtx) override;

    /// Creates the probe task buffer(s) for one work item and appends them to probeTasks.
    /// Each join implementation (HJ, NLJ, ...) packs its own data format into the probe buffers.
    /// Everything except the sequence/chunk data must be set on the returned buffers.
    virtual void createProbeTasks(
        const ProbeWorkItem& workItem,
        const WindowInfo& windowInfo,
        PipelineExecutionContext* pipelineCtx,
        std::vector<TupleBuffer>& probeTasks)
        = 0;

    JoinTriggerStrategy triggerStrategy;
};
}
