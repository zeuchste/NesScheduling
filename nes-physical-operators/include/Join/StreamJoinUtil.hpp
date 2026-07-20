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

#include <concepts>
#include <functional>
#include <memory>
#include <utility>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/NESStrongTypeFormat.hpp> /// NOLINT
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Sequencing/SequenceData.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>

namespace NES
{

enum class JoinBuildSideType : uint8_t
{
    Right,
    Left
};

/// Identifies what kind of work a probe task should perform.
/// NOLINTNEXTLINE(performance-enum-size) uint64_t required for struct alignment and Nautilus readValueFromMemRef compatibility
enum class ProbeTaskType : uint64_t
{
    MATCH_PAIRS = 0, /// Inner-join-style: emit only matched pairs
    LEFT_NULL_FILL = 1, /// Iterate left (preserved) maps, check all right maps, emit null-fill for unmatched left
    RIGHT_NULL_FILL = 2 /// Iterate right (preserved) maps, check all left maps, emit null-fill for unmatched right
};

class PipelineExecutionContext;

/// One unit of probe work for a triggered window, produced by a JoinTriggerStrategy.
/// The join implementation's operator handler expands each work item into one or more probe
/// task buffers (depending on the configured JoinProcessingVariant) and the sequence/chunk
/// numbers are assigned centrally over all tasks of a window.
struct ProbeWorkItem
{
    std::vector<std::shared_ptr<Slice>> leftSlices;
    std::vector<std::shared_ptr<Slice>> rightSlices;
    ProbeTaskType probeTaskType;
};

/// Concept: a hash join probe operator must declare which join types it supports via a static constexpr method.
/// This enables compile-time verification in the lowering rules that the chosen probe operator is compatible with
/// the requested join type.
template <typename T>
concept JoinProbeOperator = requires(JoinLogicalOperator::JoinType jt) {
    { T::supportsJoinType(jt) } -> std::same_as<bool>;
};

/// This stores the left, right and output schema for a binary join
struct JoinSchema
{
    JoinSchema(
        Schema<QualifiedUnboundField, Ordered> leftSchema,
        Schema<QualifiedUnboundField, Ordered> rightSchema,
        Schema<QualifiedUnboundField, Ordered> joinSchema)
        : leftSchema(std::move(leftSchema)), rightSchema(std::move(rightSchema)), joinSchema(std::move(joinSchema))
    {
    }

    Schema<QualifiedUnboundField, Ordered> leftSchema;
    Schema<QualifiedUnboundField, Ordered> rightSchema;
    Schema<QualifiedUnboundField, Ordered> joinSchema;
};
}
