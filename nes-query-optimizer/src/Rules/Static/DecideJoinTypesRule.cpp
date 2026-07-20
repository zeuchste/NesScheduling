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
#include <Rules/Static/DecideJoinTypesRule.hpp>

#include <algorithm>
#include <ranges>
#include <set>
#include <string_view>
#include <typeindex>
#include <typeinfo>
#include <unordered_set>
#include <vector>

#include <Functions/BooleanFunctions/AndLogicalFunction.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/BooleanFunctions/OrLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Barriers/FixedPlanStructureBarrier.hpp>
#include <Traits/JoinImplementationTypeTrait.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <QueryOptimizerConfiguration.hpp>

namespace NES
{

namespace
{
/// We set the join type to be a Hash-Join if the join function consists of solely functions that are FieldAccessLogicalFunction.
/// To be more specific, we currently support only
/// Otherwise, we use a NLJ.
bool shallUseHashJoin(const LogicalFunction& joinFunction)
{
    /// Checks if the logical function is allowed to be in our join function for a hash join
    auto allowedLogicalFunction = [](const LogicalFunction& logicalFunction)
    {
        return logicalFunction.tryGetAs<AndLogicalFunction>().has_value() or logicalFunction.tryGetAs<EqualsLogicalFunction>().has_value()
            or logicalFunction.tryGetAs<OrLogicalFunction>().has_value()
            or logicalFunction.tryGetAs<FieldAccessLogicalFunction>().has_value();
    };

    std::unordered_set<LogicalFunction> parentsOfJoinComparisons;
    for (auto logicalFunction : BFSRange<LogicalFunction>(joinFunction))
    {
        if (not allowedLogicalFunction(logicalFunction))
        {
            return false;
        }

        const auto anyChildIsLeaf
            = std::ranges::any_of(logicalFunction.getChildren(), [](const LogicalFunction& child) { return child.getChildren().empty(); });
        if (anyChildIsLeaf)
        {
            for (const auto& child : logicalFunction.getChildren())
            {
                if (not child.tryGetAs<FieldAccessLogicalFunction>().has_value())
                {
                    /// If the leaf is not a FieldAccessLogicalFunction, we need to use a NLJ
                    return false;
                }
            }
        }
    }

    return true;
}
}

const std::type_info& DecideJoinTypesRule::getType()
{
    return typeid(DecideJoinTypesRule);
}

std::string_view DecideJoinTypesRule::getName()
{
    return NAME;
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> DecideJoinTypesRule::dependsOn() const
{
    return {typeid(FixedPlanStructureBarrier)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> DecideJoinTypesRule::requiredBy() const
{
    return {};
}

LogicalPlan DecideJoinTypesRule::apply(const LogicalPlan& queryPlan) const
{
    PRECONDITION(queryPlan.getRootOperators().size() == 1, "Only single root operators are supported for now");
    PRECONDITION(not queryPlan.getRootOperators().empty(), "Query must have a sink root operator");
    return LogicalPlan{queryPlan.getQueryId(), {apply(queryPlan.getRootOperators()[0])}};
}

bool DecideJoinTypesRule::operator==(const DecideJoinTypesRule& other) const
{
    return this->joinStrategy == other.joinStrategy;
}

LogicalOperator DecideJoinTypesRule::apply(const LogicalOperator& logicalOperator) const
{
    const auto children = logicalOperator.getChildren()
        | std::views::transform([this](const LogicalOperator& child) { return apply(child); }) | std::ranges::to<std::vector>();
    auto traitSet = logicalOperator.getTraitSet();
    if (const auto joinOperator = logicalOperator.tryGetAs<JoinLogicalOperator>())
    {
        if (this->joinStrategy == StreamJoinStrategy::NESTED_LOOP_JOIN)
        {
            tryInsert(traitSet, JoinImplementationTypeTrait{JoinImplementation::NESTED_LOOP_JOIN});
        }
        else if (shallUseHashJoin(joinOperator.value()->getJoinFunction()))
        {
            /// Equi-join: pick the configured key-based implementation. The sort-merge (A2) and index (A4)
            /// variants support inner joins only; outer joins fall back to the hash join with a warning.
            auto implementation = JoinImplementation::HASH_JOIN;
            if (this->joinStrategy == StreamJoinStrategy::SORT_MERGE_JOIN or this->joinStrategy == StreamJoinStrategy::INDEX_JOIN)
            {
                if (isOuterJoin(joinOperator.value()->getJoinType()))
                {
                    NES_WARNING(
                        "The configured sort-merge/index join strategy supports inner joins only; falling back to the hash join "
                        "for operator {}",
                        logicalOperator);
                }
                else
                {
                    implementation = this->joinStrategy == StreamJoinStrategy::SORT_MERGE_JOIN ? JoinImplementation::SORT_MERGE_JOIN
                                                                                               : JoinImplementation::INDEX_JOIN;
                }
            }
            tryInsert(traitSet, JoinImplementationTypeTrait{implementation});
        }
        else
        {
            tryInsert(traitSet, JoinImplementationTypeTrait{JoinImplementation::NESTED_LOOP_JOIN});
            if (this->joinStrategy == StreamJoinStrategy::HASH_JOIN)
            {
                NES_WARNING(
                    "Operator {} has not the HashJoinTrait, as the hash join is not supported for the join condition. Therefore, we "
                    "fall-back to the NLJ!",
                    logicalOperator);
            }
        }
    }
    else
    {
        tryInsert(traitSet, JoinImplementationTypeTrait{JoinImplementation::CHOICELESS});
    }
    return logicalOperator.withChildren(children).withTraitSet(traitSet);
}
}
