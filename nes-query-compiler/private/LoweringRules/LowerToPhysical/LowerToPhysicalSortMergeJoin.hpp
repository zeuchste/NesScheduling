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

#include <utility>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Operators/LogicalOperator.hpp>
#include <QueryExecutionConfiguration.hpp>

namespace NES
{
/// Lowers a join with the SORT_MERGE_JOIN implementation trait (A2): NLJ-style append-only build,
/// trigger-time sort of both sides by join-key hash, merge-pass probe.
struct LowerToPhysicalSortMergeJoin : AbstractLoweringRule
{
    explicit LowerToPhysicalSortMergeJoin(QueryExecutionConfiguration conf) : conf(std::move(conf)) { }

    LoweringRuleResultSubgraph apply(LogicalOperator logicalOperator) override;

private:
    QueryExecutionConfiguration conf;
};

}
