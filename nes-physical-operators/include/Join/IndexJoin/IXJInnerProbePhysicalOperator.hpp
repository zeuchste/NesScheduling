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

#include <memory>
#include <vector>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Hash/HashFunction.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Join/StreamJoinProbePhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Operators/Windows/WindowMetaData.hpp>
#include <ExecutionContext.hpp>

namespace NES
{

/// Probe phase of the index join (A4): iterates the right side's tuples and, per tuple, performs a logarithmic
/// lookup in the left side's shared index; the join predicate is evaluated per candidate pair, so hash
/// collisions in the index cannot produce wrong results. Inner joins only — outer joins fall back to the
/// hash join at plan time.
class IXJInnerProbePhysicalOperator final : public StreamJoinProbePhysicalOperator
{
public:
    IXJInnerProbePhysicalOperator(
        OperatorHandlerId operatorHandlerId,
        PhysicalFunction joinFunction,
        WindowMetaData windowMetaData,
        const JoinSchema& joinSchema,
        std::shared_ptr<PagedVectorTupleLayout> leftTupleLayout,
        std::shared_ptr<PagedVectorTupleLayout> rightTupleLayout,
        std::vector<Record::RecordFieldIdentifier> leftKeyFieldNames,
        std::vector<Record::RecordFieldIdentifier> rightKeyFieldNames,
        std::shared_ptr<HashFunction> hashFunction,
        bool eager = false);

    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

    static constexpr bool supportsJoinType(JoinLogicalOperator::JoinType joinType) noexcept
    {
        return joinType == JoinLogicalOperator::JoinType::INNER_JOIN;
    }

private:
    std::shared_ptr<PagedVectorTupleLayout> leftTupleLayout, rightTupleLayout;
    std::vector<Record::RecordFieldIdentifier> leftKeyFieldNames, rightKeyFieldNames;
    std::shared_ptr<HashFunction> hashFunction;
    /// Eager trigger (T2): drain the slice's pre-found pairs instead of probing the index.
    bool eager;
};

}
