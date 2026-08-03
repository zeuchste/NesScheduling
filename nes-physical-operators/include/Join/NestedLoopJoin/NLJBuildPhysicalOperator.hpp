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

#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include <Functions/PhysicalFunction.hpp>
#include <Interface/Hash/HashFunction.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Join/StreamJoinBuildPhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/SliceStoreRef.hpp>
#include <Watermark/TimeFunction.hpp>

namespace NES
{

/// This class is the first phase of the join. For both streams (left and right), the tuples are stored in the
/// corresponding slice one after the other. Afterward, the second phase (NLJProbe) will start joining the tuples
/// via two nested loops.
/// Eager trigger (T2) configuration for the build. NLJ_SCAN is the handshake/SplitJoin-style eager
/// NestedLoopJoin: scan the opposite side under the slice lock and store predicate-verified pairs.
/// RHJ_RUNS is the per-run trigger of the eager RunHashJoin: append (hash, position) to shared runs
/// on the slice that probe the opposite side's earlier runs at seal time.
struct NLJEagerBuild
{
    enum class Mode : uint8_t
    {
        NLJ_SCAN,
        RHJ_RUNS
    };
    Mode mode;
    /// NLJ_SCAN: the join predicate plus what is needed to read the opposite side's records.
    std::optional<PhysicalFunction> joinFunction;
    std::shared_ptr<PagedVectorTupleLayout> otherTupleLayout;
    std::vector<Record::RecordFieldIdentifier> ownKeyFieldNames;
    std::vector<Record::RecordFieldIdentifier> otherKeyFieldNames;
    /// RHJ_RUNS: the (casted) key fields to hash into the runs.
    std::vector<Record::RecordFieldIdentifier> keyFieldNames;
    std::shared_ptr<HashFunction> hashFunction;
};

class NLJBuildPhysicalOperator : public StreamJoinBuildPhysicalOperator
{
public:
    NLJBuildPhysicalOperator(
        OperatorHandlerId operatorHandlerId,
        JoinBuildSideType joinBuildSide,
        std::unique_ptr<TimeFunction> timeFunction,
        std::shared_ptr<PagedVectorTupleLayout> tupleLayout,
        std::unique_ptr<SliceStoreRef> sliceStoreRef,
        std::optional<NLJEagerBuild> eager = std::nullopt);

    void execute(ExecutionContext& executionCtx, Record& record) const override;

private:
    std::optional<NLJEagerBuild> eager;
};
}
