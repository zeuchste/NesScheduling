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
#include <Interface/Record.hpp>
#include <Join/StreamJoinBuildPhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <SliceStore/SliceStoreRef.hpp>
#include <Watermark/TimeFunction.hpp>
#include <ExecutionContext.hpp>

namespace NES
{

/// Build phase of the index join (A4): appends each tuple to the worker-local paged vector (like the NLJ build)
/// and additionally registers it in the slice's SHARED ordered index under its key hash — the incremental,
/// synchronized index maintenance that defines this variant.
class IXJBuildPhysicalOperator final : public StreamJoinBuildPhysicalOperator
{
public:
    IXJBuildPhysicalOperator(
        OperatorHandlerId operatorHandlerId,
        JoinBuildSideType joinBuildSide,
        std::unique_ptr<TimeFunction> timeFunction,
        std::shared_ptr<PagedVectorTupleLayout> tupleLayout,
        std::unique_ptr<SliceStoreRef> sliceStoreRef,
        std::vector<PhysicalFunction> keyFunctions,
        std::vector<Record::RecordFieldIdentifier> keyFieldNames,
        std::shared_ptr<HashFunction> hashFunction);
    void execute(ExecutionContext& ctx, Record& record) const override;

private:
    std::vector<PhysicalFunction> keyFunctions;
    std::vector<Record::RecordFieldIdentifier> keyFieldNames;
    std::shared_ptr<HashFunction> hashFunction;
};

}
