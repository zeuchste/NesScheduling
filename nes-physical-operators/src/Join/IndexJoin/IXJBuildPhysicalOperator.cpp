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

#include <Join/IndexJoin/IXJBuildPhysicalOperator.hpp>

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>
#include <DataTypes/VarVal.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Join/IndexJoin/IXJSlice.hpp>
#include <Join/StreamJoinBuildPhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ExecutionContext.hpp>
#include <WindowBuildPhysicalOperator.hpp>
#include <function.hpp>
#include <static.hpp>
#include <val_bool.hpp>
#include <val_enum.hpp>
#include <val_ptr.hpp>

namespace NES
{

IXJBuildPhysicalOperator::IXJBuildPhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    const JoinBuildSideType joinBuildSide,
    std::unique_ptr<TimeFunction> timeFunction,
    std::shared_ptr<PagedVectorTupleLayout> tupleLayout,
    std::unique_ptr<SliceStoreRef> sliceStoreRef,
    std::vector<PhysicalFunction> keyFunctions,
    std::vector<Record::RecordFieldIdentifier> keyFieldNames,
    std::shared_ptr<HashFunction> hashFunction)
    : StreamJoinBuildPhysicalOperator{operatorHandlerId, joinBuildSide, std::move(timeFunction), std::move(tupleLayout), std::move(sliceStoreRef)}
    , keyFunctions(std::move(keyFunctions))
    , keyFieldNames(std::move(keyFieldNames))
    , hashFunction(std::move(hashFunction))
{
}

void IXJBuildPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    auto* localState = dynamic_cast<WindowOperatorBuildLocalState*>(ctx.getLocalState(id));
    auto operatorHandler = localState->getOperatorHandler();

    /// For the index join, the slice store hands out the IXJSlice pointer itself: the build needs both the
    /// worker-local paged vector and the shared index of the slice.
    const auto timestamp = timeFunction->getTs(ctx, record);
    const auto slicePtr
        = sliceStoreRef->getDataStructureRef(timestamp, ctx.workerThreadId, operatorHandler, ctx.pipelineMemoryProvider.bufferProvider);

    /// Materialize the (casted) key fields into the record and collect them for hashing.
    nautilus::val<bool> containsNullInKey{false};
    std::vector<VarVal> keyValues;
    for (nautilus::static_val<uint64_t> i = 0; i < keyFieldNames.size(); ++i)
    {
        const auto value = keyFunctions[i].execute(record, ctx.pipelineMemoryProvider.arena);
        containsNullInKey = containsNullInKey or (value.isNullable() and value.isNull());
        record.write(keyFieldNames[i], value);
        keyValues.emplace_back(value);
    }

    /// Null keys never match in an inner join, so the tuple is skipped entirely (as in the hash-join build).
    if (not containsNullInKey)
    {
        const auto hash = hashFunction->calculate(keyValues);

        /// 1) Incremental index maintenance: register the upcoming tuple position in the shared, synchronized index.
        nautilus::invoke(
            +[](int8_t* slice, const WorkerThreadId workerThreadId, const JoinBuildSideType side, const uint64_t keyHash) -> void
            {
                /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): void* token from the slice store
                reinterpret_cast<IXJSlice*>(slice)->insertIndexEntry(side, workerThreadId, keyHash);
            },
            slicePtr,
            ctx.workerThreadId,
            nautilus::val<JoinBuildSideType>(joinBuildSide),
            hash);

        /// 2) Append the tuple to the worker-local paged vector.
        const auto vectorBufferRef = nautilus::invoke(
            +[](int8_t* slice, const WorkerThreadId workerThreadId, const JoinBuildSideType side) -> TupleBuffer*
            {
                /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast,cppcoreguidelines-pro-type-const-cast)
                return const_cast<TupleBuffer*>(reinterpret_cast<IXJSlice*>(slice)->getPagedVectorTupleBufferRef(workerThreadId, side));
            },
            slicePtr,
            ctx.workerThreadId,
            nautilus::val<JoinBuildSideType>(joinBuildSide));
        PagedVectorRef pagedVectorRef{BorrowedNautilusBuffer::from(vectorBufferRef), tupleLayout};
        pagedVectorRef.pushBack(record, ctx.pipelineMemoryProvider.bufferProvider);
    }
}

}
