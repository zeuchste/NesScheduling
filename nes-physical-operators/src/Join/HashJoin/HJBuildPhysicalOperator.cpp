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

#include <Join/HashJoin/HJBuildPhysicalOperator.hpp>

#include <cstdint>
#include <functional>
#include <memory>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVector.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Join/HashJoin/HJOperatorHandler.hpp>
#include <Join/HashJoin/HJSlice.hpp>
#include <Join/StreamJoinBuildPhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>
#include <Time/Timestamp.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <HashMapSlice.hpp>
#include <WindowBuildPhysicalOperator.hpp>
#include <function.hpp>
#include <options.hpp>
#include <static.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_enum.hpp>
#include <val_ptr.hpp>

namespace NES
{

void HJBuildPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    /// Getting the operator handler from the local state
    auto* localState = dynamic_cast<WindowOperatorBuildLocalState*>(ctx.getLocalState(id));
    auto operatorHandler = localState->getOperatorHandler();

    /// Get the current slice / hash map that we have to insert the tuple into
    const auto timestamp = timeFunction->getTs(ctx, record);
    const auto hashMapBuffer
        = sliceStoreRef->getDataStructureRef(timestamp, ctx.workerThreadId, operatorHandler, ctx.pipelineMemoryProvider.bufferProvider);

    ChainedHashMapRef hashMap{
        hashMapBuffer.asArg(),
        hashMapOptions.fieldKeys,
        hashMapOptions.fieldValues,
        hashMapOptions.entriesPerPage,
        hashMapOptions.entrySize};

    /// Calling the key functions to add/update the keys to the record
    nautilus::val<bool> containsNullInKey{false};
    for (nautilus::static_val<uint64_t> i = 0; i < hashMapOptions.fieldKeys.size(); ++i)
    {
        const auto& [fieldIdentifier, type, fieldOffset] = hashMapOptions.fieldKeys[i];
        const auto& function = hashMapOptions.keyFunctions[i];
        const auto value = function.execute(record, ctx.pipelineMemoryProvider.arena);
        containsNullInKey = containsNullInKey or (value.isNullable() and value.isNull());
        record.write(fieldIdentifier, value);
    }

    /// If any key field is null, we need to skip it from inserting the tuple in the hash table, as the tuple will never be included
    /// in the result set. This is the case as an inner join requires all join conditions to be TRUE (i.e., no NULL values in the join fields).
    if (not containsNullInKey)
    {
        /// T2 (symmetric hash join): {insert own + probe opposite} under the slice's single eager mutex.
        /// The strict total order makes the pairing exactly-once: an insert pairs with every previously
        /// completed opposite insert and can never see itself or a concurrent half-finished entry. The
        /// eager mutex subsumes the per-map shared-insert lock (both maps are only written under it).
        if (eager)
        {
            const auto sliceRef = nautilus::invoke(
                +[](OperatorHandler* handler, const Timestamp ts, const WorkerThreadId workerThreadId) -> HJSlice*
                { return dynamic_cast<HJOperatorHandler*>(handler)->sliceForEagerInsert(ts, workerThreadId); },
                ctx.getGlobalOperatorHandler(operatorHandlerId),
                timestamp,
                ctx.workerThreadId);
            nautilus::invoke(+[](HJSlice* slice) -> void { slice->eagerLock(); }, sliceRef);
            const auto ownEntry = hashMap.insertEntry(record, *hashMapOptions.hashFunction, ctx.pipelineMemoryProvider.bufferProvider);
            const auto oppositeBufferRef = nautilus::invoke(
                +[](HJSlice* slice, const JoinBuildSideType side) -> TupleBuffer*
                {
                    const auto opposite = side == JoinBuildSideType::Left ? JoinBuildSideType::Right : JoinBuildSideType::Left;
                    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast): the probe only reads; writes are serialized by the eager mutex
                    return const_cast<TupleBuffer*>(slice->getHashMapBufferRefForSide(WorkerThreadId(0), opposite));
                },
                sliceRef,
                nautilus::val<JoinBuildSideType>(joinBuildSide));
            /// Both sides share the key layout, and the probe below touches keys only, so viewing the
            /// opposite map with this side's field offsets is safe (mirrors forEachMatchingEntry's contract).
            const ChainedHashMapRef oppositeMap{
                oppositeBufferRef,
                hashMapOptions.fieldKeys,
                hashMapOptions.fieldValues,
                hashMapOptions.entriesPerPage,
                hashMapOptions.entrySize};
            oppositeMap.forEachMatchingEntry(
                static_cast<nautilus::val<ChainedHashMapEntry*>>(ownEntry),
                hashMapBuffer.asArg(),
                [&](const ChainedHashMapRef::ChainedEntryRef& oppositeEntryRef)
                {
                    nautilus::invoke(
                        +[](HJSlice* slice,
                            ChainedHashMapEntry* ownEntryPtr,
                            ChainedHashMapEntry* oppositeEntryPtr,
                            const JoinBuildSideType side) -> void
                        {
                            const auto own = reinterpret_cast<uint64_t>(ownEntryPtr);
                            const auto opp = reinterpret_cast<uint64_t>(oppositeEntryPtr);
                            if (side == JoinBuildSideType::Left)
                            {
                                slice->eagerRecordPair(own, opp);
                            }
                            else
                            {
                                slice->eagerRecordPair(opp, own);
                            }
                        },
                        sliceRef,
                        static_cast<nautilus::val<ChainedHashMapEntry*>>(ownEntry),
                        oppositeEntryRef.entryRef,
                        nautilus::val<JoinBuildSideType>(joinBuildSide));
                });
            nautilus::invoke(+[](HJSlice* slice) -> void { slice->eagerUnlock(); }, sliceRef);
            return;
        }

        /// P3 (SHARED_TABLE): serialize the whole insert against the other worker threads building into the same map.
        /// storageVariant/sharedHashMap are compile-time constants during tracing, so the non-selected paths and the
        /// lock invocations vanish from the compiled pipelines of the other variants.
        if (sharedHashMap)
        {
            nautilus::invoke(
                +[](const TupleBuffer* mapBuffer) -> void { ChainedHashMap::lockForSharedInsert(*mapBuffer); }, hashMapBuffer.asArg());
        }

        if (storageVariant == JoinStorageVariant::TUPLE_CHAINED)
        {
            /// S1: every tuple becomes its own entry with the value fields inline on the shared entry pages.
            hashMap.insertEntry(record, *hashMapOptions.hashFunction, ctx.pipelineMemoryProvider.bufferProvider);
            if (sharedHashMap)
            {
                nautilus::invoke(
                    +[](const TupleBuffer* mapBuffer) -> void { ChainedHashMap::unlockAfterSharedInsert(*mapBuffer); }, hashMapBuffer.asArg());
            }
            return;
        }

        /// S2/S3: one entry per distinct key whose value slot holds a per-key PagedVector.
        /// Finding or creating the entry for the provided record
        const auto hashMapEntry = hashMap.findOrCreateEntry(
            record,
            *hashMapOptions.hashFunction,
            [&](const nautilus::val<AbstractHashMapEntry*>& entry)
            {
                /// If the entry for the provided keys does not exist, we need to create a new one and initialize the underyling paged vector
                const ChainedHashMapRef::ChainedEntryRef entryRefReset{
                    entry, hashMapBuffer.asArg(), hashMapOptions.fieldKeys, hashMapOptions.fieldValues};
                const auto state = entryRefReset.getValueMemArea();
                const nautilus::val<uint64_t> tupleSize = tupleLayout->getSchema().getSizeInBytes();
                nautilus::invoke(
                    +[](TupleBuffer* hashMapBuf, uint32_t* valueMemArea, AbstractBufferProvider* bufferProvider, uint64_t tupleSize) -> void
                    {
                        if (auto pagedVectorBuffer = bufferProvider->getUnpooledBuffer(PagedVector::getMainBufferSize()))
                        {
                            PagedVector::init(pagedVectorBuffer.value(), bufferProvider->getBufferSize(), tupleSize);
                            auto childIndex = hashMapBuf->storeChildBuffer(pagedVectorBuffer.value());
                            *valueMemArea = childIndex.getRawIndex();
                            return;
                        }
                        throw BufferAllocationFailure("No unpooled TupleBuffer available for chained hash map entry's paged vector!");
                    },
                    hashMapBuffer.asArg(),
                    static_cast<nautilus::val<uint32_t*>>(state),
                    ctx.pipelineMemoryProvider.bufferProvider,
                    tupleSize);
            },
            ctx.pipelineMemoryProvider.bufferProvider);

        /// Inserting the tuple into the corresponding hash entry
        const ChainedHashMapRef::ChainedEntryRef entryRef{
            hashMapEntry, hashMapBuffer.asArg(), hashMapOptions.fieldKeys, hashMapOptions.fieldValues};
        auto entryMemArea = entryRef.getValueMemArea();
        OwnedNautilusBuffer pagedVecBuffer;
        nautilus::invoke(
            +[](TupleBuffer* hashMapBuf, TupleBuffer* out, const uint32_t* indexPtr)
            { *out = hashMapBuf->loadChildBuffer(VariableSizedAccess::Index{*indexPtr}); },
            hashMapBuffer.asArg(),
            pagedVecBuffer.asArg(),
            static_cast<nautilus::val<uint32_t*>>(entryMemArea));
        PagedVectorRef pagedVectorRef(BorrowedNautilusBuffer::from(pagedVecBuffer.asArg()), tupleLayout);
        pagedVectorRef.pushBack(record, ctx.pipelineMemoryProvider.bufferProvider);

        if (sharedHashMap)
        {
            nautilus::invoke(
                +[](const TupleBuffer* mapBuffer) -> void { ChainedHashMap::unlockAfterSharedInsert(*mapBuffer); }, hashMapBuffer.asArg());
        }
    }
}

HJBuildPhysicalOperator::HJBuildPhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    const JoinBuildSideType joinBuildSide,
    std::unique_ptr<TimeFunction> timeFunction,
    std::shared_ptr<PagedVectorTupleLayout> tupleLayout,
    HashMapOptions hashMapOptions,
    std::unique_ptr<SliceStoreRef> sliceStoreRef,
    const JoinStorageVariant storageVariant,
    const bool sharedHashMap,
    const bool eager)
    : StreamJoinBuildPhysicalOperator{operatorHandlerId, joinBuildSide, std::move(timeFunction), std::move(tupleLayout), std::move(sliceStoreRef)}
    , hashMapOptions(std::move(hashMapOptions))
    , storageVariant(storageVariant)
    , sharedHashMap(sharedHashMap)
    , eager(eager)
{
}

}
