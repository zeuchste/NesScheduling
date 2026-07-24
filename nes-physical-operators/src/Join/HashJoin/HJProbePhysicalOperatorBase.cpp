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
#include <Join/HashJoin/HJProbePhysicalOperatorBase.hpp>

#include <cstdint>
#include <memory>
#include <utility>
#include <DataTypes/Schema.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/TimestampRef.hpp>
#include <Join/StreamJoinProbePhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Operators/Windows/WindowMetaData.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>
#include <Time/Timestamp.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <HashMapOptions.hpp>
#include <function.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>

namespace NES
{

HJProbePhysicalOperatorBase::HJProbePhysicalOperatorBase(
    const OperatorHandlerId operatorHandlerId,
    PhysicalFunction joinFunction,
    WindowMetaData windowMetaData,
    JoinSchema joinSchema,
    std::shared_ptr<PagedVectorTupleLayout> leftTupleLayout,
    std::shared_ptr<PagedVectorTupleLayout> rightTupleLayout,
    HashMapOptions leftHashMapOptions,
    HashMapOptions rightHashMapOptions,
    const JoinStorageVariant storageVariant, const JoinStorageVariant rightStorageVariant)
    : StreamJoinProbePhysicalOperator(operatorHandlerId, std::move(joinFunction), std::move(windowMetaData), std::move(joinSchema))
    , leftTupleLayout(std::move(leftTupleLayout))
    , rightTupleLayout(std::move(rightTupleLayout))
    , leftHashMapOptions(std::move(leftHashMapOptions))
    , rightHashMapOptions(std::move(rightHashMapOptions))
    , storageVariant(storageVariant)
    , rightStorageVariant(rightStorageVariant)
{
}

OwnedNautilusBuffer
HJProbePhysicalOperatorBase::pinHashMapBuffer(const nautilus::val<TupleBuffer*>& recordBufferRef, const nautilus::val<uint64_t>& index)
{
    OwnedNautilusBuffer hashMapBuffer;
    nautilus::invoke(
        +[](TupleBuffer* parent, uint32_t idx, TupleBuffer* out)
        {
            INVARIANT(parent != nullptr, "Parent TupleBuffer must not be null when pinning a hash map child buffer");
            *out = parent->loadChildBuffer(VariableSizedAccess::Index{idx});
        },
        recordBufferRef,
        index,
        hashMapBuffer.asArg());
    return hashMapBuffer;
}

ChainedHashMapRef
HJProbePhysicalOperatorBase::makeChainedHashMapRef(const nautilus::val<TupleBuffer*>& hashMapBufferRef, const HashMapOptions& options)
{
    return ChainedHashMapRef{hashMapBufferRef, options.fieldKeys, options.fieldValues, options.entriesPerPage, options.entrySize};
}

namespace
{
/// Loads the PagedVector child buffer referenced by a chained hash map entry's value area (which stores a child-buffer index).
PagedVectorRef
loadEntryPagedVector(const ChainedHashMapRef::ChainedEntryRef& entryRef, const std::shared_ptr<PagedVectorTupleLayout>& tupleLayout)
{
    auto valueMemArea = entryRef.getValueMemArea();
    OwnedNautilusBuffer pagedVectorBuffer;
    nautilus::invoke(
        +[](TupleBuffer* hashMapBuf, TupleBuffer* out, const uint32_t* indexPtr)
        { *out = hashMapBuf->loadChildBuffer(VariableSizedAccess::Index{*indexPtr}); },
        entryRef.hashMapBuffer,
        pagedVectorBuffer.asArg(),
        static_cast<nautilus::val<uint32_t*>>(valueMemArea));
    /// Must move the owned buffer into the PagedVectorRef rather than wrap a BorrowedNautilusBuffer around it: a
    /// borrowed view only stores a pointer back to `pagedVectorBuffer`, which would dangle once this function returns.
    return PagedVectorRef{std::move(pagedVectorBuffer), tupleLayout};
}
}

/// NOLINTNEXTLINE(readability-function-cognitive-complexity) inner join's N x N hash-map iteration is inherently deeply nested
void HJProbePhysicalOperatorBase::performMatchPairsProbe(
    const nautilus::val<TupleBuffer*>& recordBufferRef,
    nautilus::val<uint64_t> leftNumberOfHashMaps,
    nautilus::val<uint64_t> rightNumberOfHashMaps,
    ExecutionContext& executionCtx,
    const nautilus::val<Timestamp>& windowStart,
    const nautilus::val<Timestamp>& windowEnd,
    const nautilus::val<uint64_t>& rightPageStart,
    const nautilus::val<uint64_t>& rightPageEnd) const
{
    if (leftNumberOfHashMaps == 0 or rightNumberOfHashMaps == 0)
    {
        return;
    }

    if (storageVariant == JoinStorageVariant::TUPLE_CHAINED)
    {
        performSharedChainsMatchPairsProbe(
            recordBufferRef,
            leftNumberOfHashMaps,
            rightNumberOfHashMaps,
            executionCtx,
            windowStart,
            windowEnd,
            rightPageStart,
            rightPageEnd);
        return;
    }
    if (rightStorageVariant == JoinStorageVariant::TUPLE_CHAINED)
    {
        /// One-sided: left grouped, right inline per-tuple entries.
        performOneSidedMatchPairsProbe(
            recordBufferRef,
            leftNumberOfHashMaps,
            rightNumberOfHashMaps,
            executionCtx,
            windowStart,
            windowEnd,
            rightPageStart,
            rightPageEnd);
        return;
    }

    const auto leftFields = getOrderedFieldNames(leftTupleLayout->getSchema());
    const auto rightFields = getOrderedFieldNames(rightTupleLayout->getSchema());

    for (nautilus::val<uint64_t> leftHashMapIndex = 0; leftHashMapIndex < leftNumberOfHashMaps; ++leftHashMapIndex)
    {
        auto leftHashMapBuffer = pinHashMapBuffer(recordBufferRef, leftHashMapIndex);
        ChainedHashMapRef leftHashMap = makeChainedHashMapRef(leftHashMapBuffer.asArg(), leftHashMapOptions);
        for (nautilus::val<uint64_t> rightHashMapIndex = 0; rightHashMapIndex < rightNumberOfHashMaps; ++rightHashMapIndex)
        {
            /// Right hash map buffers are stored as child buffers right after all of the left ones
            auto rightHashMapBuffer = pinHashMapBuffer(recordBufferRef, leftNumberOfHashMaps + rightHashMapIndex);
            const ChainedHashMapRef rightHashMap = makeChainedHashMapRef(rightHashMapBuffer.asArg(), rightHashMapOptions);
            const auto rightRangeEnd = rightHashMap.endRange(rightPageStart, rightPageEnd);
            for (auto rightIt = rightHashMap.beginRange(rightPageStart, rightPageEnd); rightIt != rightRangeEnd; ++rightIt)
            {
                const auto rightEntry = *rightIt;
                const ChainedHashMapRef::ChainedEntryRef rightEntryRef{
                    rightEntry, rightHashMapBuffer.asArg(), rightHashMapOptions.fieldKeys, rightHashMapOptions.fieldValues};
                const PagedVectorRef rightPagedVector = loadEntryPagedVector(rightEntryRef, rightTupleLayout);
                auto rightItStart = rightPagedVector.begin();
                auto rightItEnd = rightPagedVector.end();

                if (const auto leftEntry = leftHashMap.findEntry(rightEntryRef.entryRef); leftEntry != nullptr)
                {
                    const ChainedHashMapRef::ChainedEntryRef leftEntryRef{
                        static_cast<nautilus::val<ChainedHashMapEntry*>>(leftEntry),
                        leftHashMapBuffer.asArg(),
                        leftHashMapOptions.fieldKeys,
                        leftHashMapOptions.fieldValues};
                    const PagedVectorRef leftPagedVector = loadEntryPagedVector(leftEntryRef, leftTupleLayout);

                    for (auto leftIt = leftPagedVector.begin(); leftIt != leftPagedVector.end(); ++leftIt)
                    {
                        for (auto rightIt = rightItStart; rightIt != rightItEnd; ++rightIt)
                        {
                            const auto leftRecord = *leftIt;
                            const auto rightRecord = *rightIt;
                            auto joinedRecord
                                = createJoinedRecord(leftRecord, rightRecord, windowStart, windowEnd, leftFields, rightFields);
                            executeChild(executionCtx, joinedRecord);
                        }
                    }
                }
            }
        }
    }
}

namespace
{
/// S1 stores the key fields only in the entry's key region; the value region holds the remaining fields.
/// Reconstructs the full record by merging both regions.
Record reconstructRecordFromEntry(const ChainedHashMapRef::ChainedEntryRef& entryRef, const HashMapOptions& options)
{
    auto record = entryRef.getValue();
    for (const auto& field : options.fieldKeys)
    {
        record.write(field.fieldIdentifier, entryRef.getKey(field.fieldIdentifier));
    }
    return record;
}
}

void HJProbePhysicalOperatorBase::performSharedChainsMatchPairsProbe(
    const nautilus::val<TupleBuffer*>& recordBufferRef,
    nautilus::val<uint64_t> leftNumberOfHashMaps,
    nautilus::val<uint64_t> rightNumberOfHashMaps,
    ExecutionContext& executionCtx,
    const nautilus::val<Timestamp>& windowStart,
    const nautilus::val<Timestamp>& windowEnd,
    const nautilus::val<uint64_t>& rightPageStart,
    const nautilus::val<uint64_t>& rightPageEnd) const
{
    const auto leftFields = getOrderedFieldNames(leftTupleLayout->getSchema());
    const auto rightFields = getOrderedFieldNames(rightTupleLayout->getSchema());

    for (nautilus::val<uint64_t> leftHashMapIndex = 0; leftHashMapIndex < leftNumberOfHashMaps; ++leftHashMapIndex)
    {
        auto leftHashMapBuffer = pinHashMapBuffer(recordBufferRef, leftHashMapIndex);
        ChainedHashMapRef leftHashMap = makeChainedHashMapRef(leftHashMapBuffer.asArg(), leftHashMapOptions);
        for (nautilus::val<uint64_t> rightHashMapIndex = 0; rightHashMapIndex < rightNumberOfHashMaps; ++rightHashMapIndex)
        {
            auto rightHashMapBuffer = pinHashMapBuffer(recordBufferRef, leftNumberOfHashMaps + rightHashMapIndex);
            const ChainedHashMapRef rightHashMap = makeChainedHashMapRef(rightHashMapBuffer.asArg(), rightHashMapOptions);
            const auto rightRangeEnd = rightHashMap.endRange(rightPageStart, rightPageEnd);
            for (auto rightIt = rightHashMap.beginRange(rightPageStart, rightPageEnd); rightIt != rightRangeEnd; ++rightIt)
            {
                const auto rightEntry = *rightIt;
                const ChainedHashMapRef::ChainedEntryRef rightEntryRef{
                    rightEntry, rightHashMapBuffer.asArg(), rightHashMapOptions.fieldKeys, rightHashMapOptions.fieldValues};
                const auto rightRecord = reconstructRecordFromEntry(rightEntryRef, rightHashMapOptions);

                leftHashMap.forEachMatchingEntry(
                    rightEntry,
                    rightHashMapBuffer.asArg(),
                    [&](const ChainedHashMapRef::ChainedEntryRef& leftEntryRef)
                    {
                        const auto leftRecord = reconstructRecordFromEntry(leftEntryRef, leftHashMapOptions);
                        auto joinedRecord = createJoinedRecord(leftRecord, rightRecord, windowStart, windowEnd, leftFields, rightFields);
                        executeChild(executionCtx, joinedRecord);
                    });
            }
        }
    }
}


/// One-sided probe: the right map holds inline per-tuple entries (scanned sequentially, its directory
/// unused); each right tuple probes the left grouped map and joins against that key's paged vector.
void HJProbePhysicalOperatorBase::performOneSidedMatchPairsProbe(
    const nautilus::val<TupleBuffer*>& recordBufferRef,
    nautilus::val<uint64_t> leftNumberOfHashMaps,
    nautilus::val<uint64_t> rightNumberOfHashMaps,
    ExecutionContext& executionCtx,
    const nautilus::val<Timestamp>& windowStart,
    const nautilus::val<Timestamp>& windowEnd,
    const nautilus::val<uint64_t>& rightPageStart,
    const nautilus::val<uint64_t>& rightPageEnd) const
{
    const auto leftFields = getOrderedFieldNames(leftTupleLayout->getSchema());
    const auto rightFields = getOrderedFieldNames(rightTupleLayout->getSchema());

    for (nautilus::val<uint64_t> leftHashMapIndex = 0; leftHashMapIndex < leftNumberOfHashMaps; ++leftHashMapIndex)
    {
        auto leftHashMapBuffer = pinHashMapBuffer(recordBufferRef, leftHashMapIndex);
        ChainedHashMapRef leftHashMap = makeChainedHashMapRef(leftHashMapBuffer.asArg(), leftHashMapOptions);
        for (nautilus::val<uint64_t> rightHashMapIndex = 0; rightHashMapIndex < rightNumberOfHashMaps; ++rightHashMapIndex)
        {
            auto rightHashMapBuffer = pinHashMapBuffer(recordBufferRef, leftNumberOfHashMaps + rightHashMapIndex);
            const ChainedHashMapRef rightHashMap = makeChainedHashMapRef(rightHashMapBuffer.asArg(), rightHashMapOptions);
            const auto rightRangeEnd = rightHashMap.endRange(rightPageStart, rightPageEnd);
            for (auto rightIt = rightHashMap.beginRange(rightPageStart, rightPageEnd); rightIt != rightRangeEnd; ++rightIt)
            {
                const auto rightEntry = *rightIt;
                const ChainedHashMapRef::ChainedEntryRef rightEntryRef{
                    rightEntry, rightHashMapBuffer.asArg(), rightHashMapOptions.fieldKeys, rightHashMapOptions.fieldValues};
                const auto rightRecord = reconstructRecordFromEntry(rightEntryRef, rightHashMapOptions);

                if (const auto leftEntry = leftHashMap.findEntry(rightEntryRef.entryRef); leftEntry != nullptr)
                {
                    const ChainedHashMapRef::ChainedEntryRef leftEntryRef{
                        static_cast<nautilus::val<ChainedHashMapEntry*>>(leftEntry),
                        leftHashMapBuffer.asArg(),
                        leftHashMapOptions.fieldKeys,
                        leftHashMapOptions.fieldValues};
                    const PagedVectorRef leftPagedVector = loadEntryPagedVector(leftEntryRef, leftTupleLayout);
                    for (auto leftIt = leftPagedVector.begin(); leftIt != leftPagedVector.end(); ++leftIt)
                    {
                        const auto leftRecord = *leftIt;
                        auto joinedRecord = createJoinedRecord(leftRecord, rightRecord, windowStart, windowEnd, leftFields, rightFields);
                        executeChild(executionCtx, joinedRecord);
                    }
                }
            }
        }
    }
}

}
