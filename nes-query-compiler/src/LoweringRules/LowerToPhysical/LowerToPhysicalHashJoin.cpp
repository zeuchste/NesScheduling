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
#include <LoweringRules/LowerToPhysical/LowerToPhysicalHashJoin.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <DataTypes/TimeUnit.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/CastToTypeLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/FieldAccessPhysicalFunction.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Interface/Hash/MurMur3HashFunction.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Interface/PagedVector/PagedVector.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Join/HashJoin/HJBuildPhysicalOperator.hpp>
#include <Join/HashJoin/HJInnerProbePhysicalOperator.hpp>
#include <Join/HashJoin/HJOperatorHandler.hpp>
#include <Join/HashJoin/HJOuterProbePhysicalOperator.hpp>
#include <Join/HashJoin/HJSlice.hpp>
#include <Join/JoinTriggerStrategy.hpp>
#include <Join/StreamJoinOperatorHandler.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <LoweringRules/LowerToPhysical/StreamJoinLoweringUtil.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Schema/Field.hpp>
#include <SliceStore/DefaultTimeBasedSliceStore.hpp>
#include <SliceStore/Slice.hpp>
#include <Traits/FieldMappingTrait.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/SchemaFactory.hpp>
#include <Util/StreamJoinKnobs.hpp>
#include <Watermark/TimeFunction.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <ErrorHandling.hpp>
#include <HashMapOptions.hpp>
#include <HashMapSlice.hpp>
#include <LoweringRuleRegistry.hpp>
#include <MapPhysicalOperator.hpp>
#include <PhysicalOperator.hpp>
#include <QueryExecutionConfiguration.hpp>
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{

using StreamJoinLoweringUtil::FieldNamesExtension;
using StreamJoinLoweringUtil::addMapOperators;
using StreamJoinLoweringUtil::getJoinFieldExtensionsLeftRight;

namespace
{
HashMapOptions createHashMapOptions(
    std::vector<FieldNamesExtension>& joinFieldExtensions,
    Schema<QualifiedUnboundField, Ordered>& inputSchema,
    const QueryExecutionConfiguration& conf,
    const JoinStorageVariant storageVariant)
{
    uint64_t keySize = 0;
    std::vector<PhysicalFunction> keyFunctions;
    std::vector<QualifiedIdentifier> fieldKeyNames;
    for (auto& fieldExtension : joinFieldExtensions)
    {
        keySize += fieldExtension.newField.getDataType().getSizeInBytesWithNull();
        keyFunctions.emplace_back(FieldAccessPhysicalFunction{fieldExtension.newField.getFullyQualifiedName()});
        fieldKeyNames.emplace_back(fieldExtension.newField.getFullyQualifiedName());
    }

    /// S2/S3 store a per-key PagedVector as the entry value; S1 (TUPLE_CHAINED) stores the non-key record
    /// fields inline in the entry, so every tuple becomes its own entry on the shared entry pages.
    uint64_t valueSize = 0;
    std::vector<QualifiedIdentifier> fieldValueNames;
    if (storageVariant == JoinStorageVariant::TUPLE_CHAINED)
    {
        for (const auto& field : inputSchema)
        {
            if (std::ranges::find(fieldKeyNames, field.getFullyQualifiedName()) == fieldKeyNames.end())
            {
                valueSize += field.getDataType().getSizeInBytesWithNull();
                fieldValueNames.emplace_back(field.getFullyQualifiedName());
            }
        }
    }
    else
    {
        /// On the redesigned map the entry value is the 4-byte child-buffer index of the entry's PagedVector,
        /// not an inline PagedVector object.
        valueSize = sizeof(uint32_t);
    }

    const auto pageSize = conf.pageSize.getValue();
    const auto numberOfBuckets
        = storageVariant == JoinStorageVariant::FIXED_ARRAY ? conf.joinFixedBuckets.getValue() : conf.numberOfPartitions.getValue();
    const auto entrySize = sizeof(ChainedHashMapEntry) + keySize + valueSize;
    const auto entriesPerPage = pageSize / entrySize;

    const auto& [fieldKeys, fieldValues] = ChainedEntryMemoryProvider::createFieldOffsets(inputSchema, fieldKeyNames, fieldValueNames);
    HashMapOptions hashMapOptions{
        std::make_unique<MurMur3HashFunction>(),
        std::move(keyFunctions),
        fieldKeys,
        fieldValues,
        entriesPerPage,
        entrySize,
        keySize,
        valueSize,
        pageSize,
        numberOfBuckets};
    return hashMapOptions;
}
}

LoweringRuleResultSubgraph LowerToPhysicalHashJoin::apply(LogicalOperator logicalOperator)
{
    auto join = logicalOperator.getAs<JoinLogicalOperator>();
    const auto children = join->getBothChildren();
    const auto traitSet = join->getTraitSet();
    auto outputOriginIds = traitSet.get<OutputOriginIdsTrait>();
    const auto memoryLayoutTypeTrait = traitSet.get<MemoryLayoutTypeTrait>();
    const auto memoryLayoutType = memoryLayoutTypeTrait->memoryLayout;
    PRECONDITION(std::ranges::size(*outputOriginIds) == 1, "Expected one output origin id");

    const auto& leftOperator = children[0];
    const auto& rightOperator = children[1];

    const auto logicalOutputSchema = join.getOutputSchema();
    const auto physicalOutputSchema = createPhysicalOutputSchema(traitSet);
    auto outputOriginId = (*outputOriginIds)[0];
    auto logicalJoinFunction = join->getJoinFunction();
    auto windowType = join->getWindowType();
    const auto& joinTimeCharacteristicsVariant = join->getJoinTimeCharacteristics();
    auto characteristicsAreBound
        = std::holds_alternative<std::array<Windowing::BoundTimeCharacteristic, 2>>(joinTimeCharacteristicsVariant);
    PRECONDITION(characteristicsAreBound, "Expected the join time characteristics to be bound");
    const auto& [timeStampFieldLeft, timeStampFieldRight]
        = std::get<std::array<Windowing::BoundTimeCharacteristic, 2>>(joinTimeCharacteristicsVariant);

    auto combinedFieldMappingVec = join->getChildren()
        | std::views::transform([](const auto& child)
                                { return child.getTraitSet().template get<FieldMappingTrait>()->getUnderlying() | std::views::all; })
        | std::views::join | std::views::common | std::ranges::to<std::unordered_map>();
    auto combinedFieldMapping = FieldMappingTrait{std::move(combinedFieldMappingVec)};

    auto physicalJoinFunction = QueryCompilation::FunctionProvider::lowerFunction(logicalJoinFunction, combinedFieldMapping);
    const auto inputOriginIds = join.getChildren()
        | std::views::transform(
                                    [](const auto& child)
                                    {
                                        auto childOutputOriginIds = child.getTraitSet().template get<OutputOriginIdsTrait>();
                                        return *childOutputOriginIds;
                                    })
        | std::views::join | std::ranges::to<std::vector<OriginId>>();

    /// Design-space knobs of the join: storage (S1-S3), processing (P1-P4), and trigger (T1/T2).
    /// Unsupported settings degrade to the default with a warning instead of throwing: a throw here executes
    /// during query compilation on the deployed node and stalls the peers of the distributed plan.
    auto storageVariant = conf.joinStorage.getValue();
    const auto buildVariant = conf.joinBuild.getValue();
    const auto probeVariant = conf.joinProbe.getValue();
    const auto probeRanges = conf.joinProbeRanges.getValue();
    if (conf.joinTrigger.getValue() == JoinTriggerVariant::EAGER)
    {
        NES_WARNING(
            "join_trigger=EAGER (T2) is implemented for the index join (join_strategy=INDEX_JOIN) only; "
            "falling back to LAZY (T1) for the hash join.");
    }
    if (storageVariant == JoinStorageVariant::TUPLE_CHAINED and isOuterJoin(join->getJoinType()))
    {
        NES_WARNING("join_storage=TUPLE_CHAINED (S1) supports inner joins only; falling back to KEY_GROUPED (S2) for this join.");
        storageVariant = JoinStorageVariant::KEY_GROUPED;
    }

    /// Our current hash join implementation uses a hash table that requires each key to be 100% identical in terms of no. fields and data types.
    /// Therefore, we need to create map operators that extend and cast the fields to the correct data types.
    auto [leftJoinFields, rightJoinFields] = getJoinFieldExtensionsLeftRight(leftOperator, rightOperator, logicalJoinFunction);
    auto [newLeftInputSchema, leftMapOperators] = addMapOperators(leftOperator, leftJoinFields, memoryLayoutType);
    auto [newRightInputSchema, rightMapOperators] = addMapOperators(rightOperator, rightJoinFields, memoryLayoutType);
    auto leftTupleLayout = std::make_shared<DefaultPagedVectorTupleLayout>(newLeftInputSchema);
    auto rightTupleLayout = std::make_shared<DefaultPagedVectorTupleLayout>(newRightInputSchema);
    /// One-sided directories (inner joins only): the probe scans the right side's entries and probes
    /// the left map, so the right side stores inline per-tuple entries -- no per-key allocations on
    /// the right. SMALLER degrades to the static left choice: per-tuple directories are built during
    /// ingestion, before the side sizes are known.
    auto rightStorageVariant = storageVariant;
    if (conf.joinDirectorySides.getValue() != JoinDirectorySides::BOTH and not isOuterJoin(join->getJoinType()))
    {
        rightStorageVariant = JoinStorageVariant::TUPLE_CHAINED;
    }
    auto leftHashMapOptions = createHashMapOptions(leftJoinFields, newLeftInputSchema, conf, storageVariant);
    auto rightHashMapOptions = createHashMapOptions(rightJoinFields, newRightInputSchema, conf, rightStorageVariant);

    /// Creating the hash join operator handler and slice store
    auto handlerId = getNextOperatorHandlerId();
    auto sliceAndWindowStore = std::make_unique<DefaultTimeBasedSliceStore>(
        windowType.getSize().getTime(), windowType.getSlide().getTime(), conf.sliceCacheConfiguration);
    auto sliceStoreRefLeft = sliceAndWindowStore->createSliceStoreRef(
        [](Slice& slice, const WorkerThreadId workerThreadId)
        {
            auto& hjSlice = dynamic_cast<HJSlice&>(slice);
            return hjSlice.getHashMapBufferRefForSide(workerThreadId, JoinBuildSideType::Left);
        },
        [hashMapOptions = leftHashMapOptions, rightValueSize = rightHashMapOptions.valueSize](
            WindowBasedOperatorHandler& handler, AbstractBufferProvider& bufferProvider)
        {
            const CreateNewHJSliceArgs hashMapSliceArgs{
                hashMapOptions.keySize,
                hashMapOptions.valueSize,
                rightValueSize,
                hashMapOptions.pageSize,
                hashMapOptions.numberOfBuckets,
                &bufferProvider,
                JoinBuildSideType::Left};
            return handler.getCreateNewSlicesFunction(hashMapSliceArgs);
        });
    auto sliceStoreRefRight = sliceAndWindowStore->createSliceStoreRef(
        [](Slice& slice, const WorkerThreadId workerThreadId)
        {
            auto& hjSlice = dynamic_cast<HJSlice&>(slice);
            return hjSlice.getHashMapBufferRefForSide(workerThreadId, JoinBuildSideType::Right);
        },
        [hashMapOptions = rightHashMapOptions, leftValueSize = leftHashMapOptions.valueSize](
            WindowBasedOperatorHandler& handler, AbstractBufferProvider& bufferProvider)
        {
            /// keySize/valueSize describe the LEFT side, rightValueSize the right side (see CreateNewHJSliceArgs).
            const CreateNewHJSliceArgs hashMapSliceArgs{
                hashMapOptions.keySize,
                leftValueSize,
                hashMapOptions.valueSize,
                hashMapOptions.pageSize,
                hashMapOptions.numberOfBuckets,
                &bufferProvider,
                JoinBuildSideType::Right};
            return handler.getCreateNewSlicesFunction(hashMapSliceArgs);
        });
    /// Create the trigger strategy based on join type — determines what probe tasks are emitted at runtime
    const auto currentJoinType = join->getJoinType();
    using JT = JoinLogicalOperator::JoinType;
    auto createTriggerStrategy = [&]() -> JoinTriggerStrategy
    {
        switch (currentJoinType)
        {
            case JT::OUTER_LEFT_JOIN:
                return OuterJoinTriggerStrategy<true, false>{};
            case JT::OUTER_RIGHT_JOIN:
                return OuterJoinTriggerStrategy<false, true>{};
            case JT::OUTER_FULL_JOIN:
                return OuterJoinTriggerStrategy<true, true>{};
            case JT::CARTESIAN_PRODUCT:
            case JT::INNER_JOIN:
                return InnerJoinTriggerStrategy{};
        }
        std::unreachable();
    };

    /// S3 (FIXED_ARRAY): fix the bucket-array size to the estimated key cardinality, disabling adaptive sizing.
    const auto fixedNumberOfBuckets = storageVariant == JoinStorageVariant::FIXED_ARRAY
        ? std::optional<uint64_t>{conf.joinFixedBuckets.getValue()}
        : std::nullopt;
    auto handler = std::make_shared<HJOperatorHandler>(
        inputOriginIds,
        outputOriginId,
        std::move(sliceAndWindowStore),
        conf.maxNumberOfBuckets,
        createTriggerStrategy(),
        buildVariant,
        probeVariant,
        probeRanges,
        fixedNumberOfBuckets);

    /// Creating the left and right hash join build operator
    const auto sharedHashMap = buildVariant == JoinBuildVariant::SHARED_TABLE;
    const HJBuildPhysicalOperator leftBuildOperator{
        handlerId,
        JoinBuildSideType::Left,
        TimeFunction::create(timeStampFieldLeft),
        leftTupleLayout,
        leftHashMapOptions,
        std::move(sliceStoreRefLeft),
        storageVariant,
        sharedHashMap};
    const HJBuildPhysicalOperator rightBuildOperator{
        handlerId,
        JoinBuildSideType::Right,
        TimeFunction::create(timeStampFieldRight),
        rightTupleLayout,
        rightHashMapOptions,
        std::move(sliceStoreRefRight),
        rightStorageVariant,
        sharedHashMap};

    /// Creating the hash join probe — select inner or outer probe based on join type
    auto joinSchema = JoinSchema(newLeftInputSchema, newRightInputSchema, physicalOutputSchema);

    /// Building operator wrapper for the two builds and the probe.
    auto leftBuildWrapper = std::make_shared<PhysicalOperatorWrapper>(
        std::move(leftBuildOperator),
        newLeftInputSchema,
        physicalOutputSchema,
        memoryLayoutType,
        memoryLayoutType,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::EMIT);

    auto rightBuildWrapper = std::make_shared<PhysicalOperatorWrapper>(
        std::move(rightBuildOperator),
        newRightInputSchema,
        physicalOutputSchema,
        memoryLayoutType,
        memoryLayoutType,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::EMIT);

    /// Verify at compile time that both probe operators satisfy the JoinProbeOperator concept
    static_assert(JoinProbeOperator<HJInnerProbePhysicalOperator>);
    static_assert(JoinProbeOperator<HJOuterProbePhysicalOperator>);

    auto createProbeWrapper = [&](const auto& probeOperator)
    {
        return std::make_shared<PhysicalOperatorWrapper>(
            std::move(probeOperator),
            physicalOutputSchema,
            physicalOutputSchema,
            memoryLayoutType,
            memoryLayoutType,
            handlerId,
            handler,
            PhysicalOperatorWrapper::PipelineLocation::SCAN,
            std::vector{leftBuildWrapper, rightBuildWrapper});
    };

    std::shared_ptr<PhysicalOperatorWrapper> probeWrapper;
    if (isOuterJoin(currentJoinType))
    {
        PRECONDITION(
            HJOuterProbePhysicalOperator::supportsJoinType(currentJoinType), "HJOuterProbePhysicalOperator does not support join type");
        probeWrapper = createProbeWrapper(HJOuterProbePhysicalOperator(
            handlerId,
            physicalJoinFunction,
            WindowMetaData{join->getStartField(), join->getEndField()},
            joinSchema,
            leftTupleLayout,
            rightTupleLayout,
            leftHashMapOptions,
            rightHashMapOptions));
    }
    else
    {
        PRECONDITION(
            HJInnerProbePhysicalOperator::supportsJoinType(currentJoinType), "HJInnerProbePhysicalOperator does not support join type");
        probeWrapper = createProbeWrapper(HJInnerProbePhysicalOperator(
            handlerId,
            physicalJoinFunction,
            WindowMetaData{join->getStartField(), join->getEndField()},
            joinSchema,
            leftTupleLayout,
            rightTupleLayout,
            leftHashMapOptions,
            rightHashMapOptions,
            storageVariant,
            rightStorageVariant));
    }

    std::shared_ptr<PhysicalOperatorWrapper> leftLeaf = leftBuildWrapper;
    std::shared_ptr<PhysicalOperatorWrapper> rightLeaf = rightBuildWrapper;
    /// As we have the query plan still flipped, we need to iterate in reverse for inserting the map operators into the query plan
    if (not leftMapOperators.empty())
    {
        for (const auto& mapPhysicalOperator : leftMapOperators | std::views::reverse)
        {
            leftLeaf->addChild(mapPhysicalOperator);
            leftLeaf = mapPhysicalOperator;
        }
    }
    if (not rightMapOperators.empty())
    {
        for (const auto& mapPhysicalOperator : rightMapOperators | std::views::reverse)
        {
            rightLeaf->addChild(mapPhysicalOperator);
            rightLeaf = mapPhysicalOperator;
        }
    }

    return {.root = {probeWrapper}, .leaves = {leftLeaf, rightLeaf}};
};

std::unique_ptr<AbstractLoweringRule>
LoweringRuleGeneratedRegistrar::RegisterHashJoinLoweringRule(LoweringRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalHashJoin>(argument.conf);
}

}
