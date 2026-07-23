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

#include <LoweringRules/LowerToPhysical/LowerToPhysicalIndexJoin.hpp>

#include <array>
#include <memory>
#include <ranges>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/FieldAccessPhysicalFunction.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Interface/Hash/MurMur3HashFunction.hpp>
#include <Join/IndexJoin/IXJBuildPhysicalOperator.hpp>
#include <Join/IndexJoin/IXJInnerProbePhysicalOperator.hpp>
#include <Join/IndexJoin/IXJOperatorHandler.hpp>
#include <Join/IndexJoin/IXJSlice.hpp>
#include <Join/JoinTriggerStrategy.hpp>
#include <Join/NestedLoopJoin/NLJSlice.hpp>
#include <Join/StreamJoinOperatorHandler.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <LoweringRules/LowerToPhysical/StreamJoinLoweringUtil.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <SliceStore/DefaultTimeBasedSliceStore.hpp>
#include <SliceStore/Slice.hpp>
#include <Traits/FieldMappingTrait.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/SchemaFactory.hpp>
#include <Watermark/TimeFunction.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <ErrorHandling.hpp>
#include <LoweringRuleRegistry.hpp>
#include <PhysicalOperator.hpp>
#include <QueryExecutionConfiguration.hpp>
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{

LoweringRuleResultSubgraph LowerToPhysicalIndexJoin::apply(LogicalOperator logicalOperator)
{
    auto join = logicalOperator.getAs<JoinLogicalOperator>();
    const auto children = join->getBothChildren();
    const auto traitSet = join->getTraitSet();
    auto outputOriginIds = traitSet.get<OutputOriginIdsTrait>();
    const auto memoryLayoutType = traitSet.get<MemoryLayoutTypeTrait>()->memoryLayout;
    PRECONDITION(std::ranges::size(*outputOriginIds) == 1, "Expected one output origin id");
    PRECONDITION(
        join->getJoinType() == JoinLogicalOperator::JoinType::INNER_JOIN,
        "The index join supports inner joins only; outer joins fall back at plan time");

    const auto& leftOperator = children[0];
    const auto& rightOperator = children[1];
    const auto physicalOutputSchema = createPhysicalOutputSchema(traitSet);
    auto outputOriginId = (*outputOriginIds)[0];
    auto logicalJoinFunction = join->getJoinFunction();
    auto windowType = join->getWindowType();
    const auto& joinTimeCharacteristicsVariant = join->getJoinTimeCharacteristics();
    const auto characteristicsAreBound
        = std::holds_alternative<std::array<Windowing::BoundTimeCharacteristic, 2>>(joinTimeCharacteristicsVariant);
    PRECONDITION(characteristicsAreBound, "Expected the join time characteristics to be bound");
    const auto& [timeStampFieldLeft, timeStampFieldRight]
        = std::get<std::array<Windowing::BoundTimeCharacteristic, 2>>(joinTimeCharacteristicsVariant);

    auto combinedFieldMappingVec = join->getChildren()
        | std::views::transform([](const auto& child)
                                { return child.getTraitSet().template get<FieldMappingTrait>()->getUnderlying() | std::views::all; })
        | std::views::join | std::views::common | std::ranges::to<std::unordered_map>();
    auto physicalJoinFunction
        = QueryCompilation::FunctionProvider::lowerFunction(logicalJoinFunction, FieldMappingTrait{std::move(combinedFieldMappingVec)});

    const auto inputOriginIds = join.getChildren()
        | std::views::transform(
                                    [](const auto& child)
                                    {
                                        auto childOutputOriginIds = getTrait<OutputOriginIdsTrait>(child.getTraitSet());
                                        PRECONDITION(childOutputOriginIds.has_value(), "Expected the outputOriginIds trait to be set");
                                        return *childOutputOriginIds.value();
                                    })
        | std::views::join | std::ranges::to<std::vector<OriginId>>();

    /// Both the build (index maintenance) and the probe hash the key fields, so both sides' keys must have
    /// identical layouts: insert the same cast map operators the hash join uses.
    auto [leftJoinFields, rightJoinFields]
        = StreamJoinLoweringUtil::getJoinFieldExtensionsLeftRight(leftOperator, rightOperator, logicalJoinFunction);
    auto [newLeftInputSchema, leftMapOperators] = StreamJoinLoweringUtil::addMapOperators(leftOperator, leftJoinFields, memoryLayoutType);
    auto [newRightInputSchema, rightMapOperators]
        = StreamJoinLoweringUtil::addMapOperators(rightOperator, rightJoinFields, memoryLayoutType);
    auto leftTupleLayout = std::make_shared<DefaultPagedVectorTupleLayout>(newLeftInputSchema);
    auto rightTupleLayout = std::make_shared<DefaultPagedVectorTupleLayout>(newRightInputSchema);
    const uint64_t tupleSizeLeft = leftTupleLayout->getSchema().getSizeInBytes();
    const uint64_t tupleSizeRight = rightTupleLayout->getSchema().getSizeInBytes();

    const auto toKeyNames = [](const auto& joinFields)
    {
        return joinFields | std::views::transform([](const auto& extension) { return extension.newField.getFullyQualifiedName(); })
            | std::ranges::to<std::vector<Record::RecordFieldIdentifier>>();
    };
    const auto leftKeyFieldNames = toKeyNames(leftJoinFields);
    const auto rightKeyFieldNames = toKeyNames(rightJoinFields);
    const auto toKeyFunctions = [](const auto& keyNames)
    {
        return keyNames | std::views::transform([](const auto& name) { return PhysicalFunction{FieldAccessPhysicalFunction{name}}; })
            | std::ranges::to<std::vector<PhysicalFunction>>();
    };

    /// The slice store hands out the IXJSlice pointer itself: the build needs both the worker-local paged
    /// vector and the shared index of the slice.
    auto sliceAndWindowStore = std::make_unique<DefaultTimeBasedSliceStore>(
        windowType.getSize().getTime(), windowType.getSlide().getTime(), conf.sliceCacheConfiguration);
    auto makeSliceStoreRef = [&](const JoinBuildSideType side)
    {
        return sliceAndWindowStore->createSliceStoreRef(
            [side](Slice& slice, const WorkerThreadId workerThreadId) -> const TupleBuffer*
            {
                const auto& ixjSlice = dynamic_cast<IXJSlice&>(slice);
                return ixjSlice.getPagedVectorTupleBufferRef(workerThreadId, side);
            },
            [tupleSizeLeft, tupleSizeRight](const WindowBasedOperatorHandler& handler, AbstractBufferProvider& bufferProvider)
            {
                const CreateNewNLJSliceArgs nljSliceArgs{bufferProvider, tupleSizeLeft, tupleSizeRight};
                return handler.getCreateNewSlicesFunction(nljSliceArgs);
            });
    };
    auto sliceStoreRefLeft = makeSliceStoreRef(JoinBuildSideType::Left);
    auto sliceStoreRefRight = makeSliceStoreRef(JoinBuildSideType::Right);

    auto handler = std::make_shared<IXJOperatorHandler>(
        inputOriginIds, outputOriginId, std::move(sliceAndWindowStore), InnerJoinTriggerStrategy{});

    const auto handlerId = getNextOperatorHandlerId();
    const IXJBuildPhysicalOperator leftBuildOperator{
        handlerId,
        JoinBuildSideType::Left,
        TimeFunction::create(timeStampFieldLeft),
        leftTupleLayout,
        std::move(sliceStoreRefLeft),
        toKeyFunctions(leftKeyFieldNames),
        leftKeyFieldNames,
        std::make_shared<MurMur3HashFunction>()};
    const IXJBuildPhysicalOperator rightBuildOperator{
        handlerId,
        JoinBuildSideType::Right,
        TimeFunction::create(timeStampFieldRight),
        rightTupleLayout,
        std::move(sliceStoreRefRight),
        toKeyFunctions(rightKeyFieldNames),
        rightKeyFieldNames,
        std::make_shared<MurMur3HashFunction>()};

    auto joinSchema = JoinSchema(newLeftInputSchema, newRightInputSchema, physicalOutputSchema);

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

    static_assert(JoinProbeOperator<IXJInnerProbePhysicalOperator>);
    PRECONDITION(
        IXJInnerProbePhysicalOperator::supportsJoinType(join->getJoinType()), "IXJInnerProbePhysicalOperator does not support join type");
    auto probeWrapper = std::make_shared<PhysicalOperatorWrapper>(
        IXJInnerProbePhysicalOperator(
            handlerId,
            physicalJoinFunction,
            WindowMetaData{join->getStartField(), join->getEndField()},
            joinSchema,
            leftTupleLayout,
            rightTupleLayout,
            leftKeyFieldNames,
            rightKeyFieldNames,
            std::make_shared<MurMur3HashFunction>()),
        physicalOutputSchema,
        physicalOutputSchema,
        memoryLayoutType,
        memoryLayoutType,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::SCAN,
        std::vector{leftBuildWrapper, rightBuildWrapper});

    /// As we have the query plan still flipped, we need to iterate in reverse for inserting the map operators into the query plan
    std::shared_ptr<PhysicalOperatorWrapper> leftLeaf = leftBuildWrapper;
    std::shared_ptr<PhysicalOperatorWrapper> rightLeaf = rightBuildWrapper;
    for (const auto& mapPhysicalOperator : leftMapOperators | std::views::reverse)
    {
        leftLeaf->addChild(mapPhysicalOperator);
        leftLeaf = mapPhysicalOperator;
    }
    for (const auto& mapPhysicalOperator : rightMapOperators | std::views::reverse)
    {
        rightLeaf->addChild(mapPhysicalOperator);
        rightLeaf = mapPhysicalOperator;
    }

    return {.root = {probeWrapper}, .leaves = {leftLeaf, rightLeaf}};
};

std::unique_ptr<AbstractLoweringRule>
LoweringRuleGeneratedRegistrar::RegisterIndexJoinLoweringRule(LoweringRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalIndexJoin>(argument.conf);
}

}
