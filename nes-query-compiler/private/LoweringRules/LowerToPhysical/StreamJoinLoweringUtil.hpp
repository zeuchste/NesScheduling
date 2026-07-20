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

#include <algorithm>
#include <memory>
#include <ranges>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/CastToTypeLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Schema/Field.hpp>
#include <Traits/FieldMappingTrait.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Util/SchemaFactory.hpp>
#include <ErrorHandling.hpp>
#include <MapPhysicalOperator.hpp>
#include <PhysicalOperator.hpp>

/// Shared helpers of the key-based stream-join lowering rules (hash, sort-merge, index): pairing the join key
/// fields of both sides and inserting cast map operators so that both sides' keys have identical layouts —
/// a prerequisite for hashing tuples of either side consistently.
/// Header-only so that each add_plugin lowering rule can use them without a shared translation unit.
namespace NES::StreamJoinLoweringUtil
{

/// Helper struct for storing the old and new field name and datatype for each join comparison
struct FieldNamesExtension
{
    Field oldField;
    QualifiedUnboundField newField;
};

inline std::pair<std::vector<FieldNamesExtension>, std::vector<FieldNamesExtension>>
getJoinFieldExtensionsLeftRight(const LogicalOperator& leftChild, const LogicalOperator& rightChild, const LogicalFunction& joinFunction)
{
    /// Tuple  of left, right join fields and the combined data type, e.g., i32 and i8 --> i32
    std::vector<FieldNamesExtension> leftJoinNames;
    std::vector<FieldNamesExtension> rightJoinNames;

    /// Retrieves all leaf functions, as we need the leaf functions (join comparison) to check if they have the same number and data types
    /// for both join sides.
    std::unordered_set<LogicalFunction> parentsOfJoinComparisons;
    for (auto itr : BFSRange<LogicalFunction>(joinFunction))
    {
        /// If any child is a leaf function, we put the current function into the set
        const auto anyChildIsLeaf
            = std::ranges::any_of(itr.getChildren(), [](const LogicalFunction& child) { return child.getChildren().empty(); });
        if (anyChildIsLeaf)
        {
            parentsOfJoinComparisons.insert(itr);
        }
    }
    uint64_t counter = 0;
    std::ranges::for_each(
        parentsOfJoinComparisons,
        [leftChild, rightChild, &leftJoinNames, &rightJoinNames, &counter, &joinFunction](const LogicalFunction& parent)
        {
            /// We expect the parent to have exactly two children and that both children are FieldAccessLogicalFunction
            /// This should be true, as the join operator receives an input schema from its parent operator without any additional functions
            /// over the join fields.
            PRECONDITION(parent.getChildren().size() == 2, "Expect the parent to have exact two children, left and right join fields");
            const auto& firstField = parent.getChildren().at(0).tryGetAs<FieldAccessLogicalFunction>();
            const auto& secondField = parent.getChildren().at(1).tryGetAs<FieldAccessLogicalFunction>();
            if (not(firstField.has_value() && secondField.has_value()))
            {
                throw UnknownJoinStrategy(
                    "Could not handle join strategy that has chained logical functions operating over the join fields!");
            }

            auto [leftField, rightField] = [&]
            {
                if (firstField.value()->getField().getProducedBy() == leftChild)
                {
                    PRECONDITION(
                        secondField.value()->getField().getProducedBy() == rightChild, "Expected the second field to be the right field");
                    return std::pair{firstField.value()->getField(), secondField.value()->getField()};
                }
                PRECONDITION(
                    firstField.value()->getField().getProducedBy() == rightChild, "Expected the first field to be the right field");
                PRECONDITION(
                    secondField.value()->getField().getProducedBy() == leftChild, "Expected the second field to be the left field");
                return std::pair{secondField.value()->getField(), firstField.value()->getField()};
            }();
            if (leftField.getProducedBy() == rightField.getProducedBy())
            {
                throw UnknownJoinStrategy("Cannot handle self joins yet, but got {} as part of the predicate", joinFunction);
            }

            /// If they do not have the same data types, we need to cast both to a common one
            if (firstField->getDataType() != secondField->getDataType())
            {
                /// We are now converting the fields to a physical data type and then joining them together
                if (auto joinedDataType = leftField.getDataType().join(rightField.getDataType()); joinedDataType.has_value())
                {
                    const auto leftFieldNewName
                        = QualifiedIdentifier::create(leftField.getLastName(), Identifier::parse("j" + std::to_string(counter++)));
                    const auto rightFieldNewName
                        = QualifiedIdentifier::create(rightField.getLastName(), Identifier::parse("j" + std::to_string(counter++)));
                    leftJoinNames.emplace_back(
                        FieldNamesExtension{.oldField = leftField, .newField = QualifiedUnboundField{leftFieldNewName, *joinedDataType}});
                    rightJoinNames.emplace_back(
                        FieldNamesExtension{.oldField = rightField, .newField = QualifiedUnboundField{rightFieldNewName, *joinedDataType}});
                }
                else
                {
                    throw UnknownJoinStrategy("Cannot join field types {} and {}", leftField.getDataType(), rightField.getDataType());
                }
            }
            else
            {
                leftJoinNames.emplace_back(FieldNamesExtension{
                    .oldField = leftField, .newField = QualifiedUnboundField{leftField.getLastName(), leftField.getDataType()}});
                rightJoinNames.emplace_back(FieldNamesExtension{
                    .oldField = rightField, .newField = QualifiedUnboundField{rightField.getLastName(), rightField.getDataType()}});
            }
        });

    return {leftJoinNames, rightJoinNames};
}

/// Creates for each field a map operator that has as its function a cast to the correct data type
inline std::pair<Schema<QualifiedUnboundField, Ordered>, std::vector<std::shared_ptr<PhysicalOperatorWrapper>>> addMapOperators(
    const LogicalOperator& inputOperator,
    const std::vector<FieldNamesExtension>& fieldNameExtensions,
    const MemoryLayoutType& memoryLayoutType)
{
    auto currentFields = createPhysicalOutputSchema(inputOperator.getTraitSet()) | std::ranges::to<std::vector<QualifiedUnboundField>>();
    std::vector<std::shared_ptr<PhysicalOperatorWrapper>> mapPhysicalOperators;
    for (const auto& [oldField, newField] : fieldNameExtensions)
    {
        if (oldField.getLastName() == newField.getFullyQualifiedName() and oldField.getDataType() == newField.getDataType())
        {
            continue;
        }

        /// Creating a new physical function that reads from the old field and casts it to the new data type
        const FieldAccessLogicalFunction fieldAccessOldField(oldField);
        const CastToTypeLogicalFunction castToTypeFunction(newField.getDataType(), fieldAccessOldField);
        const PhysicalFunction castedPhysicalFunction
            = QueryCompilation::FunctionProvider::lowerFunction(castToTypeFunction, *inputOperator.getTraitSet().get<FieldMappingTrait>());

        /// Get a copy of the current input schema before adding to the inputSchemaOfMap the newly added field
        auto inputSchema = Schema<QualifiedUnboundField, Ordered>{currentFields};
        currentFields.emplace_back(newField);
        const Schema<QualifiedUnboundField, Ordered> outputSchema(currentFields);

        /// Create a new map operator with the cast as its function
        mapPhysicalOperators.emplace_back(std::make_shared<PhysicalOperatorWrapper>(
            MapPhysicalOperator(newField.getFullyQualifiedName(), castedPhysicalFunction),
            inputSchema,
            outputSchema,
            memoryLayoutType,
            memoryLayoutType));
    }

    return {Schema<QualifiedUnboundField, Ordered>{currentFields}, mapPhysicalOperators};
}

}
