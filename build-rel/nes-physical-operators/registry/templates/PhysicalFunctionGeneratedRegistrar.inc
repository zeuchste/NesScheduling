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

/// NOLINT(clang-diagnostic-error)
#ifndef INCLUDED_FROM_REGISTRY_PHYSICAL_FUNCTION
#    error "This file should not be included directly! Include instead include <PhysicalFunctionRegistry.hpp>"
#endif

#include <memory>
#include <string>
#include <vector>
#include <Functions/PhysicalFunction.hpp>
#include <Util/Registry.hpp>

namespace NES::PhysicalFunctionGeneratedRegistrar
{

/// declaration of register functions for 'PhysicalFunctions'
PhysicalFunctionRegistryReturnType RegisterConcatPhysicalFunction(PhysicalFunctionRegistryArguments);
PhysicalFunctionRegistryReturnType RegisterOCTET_LENGTHPhysicalFunction(PhysicalFunctionRegistryArguments);
PhysicalFunctionRegistryReturnType RegisterCHAR_LENGTHPhysicalFunction(PhysicalFunctionRegistryArguments);
PhysicalFunctionRegistryReturnType RegisterFROM_BASE64PhysicalFunction(PhysicalFunctionRegistryArguments);
PhysicalFunctionRegistryReturnType RegisterTO_BASE64PhysicalFunction(PhysicalFunctionRegistryArguments);
PhysicalFunctionRegistryReturnType RegisterCastToTypePhysicalFunction(PhysicalFunctionRegistryArguments);
PhysicalFunctionRegistryReturnType RegisterCastToUnixTsPhysicalFunction(PhysicalFunctionRegistryArguments);
PhysicalFunctionRegistryReturnType RegisterCastFromUnixTsPhysicalFunction(PhysicalFunctionRegistryArguments);
PhysicalFunctionRegistryReturnType RegisterVarSizedToNumericPhysicalFunction(PhysicalFunctionRegistryArguments);
PhysicalFunctionRegistryReturnType RegisterDay_OfPhysicalFunction(PhysicalFunctionRegistryArguments);
PhysicalFunctionRegistryReturnType RegisterMonth_OfPhysicalFunction(PhysicalFunctionRegistryArguments);
PhysicalFunctionRegistryReturnType RegisterYear_OfPhysicalFunction(PhysicalFunctionRegistryArguments);
PhysicalFunctionRegistryReturnType RegisterAbsPhysicalFunction(PhysicalFunctionRegistryArguments);
PhysicalFunctionRegistryReturnType RegisterAddPhysicalFunction(PhysicalFunctionRegistryArguments);
PhysicalFunctionRegistryReturnType RegisterCeilPhysicalFunction(PhysicalFunctionRegistryArguments);
PhysicalFunctionRegistryReturnType RegisterDivPhysicalFunction(PhysicalFunctionRegistryArguments);
PhysicalFunctionRegistryReturnType RegisterFloorPhysicalFunction(PhysicalFunctionRegistryArguments);
PhysicalFunctionRegistryReturnType RegisterModPhysicalFunction(PhysicalFunctionRegistryArguments);
PhysicalFunctionRegistryReturnType RegisterMulPhysicalFunction(PhysicalFunctionRegistryArguments);
PhysicalFunctionRegistryReturnType RegisterPowPhysicalFunction(PhysicalFunctionRegistryArguments);
PhysicalFunctionRegistryReturnType RegisterRoundPhysicalFunction(PhysicalFunctionRegistryArguments);
PhysicalFunctionRegistryReturnType RegisterSubPhysicalFunction(PhysicalFunctionRegistryArguments);
PhysicalFunctionRegistryReturnType RegisterSqrtPhysicalFunction(PhysicalFunctionRegistryArguments);
PhysicalFunctionRegistryReturnType RegisterGreaterPhysicalFunction(PhysicalFunctionRegistryArguments);
PhysicalFunctionRegistryReturnType RegisterGreaterEqualsPhysicalFunction(PhysicalFunctionRegistryArguments);
PhysicalFunctionRegistryReturnType RegisterLessPhysicalFunction(PhysicalFunctionRegistryArguments);
PhysicalFunctionRegistryReturnType RegisterLessEqualsPhysicalFunction(PhysicalFunctionRegistryArguments);
PhysicalFunctionRegistryReturnType RegisterAndPhysicalFunction(PhysicalFunctionRegistryArguments);
PhysicalFunctionRegistryReturnType RegisterEqualsPhysicalFunction(PhysicalFunctionRegistryArguments);
PhysicalFunctionRegistryReturnType RegisterNegatePhysicalFunction(PhysicalFunctionRegistryArguments);
PhysicalFunctionRegistryReturnType RegisterOrPhysicalFunction(PhysicalFunctionRegistryArguments);
PhysicalFunctionRegistryReturnType RegisterIsNullPhysicalFunction(PhysicalFunctionRegistryArguments);

}

namespace NES
{
template <>
inline void
Registrar<PhysicalFunctionRegistry, std::string, PhysicalFunctionRegistryReturnType, PhysicalFunctionRegistryArguments>::registerAll(
    [[maybe_unused]] Registry<Registrar>& registry)
{
    using namespace NES::PhysicalFunctionGeneratedRegistrar;
    registry.addEntry("Concat", RegisterConcatPhysicalFunction);
	registry.addEntry("OCTET_LENGTH", RegisterOCTET_LENGTHPhysicalFunction);
	registry.addEntry("CHAR_LENGTH", RegisterCHAR_LENGTHPhysicalFunction);
	registry.addEntry("FROM_BASE64", RegisterFROM_BASE64PhysicalFunction);
	registry.addEntry("TO_BASE64", RegisterTO_BASE64PhysicalFunction);
	registry.addEntry("CastToType", RegisterCastToTypePhysicalFunction);
	registry.addEntry("CastToUnixTs", RegisterCastToUnixTsPhysicalFunction);
	registry.addEntry("CastFromUnixTs", RegisterCastFromUnixTsPhysicalFunction);
	registry.addEntry("VarSizedToNumeric", RegisterVarSizedToNumericPhysicalFunction);
	registry.addEntry("Day_Of", RegisterDay_OfPhysicalFunction);
	registry.addEntry("Month_Of", RegisterMonth_OfPhysicalFunction);
	registry.addEntry("Year_Of", RegisterYear_OfPhysicalFunction);
	registry.addEntry("Abs", RegisterAbsPhysicalFunction);
	registry.addEntry("Add", RegisterAddPhysicalFunction);
	registry.addEntry("Ceil", RegisterCeilPhysicalFunction);
	registry.addEntry("Div", RegisterDivPhysicalFunction);
	registry.addEntry("Floor", RegisterFloorPhysicalFunction);
	registry.addEntry("Mod", RegisterModPhysicalFunction);
	registry.addEntry("Mul", RegisterMulPhysicalFunction);
	registry.addEntry("Pow", RegisterPowPhysicalFunction);
	registry.addEntry("Round", RegisterRoundPhysicalFunction);
	registry.addEntry("Sub", RegisterSubPhysicalFunction);
	registry.addEntry("Sqrt", RegisterSqrtPhysicalFunction);
	registry.addEntry("Greater", RegisterGreaterPhysicalFunction);
	registry.addEntry("GreaterEquals", RegisterGreaterEqualsPhysicalFunction);
	registry.addEntry("Less", RegisterLessPhysicalFunction);
	registry.addEntry("LessEquals", RegisterLessEqualsPhysicalFunction);
	registry.addEntry("And", RegisterAndPhysicalFunction);
	registry.addEntry("Equals", RegisterEqualsPhysicalFunction);
	registry.addEntry("Negate", RegisterNegatePhysicalFunction);
	registry.addEntry("Or", RegisterOrPhysicalFunction);
	registry.addEntry("IsNull", RegisterIsNullPhysicalFunction);
	
}
}
