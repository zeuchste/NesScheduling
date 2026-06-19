/// Auto-generated unreflector glue for AggregationLogicalFunction::Sum.
/// Self-registers at static initialization time; kept alive by --whole-archive on the glue sub-library.
#include <stdexcept>
#include <string>
#include <AggregationLogicalFunctionUnreflectionRegistry.hpp>
#include <Operators/Windows/Aggregations/SumAggregationLogicalFunction.hpp>

namespace NES
{
namespace
{
const auto registered_Sum_AggregationLogicalFunction = [] {
    const bool registered = AggregationLogicalFunctionUnreflectionRegistry::instance().addUnreflectorEntry(
        "Sum",
        [](const Reflected& data, const ReflectionContext& context) {
            return context.unreflect<SumAggregationLogicalFunction>(data);
        });
    if (!registered)
    {
        /// Static-init context: an uncaught throw aborts the program loudly, without main() being able to catch it.
        throw std::runtime_error{std::string{"Duplicate unreflection registration for \"Sum\" in AggregationLogicalFunction"}};
    }
    return 0;
}();
}
}
