/// Auto-generated unreflector glue for AggregationLogicalFunction::Avg.
/// Self-registers at static initialization time; kept alive by --whole-archive on the glue sub-library.
#include <stdexcept>
#include <string>
#include <AggregationLogicalFunctionUnreflectionRegistry.hpp>
#include <Operators/Windows/Aggregations/AvgAggregationLogicalFunction.hpp>

namespace NES
{
namespace
{
const auto registered_Avg_AggregationLogicalFunction = [] {
    const bool registered = AggregationLogicalFunctionUnreflectionRegistry::instance().addUnreflectorEntry(
        "Avg",
        [](const Reflected& data, const ReflectionContext& context) {
            return context.unreflect<AvgAggregationLogicalFunction>(data);
        });
    if (!registered)
    {
        /// Static-init context: an uncaught throw aborts the program loudly, without main() being able to catch it.
        throw std::runtime_error{std::string{"Duplicate unreflection registration for \"Avg\" in AggregationLogicalFunction"}};
    }
    return 0;
}();
}
}
