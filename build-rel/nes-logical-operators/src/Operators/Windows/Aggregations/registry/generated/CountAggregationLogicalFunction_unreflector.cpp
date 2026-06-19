/// Auto-generated unreflector glue for AggregationLogicalFunction::Count.
/// Self-registers at static initialization time; kept alive by --whole-archive on the glue sub-library.
#include <stdexcept>
#include <string>
#include <AggregationLogicalFunctionUnreflectionRegistry.hpp>
#include <Operators/Windows/Aggregations/CountAggregationLogicalFunction.hpp>

namespace NES
{
namespace
{
const auto registered_Count_AggregationLogicalFunction = [] {
    const bool registered = AggregationLogicalFunctionUnreflectionRegistry::instance().addUnreflectorEntry(
        "Count",
        [](const Reflected& data, const ReflectionContext& context) {
            return context.unreflect<CountAggregationLogicalFunction>(data);
        });
    if (!registered)
    {
        /// Static-init context: an uncaught throw aborts the program loudly, without main() being able to catch it.
        throw std::runtime_error{std::string{"Duplicate unreflection registration for \"Count\" in AggregationLogicalFunction"}};
    }
    return 0;
}();
}
}
