/// Auto-generated unreflector glue for LogicalFunction::Day_Of.
/// Self-registers at static initialization time; kept alive by --whole-archive on the glue sub-library.
#include <stdexcept>
#include <string>
#include <LogicalFunctionUnreflectionRegistry.hpp>
#include <Functions/ExtractFromTimestampLogicalFunction.hpp>

namespace NES
{
namespace
{
const auto registered_Day_Of_LogicalFunction = [] {
    const bool registered = LogicalFunctionUnreflectionRegistry::instance().addUnreflectorEntry(
        "Day_Of",
        [](const Reflected& data, const ReflectionContext& context) {
            return context.unreflect<ExtractFromTimestampLogicalFunction>(data);
        });
    if (!registered)
    {
        /// Static-init context: an uncaught throw aborts the program loudly, without main() being able to catch it.
        throw std::runtime_error{std::string{"Duplicate unreflection registration for \"Day_Of\" in LogicalFunction"}};
    }
    return 0;
}();
}
}
