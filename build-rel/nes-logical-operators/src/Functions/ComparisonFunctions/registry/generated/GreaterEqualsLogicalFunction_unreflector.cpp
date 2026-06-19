/// Auto-generated unreflector glue for LogicalFunction::GreaterEquals.
/// Self-registers at static initialization time; kept alive by --whole-archive on the glue sub-library.
#include <stdexcept>
#include <string>
#include <LogicalFunctionUnreflectionRegistry.hpp>
#include <Functions/ComparisonFunctions/GreaterEqualsLogicalFunction.hpp>

namespace NES
{
namespace
{
const auto registered_GreaterEquals_LogicalFunction = [] {
    const bool registered = LogicalFunctionUnreflectionRegistry::instance().addUnreflectorEntry(
        "GreaterEquals",
        [](const Reflected& data, const ReflectionContext& context) {
            return context.unreflect<GreaterEqualsLogicalFunction>(data);
        });
    if (!registered)
    {
        /// Static-init context: an uncaught throw aborts the program loudly, without main() being able to catch it.
        throw std::runtime_error{std::string{"Duplicate unreflection registration for \"GreaterEquals\" in LogicalFunction"}};
    }
    return 0;
}();
}
}
