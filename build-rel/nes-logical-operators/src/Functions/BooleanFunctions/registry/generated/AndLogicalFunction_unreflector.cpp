/// Auto-generated unreflector glue for LogicalFunction::And.
/// Self-registers at static initialization time; kept alive by --whole-archive on the glue sub-library.
#include <stdexcept>
#include <string>
#include <LogicalFunctionUnreflectionRegistry.hpp>
#include <Functions/BooleanFunctions/AndLogicalFunction.hpp>

namespace NES
{
namespace
{
const auto registered_And_LogicalFunction = [] {
    const bool registered = LogicalFunctionUnreflectionRegistry::instance().addUnreflectorEntry(
        "And",
        [](const Reflected& data, const ReflectionContext& context) {
            return context.unreflect<AndLogicalFunction>(data);
        });
    if (!registered)
    {
        /// Static-init context: an uncaught throw aborts the program loudly, without main() being able to catch it.
        throw std::runtime_error{std::string{"Duplicate unreflection registration for \"And\" in LogicalFunction"}};
    }
    return 0;
}();
}
}
