/// Auto-generated unreflector glue for LogicalFunction::Sub.
/// Self-registers at static initialization time; kept alive by --whole-archive on the glue sub-library.
#include <stdexcept>
#include <string>
#include <LogicalFunctionUnreflectionRegistry.hpp>
#include <Functions/ArithmeticalFunctions/SubLogicalFunction.hpp>

namespace NES
{
namespace
{
const auto registered_Sub_LogicalFunction = [] {
    const bool registered = LogicalFunctionUnreflectionRegistry::instance().addUnreflectorEntry(
        "Sub",
        [](const Reflected& data, const ReflectionContext& context) {
            return context.unreflect<SubLogicalFunction>(data);
        });
    if (!registered)
    {
        /// Static-init context: an uncaught throw aborts the program loudly, without main() being able to catch it.
        throw std::runtime_error{std::string{"Duplicate unreflection registration for \"Sub\" in LogicalFunction"}};
    }
    return 0;
}();
}
}
