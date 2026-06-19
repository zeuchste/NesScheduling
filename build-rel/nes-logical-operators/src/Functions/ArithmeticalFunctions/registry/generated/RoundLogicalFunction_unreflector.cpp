/// Auto-generated unreflector glue for LogicalFunction::Round.
/// Self-registers at static initialization time; kept alive by --whole-archive on the glue sub-library.
#include <stdexcept>
#include <string>
#include <LogicalFunctionUnreflectionRegistry.hpp>
#include <Functions/ArithmeticalFunctions/RoundLogicalFunction.hpp>

namespace NES
{
namespace
{
const auto registered_Round_LogicalFunction = [] {
    const bool registered = LogicalFunctionUnreflectionRegistry::instance().addUnreflectorEntry(
        "Round",
        [](const Reflected& data, const ReflectionContext& context) {
            return context.unreflect<RoundLogicalFunction>(data);
        });
    if (!registered)
    {
        /// Static-init context: an uncaught throw aborts the program loudly, without main() being able to catch it.
        throw std::runtime_error{std::string{"Duplicate unreflection registration for \"Round\" in LogicalFunction"}};
    }
    return 0;
}();
}
}
