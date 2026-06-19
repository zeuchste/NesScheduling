/// Auto-generated unreflector glue for LogicalFunction::Pow.
/// Self-registers at static initialization time; kept alive by --whole-archive on the glue sub-library.
#include <stdexcept>
#include <string>
#include <LogicalFunctionUnreflectionRegistry.hpp>
#include <Functions/ArithmeticalFunctions/PowLogicalFunction.hpp>

namespace NES
{
namespace
{
const auto registered_Pow_LogicalFunction = [] {
    const bool registered = LogicalFunctionUnreflectionRegistry::instance().addUnreflectorEntry(
        "Pow",
        [](const Reflected& data, const ReflectionContext& context) {
            return context.unreflect<PowLogicalFunction>(data);
        });
    if (!registered)
    {
        /// Static-init context: an uncaught throw aborts the program loudly, without main() being able to catch it.
        throw std::runtime_error{std::string{"Duplicate unreflection registration for \"Pow\" in LogicalFunction"}};
    }
    return 0;
}();
}
}
