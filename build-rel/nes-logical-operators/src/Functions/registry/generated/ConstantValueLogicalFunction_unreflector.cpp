/// Auto-generated unreflector glue for LogicalFunction::ConstantValue.
/// Self-registers at static initialization time; kept alive by --whole-archive on the glue sub-library.
#include <stdexcept>
#include <string>
#include <LogicalFunctionUnreflectionRegistry.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>

namespace NES
{
namespace
{
const auto registered_ConstantValue_LogicalFunction = [] {
    const bool registered = LogicalFunctionUnreflectionRegistry::instance().addUnreflectorEntry(
        "ConstantValue",
        [](const Reflected& data, const ReflectionContext& context) {
            return context.unreflect<ConstantValueLogicalFunction>(data);
        });
    if (!registered)
    {
        /// Static-init context: an uncaught throw aborts the program loudly, without main() being able to catch it.
        throw std::runtime_error{std::string{"Duplicate unreflection registration for \"ConstantValue\" in LogicalFunction"}};
    }
    return 0;
}();
}
}
