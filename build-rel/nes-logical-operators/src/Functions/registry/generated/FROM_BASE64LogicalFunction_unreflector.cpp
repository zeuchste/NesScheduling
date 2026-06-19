/// Auto-generated unreflector glue for LogicalFunction::FROM_BASE64.
/// Self-registers at static initialization time; kept alive by --whole-archive on the glue sub-library.
#include <stdexcept>
#include <string>
#include <LogicalFunctionUnreflectionRegistry.hpp>
#include <Functions/FromBase64LogicalFunction.hpp>

namespace NES
{
namespace
{
const auto registered_FROM_BASE64_LogicalFunction = [] {
    const bool registered = LogicalFunctionUnreflectionRegistry::instance().addUnreflectorEntry(
        "FROM_BASE64",
        [](const Reflected& data, const ReflectionContext& context) {
            return context.unreflect<FromBase64LogicalFunction>(data);
        });
    if (!registered)
    {
        /// Static-init context: an uncaught throw aborts the program loudly, without main() being able to catch it.
        throw std::runtime_error{std::string{"Duplicate unreflection registration for \"FROM_BASE64\" in LogicalFunction"}};
    }
    return 0;
}();
}
}
