/// Auto-generated unreflector glue for LogicalFunction::CastToType.
/// Self-registers at static initialization time; kept alive by --whole-archive on the glue sub-library.
#include <stdexcept>
#include <string>
#include <LogicalFunctionUnreflectionRegistry.hpp>
#include <Functions/CastToTypeLogicalFunction.hpp>

namespace NES
{
namespace
{
const auto registered_CastToType_LogicalFunction = [] {
    const bool registered = LogicalFunctionUnreflectionRegistry::instance().addUnreflectorEntry(
        "CastToType",
        [](const Reflected& data, const ReflectionContext& context) {
            return context.unreflect<CastToTypeLogicalFunction>(data);
        });
    if (!registered)
    {
        /// Static-init context: an uncaught throw aborts the program loudly, without main() being able to catch it.
        throw std::runtime_error{std::string{"Duplicate unreflection registration for \"CastToType\" in LogicalFunction"}};
    }
    return 0;
}();
}
}
