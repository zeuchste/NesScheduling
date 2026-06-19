/// Auto-generated unreflector glue for LogicalFunction::CastFromUnixTs.
/// Self-registers at static initialization time; kept alive by --whole-archive on the glue sub-library.
#include <stdexcept>
#include <string>
#include <LogicalFunctionUnreflectionRegistry.hpp>
#include <Functions/CastFromUnixTimestampLogicalFunction.hpp>

namespace NES
{
namespace
{
const auto registered_CastFromUnixTs_LogicalFunction = [] {
    const bool registered = LogicalFunctionUnreflectionRegistry::instance().addUnreflectorEntry(
        "CastFromUnixTs",
        [](const Reflected& data, const ReflectionContext& context) {
            return context.unreflect<CastFromUnixTimestampLogicalFunction>(data);
        });
    if (!registered)
    {
        /// Static-init context: an uncaught throw aborts the program loudly, without main() being able to catch it.
        throw std::runtime_error{std::string{"Duplicate unreflection registration for \"CastFromUnixTs\" in LogicalFunction"}};
    }
    return 0;
}();
}
}
