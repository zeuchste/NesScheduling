/// Auto-generated unreflector glue for LogicalFunction::OCTET_LENGTH.
/// Self-registers at static initialization time; kept alive by --whole-archive on the glue sub-library.
#include <stdexcept>
#include <string>
#include <LogicalFunctionUnreflectionRegistry.hpp>
#include <Functions/OctetLengthLogicalFunction.hpp>

namespace NES
{
namespace
{
const auto registered_OCTET_LENGTH_LogicalFunction = [] {
    const bool registered = LogicalFunctionUnreflectionRegistry::instance().addUnreflectorEntry(
        "OCTET_LENGTH",
        [](const Reflected& data, const ReflectionContext& context) {
            return context.unreflect<OctetLengthLogicalFunction>(data);
        });
    if (!registered)
    {
        /// Static-init context: an uncaught throw aborts the program loudly, without main() being able to catch it.
        throw std::runtime_error{std::string{"Duplicate unreflection registration for \"OCTET_LENGTH\" in LogicalFunction"}};
    }
    return 0;
}();
}
}
