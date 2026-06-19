/// Auto-generated unreflector glue for LogicalFunction::CHAR_LENGTH.
/// Self-registers at static initialization time; kept alive by --whole-archive on the glue sub-library.
#include <stdexcept>
#include <string>
#include <LogicalFunctionUnreflectionRegistry.hpp>
#include <Functions/CharLengthLogicalFunction.hpp>

namespace NES
{
namespace
{
const auto registered_CHAR_LENGTH_LogicalFunction = [] {
    const bool registered = LogicalFunctionUnreflectionRegistry::instance().addUnreflectorEntry(
        "CHAR_LENGTH",
        [](const Reflected& data, const ReflectionContext& context) {
            return context.unreflect<CharLengthLogicalFunction>(data);
        });
    if (!registered)
    {
        /// Static-init context: an uncaught throw aborts the program loudly, without main() being able to catch it.
        throw std::runtime_error{std::string{"Duplicate unreflection registration for \"CHAR_LENGTH\" in LogicalFunction"}};
    }
    return 0;
}();
}
}
