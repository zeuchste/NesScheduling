/// Auto-generated unreflector glue for LogicalOperator::InlineSink.
/// Self-registers at static initialization time; kept alive by --whole-archive on the glue sub-library.
#include <stdexcept>
#include <string>
#include <LogicalOperatorUnreflectionRegistry.hpp>
#include <Operators/Sinks/InlineSinkLogicalOperator.hpp>

namespace NES
{
namespace
{
const auto registered_InlineSink_LogicalOperator = [] {
    const bool registered = LogicalOperatorUnreflectionRegistry::instance().addUnreflectorEntry(
        "InlineSink",
        [](const Reflected& data, const ReflectionContext& context) {
            return context.unreflect<TypedLogicalOperator<InlineSinkLogicalOperator>>(data);
        });
    if (!registered)
    {
        /// Static-init context: an uncaught throw aborts the program loudly, without main() being able to catch it.
        throw std::runtime_error{std::string{"Duplicate unreflection registration for \"InlineSink\" in LogicalOperator"}};
    }
    return 0;
}();
}
}
