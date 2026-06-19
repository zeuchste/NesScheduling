/// Auto-generated unreflector glue for LogicalOperator::Projection.
/// Self-registers at static initialization time; kept alive by --whole-archive on the glue sub-library.
#include <stdexcept>
#include <string>
#include <LogicalOperatorUnreflectionRegistry.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>

namespace NES
{
namespace
{
const auto registered_Projection_LogicalOperator = [] {
    const bool registered = LogicalOperatorUnreflectionRegistry::instance().addUnreflectorEntry(
        "Projection",
        [](const Reflected& data, const ReflectionContext& context) {
            return context.unreflect<TypedLogicalOperator<ProjectionLogicalOperator>>(data);
        });
    if (!registered)
    {
        /// Static-init context: an uncaught throw aborts the program loudly, without main() being able to catch it.
        throw std::runtime_error{std::string{"Duplicate unreflection registration for \"Projection\" in LogicalOperator"}};
    }
    return 0;
}();
}
}
