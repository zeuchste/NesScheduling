/// Auto-generated unreflector glue for LogicalOperator::SourceName.
/// Self-registers at static initialization time; kept alive by --whole-archive on the glue sub-library.
#include <stdexcept>
#include <string>
#include <LogicalOperatorUnreflectionRegistry.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>

namespace NES
{
namespace
{
const auto registered_SourceName_LogicalOperator = [] {
    const bool registered = LogicalOperatorUnreflectionRegistry::instance().addUnreflectorEntry(
        "SourceName",
        [](const Reflected& data, const ReflectionContext& context) {
            return context.unreflect<TypedLogicalOperator<SourceNameLogicalOperator>>(data);
        });
    if (!registered)
    {
        /// Static-init context: an uncaught throw aborts the program loudly, without main() being able to catch it.
        throw std::runtime_error{std::string{"Duplicate unreflection registration for \"SourceName\" in LogicalOperator"}};
    }
    return 0;
}();
}
}
