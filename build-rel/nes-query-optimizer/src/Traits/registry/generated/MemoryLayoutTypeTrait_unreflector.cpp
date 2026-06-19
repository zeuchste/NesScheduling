/// Auto-generated unreflector glue for Trait::MemoryLayoutType.
/// Self-registers at static initialization time; kept alive by --whole-archive on the glue sub-library.
#include <stdexcept>
#include <string>
#include <TraitUnreflectionRegistry.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>

namespace NES
{
namespace
{
const auto registered_MemoryLayoutType_Trait = [] {
    const bool registered = TraitUnreflectionRegistry::instance().addUnreflectorEntry(
        "MemoryLayoutType",
        [](const Reflected& data, const ReflectionContext& context) {
            return context.unreflect<MemoryLayoutTypeTrait>(data);
        });
    if (!registered)
    {
        /// Static-init context: an uncaught throw aborts the program loudly, without main() being able to catch it.
        throw std::runtime_error{std::string{"Duplicate unreflection registration for \"MemoryLayoutType\" in Trait"}};
    }
    return 0;
}();
}
}
