/// Auto-generated unreflector glue for Trait::Placement.
/// Self-registers at static initialization time; kept alive by --whole-archive on the glue sub-library.
#include <stdexcept>
#include <string>
#include <TraitUnreflectionRegistry.hpp>
#include <Traits/PlacementTrait.hpp>

namespace NES
{
namespace
{
const auto registered_Placement_Trait = [] {
    const bool registered = TraitUnreflectionRegistry::instance().addUnreflectorEntry(
        "Placement",
        [](const Reflected& data, const ReflectionContext& context) {
            return context.unreflect<PlacementTrait>(data);
        });
    if (!registered)
    {
        /// Static-init context: an uncaught throw aborts the program loudly, without main() being able to catch it.
        throw std::runtime_error{std::string{"Duplicate unreflection registration for \"Placement\" in Trait"}};
    }
    return 0;
}();
}
}
