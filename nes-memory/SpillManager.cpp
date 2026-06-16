/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <Runtime/Spill/SpillManager.hpp>

#include <algorithm>
#include <cstddef>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <utility>
#include <vector>

#include <ErrorHandling.hpp>

namespace NES
{

SpillManager::SpillManager(SpillConfiguration configuration) : config(configuration)
{
    PRECONDITION(
        config.lowWatermark <= config.highWatermark,
        "SpillManager lowWatermark ({}) must not exceed highWatermark ({})",
        config.lowWatermark,
        config.highWatermark);
}

void SpillManager::registerState(const std::shared_ptr<SpillableState>& state)
{
    PRECONDITION(state != nullptr, "Cannot register a null SpillableState");
    const std::unique_lock lock(registryMutex);
    auto entry = std::make_shared<Entry>();
    entry->state = state;
    registry[state.get()] = std::move(entry);
}

void SpillManager::unregisterState(const SpillableState* state)
{
    const std::unique_lock lock(registryMutex);
    registry.erase(state);
}

std::shared_ptr<SpillManager::Entry> SpillManager::lookup(const SpillableState* state) const
{
    const std::shared_lock lock(registryMutex);
    if (const auto it = registry.find(state); it != registry.end())
    {
        return it->second;
    }
    return nullptr;
}

void SpillManager::pin(SpillableState& state)
{
    const auto entry = lookup(&state);
    PRECONDITION(entry != nullptr, "pin() called on an unregistered SpillableState");
    const std::lock_guard guard(entry->residencyMutex);
    if (state.isEvicted())
    {
        state.reloadState();
    }
    entry->pinCount.fetch_add(1);
}

void SpillManager::unpin(SpillableState& state)
{
    const auto entry = lookup(&state);
    PRECONDITION(entry != nullptr, "unpin() called on an unregistered SpillableState");
    USED_IN_DEBUG const auto previous = entry->pinCount.fetch_sub(1);
    INVARIANT(previous > 0, "unpin() called more times than pin() for a SpillableState");
}

size_t SpillManager::residentBytes() const
{
    const std::shared_lock lock(registryMutex);
    size_t total = 0;
    for (const auto& [ptr, entry] : registry)
    {
        total += entry->state->residentBytes();
    }
    return total;
}

size_t SpillManager::registeredCount() const
{
    const std::shared_lock lock(registryMutex);
    return registry.size();
}

size_t SpillManager::maybeSpill()
{
    if (!config.enabled || config.stateMemoryBudgetBytes == 0)
    {
        return 0;
    }
    const auto highBytes = static_cast<size_t>(static_cast<double>(config.stateMemoryBudgetBytes) * config.highWatermark);
    if (residentBytes() <= highBytes)
    {
        return 0;
    }
    const auto lowBytes = static_cast<size_t>(static_cast<double>(config.stateMemoryBudgetBytes) * config.lowWatermark);
    return evictDownTo(lowBytes);
}

size_t SpillManager::evictDownTo(size_t targetBytes)
{
    /// Snapshot the registry under a brief read-lock, then do the actual evict I/O outside it (under each unit's own
    /// residency mutex) so disk I/O never serializes the whole registry.
    std::vector<std::shared_ptr<Entry>> candidates;
    size_t resident = 0;
    {
        const std::shared_lock lock(registryMutex);
        candidates.reserve(registry.size());
        for (const auto& [ptr, entry] : registry)
        {
            resident += entry->state->residentBytes();
            candidates.push_back(entry);
        }
    }
    if (resident <= targetBytes)
    {
        return 0;
    }

    /// Coldest first (smallest coldnessKey is evicted first).
    std::ranges::sort(candidates, [](const auto& lhs, const auto& rhs) { return lhs->state->coldnessKey() < rhs->state->coldnessKey(); });

    size_t freed = 0;
    for (const auto& entry : candidates)
    {
        if (resident - freed <= targetBytes)
        {
            break;
        }
        const std::lock_guard guard(entry->residencyMutex);
        /// Re-check under the residency mutex: skip pinned or already-evicted units.
        if (entry->pinCount.load() != 0 || entry->state->isEvicted())
        {
            continue;
        }
        const auto bytes = entry->state->residentBytes();
        entry->state->evictState();
        freed += bytes;
    }
    return freed;
}

}
