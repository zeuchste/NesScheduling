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

#include <Runtime/NodeEngineBuilder.hpp>

#include <algorithm>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <utility>
#include <unistd.h>
#include <Configuration/WorkerConfiguration.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Sources/SourceProvider.hpp>
#include <ErrorHandling.hpp>
#include <QueryEngine.hpp>

namespace NES
{

namespace
{
/// Reads a single non-negative integer from a cgroup memory file. Returns nullopt when the file is absent (not
/// containerized) or holds the v2 sentinel "max"/an unparseable value, i.e. "no concrete limit".
std::optional<size_t> readMemoryLimitFromFile(const char* path)
{
    std::ifstream file{path};
    if (not file.is_open())
    {
        return std::nullopt;
    }
    std::string content;
    std::getline(file, content);
    size_t value{};
    const auto* const begin = content.data();
    const auto* const end = begin + content.size();
    if (const auto [ptr, ec] = std::from_chars(begin, end, value); ec == std::errc{} and ptr == end)
    {
        return value;
    }
    return std::nullopt;
}

/// Best-effort detection of the memory the OOM-killer will actually enforce: the cgroup limit when containerized
/// (v2 first, then v1), otherwise physical RAM. A cgroup limit is only honoured when it is below physical RAM, since
/// an unset limit is reported as a huge sentinel rather than "max" on some kernels.
/// WARNING: in a Docker (or otherwise containerized) deployment sysconf(_SC_PHYS_PAGES) reports the *host* physical
/// memory, not the container's limit. If the cgroup files are unreadable/absent we therefore fall back to the host
/// size and may size the pools far larger than the container allows -- which then OOM-kills the worker. If you are
/// chasing a phantom OOM in a container, set total_memory_in_bytes explicitly rather than trusting this auto-detection.
size_t detectAvailableMemoryBytes()
{
    const auto physicalMemoryInBytes = static_cast<size_t>(sysconf(_SC_PHYS_PAGES)) * static_cast<size_t>(sysconf(_SC_PAGE_SIZE));
    for (const auto* const path : {"/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"})
    {
        if (const auto limit = readMemoryLimitFromFile(path); limit.has_value() and limit.value() < physicalMemoryInBytes)
        {
            return limit.value();
        }
    }
    return physicalMemoryInBytes;
}

struct MemoryBudgets
{
    uint32_t numberOfBuffers;
    size_t unpooledLimitInBytes;
};

/// Resolve the pooled/unpooled memory budgets from the three user-facing knobs: total memory, the unpooled fraction,
/// and the fixed buffer size. The unpooled share is totalMemoryInBytes * unpooledFraction; the remainder sizes the
/// pooled pool. By construction the two sum to the total, so neither can independently exceed it. totalMemoryInBytes
/// == 0 means auto-detect (see detectAvailableMemoryBytes). The fraction is clamped into [0, 1] rather than asserted,
/// so a misconfigured value degrades gracefully instead of crashing the worker.
MemoryBudgets resolveMemoryBudgets(size_t totalMemoryInBytes, const uint32_t bufferSize, double unpooledFraction)
{
    unpooledFraction = std::clamp(unpooledFraction, 0.0, 1.0);
    if (totalMemoryInBytes == 0)
    {
        totalMemoryInBytes = detectAvailableMemoryBytes();
    }
    const auto unpooledLimitInBytes = static_cast<size_t>(static_cast<double>(totalMemoryInBytes) * unpooledFraction);
    const auto numberOfBuffers = static_cast<uint32_t>((totalMemoryInBytes - unpooledLimitInBytes) / bufferSize);
    INVARIANT(
        numberOfBuffers > 0,
        "total_memory_in_bytes={} with unpooled_memory_fraction={} leaves no room for a single {}B pooled buffer",
        totalMemoryInBytes,
        unpooledFraction,
        bufferSize);
    return MemoryBudgets{.numberOfBuffers = numberOfBuffers, .unpooledLimitInBytes = unpooledLimitInBytes};
}
}

NodeEngineBuilder::NodeEngineBuilder(const WorkerConfiguration& workerConfiguration, std::shared_ptr<StatisticListener> statisticsListener)
    : workerConfiguration(workerConfiguration), statisticsListener(std::move(statisticsListener))
{
}

std::optional<SizeClassConfig> NodeEngineBuilder::makeSizeClassConfig(const WorkerConfiguration& workerConfiguration)
{
    if (!workerConfiguration.enableBufferSizeClasses.getValue())
    {
        return std::nullopt;
    }
    const auto minClassSize = workerConfiguration.bufferSizeClassMinBytes.getValue();
    const auto maxClassSize = workerConfiguration.bufferSizeClassMaxBytes.getValue();
    if (minClassSize > maxClassSize)
    {
        throw InvalidConfigParameter(
            "buffer_size_class_min_bytes ({}) must be <= buffer_size_class_max_bytes ({})", minClassSize, maxClassSize);
    }
    SizeClassConfig config{.minClassSize = minClassSize, .maxClassSize = maxClassSize};
    config.policy = workerConfiguration.bufferSizeClassProvisioning.getValue();
    config.totalBudgetBytes = workerConfiguration.bufferSizeClassBudgetBytes.getValue();
    config.buffersPerClass = workerConfiguration.bufferSizeClassBuffersPerClass.getValue();
    return config;
}

std::unique_ptr<NodeEngine> NodeEngineBuilder::build(const Host& host)
{
    const auto bufferSize = static_cast<uint32_t>(workerConfiguration.defaultQueryExecution.operatorBufferSize.getValue());
    const auto [numberOfBuffers, unpooledLimitInBytes] = resolveMemoryBudgets(
        workerConfiguration.totalMemoryInBytes.getValue(), bufferSize, workerConfiguration.unpooledMemoryFraction.getValue());
    /// Hybrid pooled-count model: an explicit number_of_buffers_in_global_buffer_manager (> 0) wins so stress
    /// configs (e.g. tiny-pool.yaml for buffer-exhaustion tests) can pin an exact count; otherwise use the
    /// total-memory-budget-derived count from resolveMemoryBudgets.
    const auto explicitBufferCount = static_cast<uint32_t>(workerConfiguration.numberOfBuffersInGlobalBufferManager.getValue());
    auto bufferManager = BufferManager::create(
        bufferSize,
        explicitBufferCount > 0 ? explicitBufferCount : numberOfBuffers,
        std::make_shared<NesDefaultMemoryAllocator>(),
        BufferManager::DEFAULT_ALIGNMENT,
        makeSizeClassConfig(workerConfiguration),
        unpooledLimitInBytes);
    auto queryLog = std::make_shared<QueryLog>();

    auto queryEngine = std::make_unique<QueryEngine>(workerConfiguration.queryEngine, statisticsListener, queryLog, bufferManager, host);

    auto sourceProvider = std::make_unique<SourceProvider>(workerConfiguration.defaultMaxInflightBuffers.getValue(), bufferManager);

    return std::make_unique<NodeEngine>(
        std::move(bufferManager), statisticsListener, std::move(queryLog), std::move(queryEngine), std::move(sourceProvider));
}

}
