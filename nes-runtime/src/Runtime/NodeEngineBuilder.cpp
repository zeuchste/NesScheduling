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

/// Splits a single memory ceiling into the pooled-pool buffer count and the unpooled budget. The unpooled share is
/// totalMemoryInBytes * unpooledFraction; the remainder sizes the pooled pool. By construction the two sum to the
/// total, so neither can independently exceed it. totalMemoryInBytes == 0 means auto-detect.
MemoryBudgets resolveMemoryBudgets(size_t totalMemoryInBytes, const uint32_t bufferSize, const double unpooledFraction)
{
    INVARIANT(unpooledFraction > 0.0 and unpooledFraction < 1.0, "unpooled_memory_fraction={} must be in (0, 1)", unpooledFraction);
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
    return {numberOfBuffers, unpooledLimitInBytes};
}
}

NodeEngineBuilder::NodeEngineBuilder(const WorkerConfiguration& workerConfiguration, std::shared_ptr<StatisticListener> statisticsListener)
    : workerConfiguration(workerConfiguration), statisticsListener(std::move(statisticsListener))
{
}

std::unique_ptr<NodeEngine> NodeEngineBuilder::build(const Host& host)
{
    const auto bufferSize = static_cast<uint32_t>(workerConfiguration.defaultQueryExecution.operatorBufferSize.getValue());
    const auto [numberOfBuffers, unpooledLimitInBytes] = resolveMemoryBudgets(
        workerConfiguration.totalMemoryInBytes.getValue(), bufferSize, workerConfiguration.unpooledMemoryFraction.getValue());
    auto bufferManager = BufferManager::create(
        bufferSize, numberOfBuffers, std::make_shared<NesDefaultMemoryAllocator>(), BufferManager::DEFAULT_ALIGNMENT, unpooledLimitInBytes);
    auto queryLog = std::make_shared<QueryLog>();

    auto queryEngine = std::make_unique<QueryEngine>(workerConfiguration.queryEngine, statisticsListener, queryLog, bufferManager, host);

    auto sourceProvider = std::make_unique<SourceProvider>(workerConfiguration.defaultMaxInflightBuffers.getValue(), bufferManager);

    return std::make_unique<NodeEngine>(
        std::move(bufferManager), statisticsListener, std::move(queryLog), std::move(queryEngine), std::move(sourceProvider));
}

}
