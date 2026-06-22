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

#pragma once

#include <memory>
#include <string>
#include <vector>
#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/BaseOption.hpp>
#include <Configurations/Enums/EnumOption.hpp>
#include <Configurations/ScalarOption.hpp>
#include <Configurations/Validation/FloatValidation.hpp>
#include <Configurations/Validation/NumberValidation.hpp>
#include <Configurations/Validation/PowerOfTwoValidation.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/DumpMode.hpp>
#include <fmt/format.h>
#include <QueryEngineConfiguration.hpp>
#include <QueryExecutionConfiguration.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <WorkerNetworkConfiguration.hpp>

namespace NES
{
class WorkerConfiguration final : public BaseConfiguration
{
public:
    WorkerConfiguration() = default;
    WorkerConfiguration(const std::string& name, const std::string& description) : BaseConfiguration(name, description) { };

    QueryEngineConfiguration queryEngine = {"query_engine", "Configuration for the query engine"};
    QueryExecutionConfiguration defaultQueryExecution = {"default_query_execution", "Default configuration for query executions"};
    QueryOptimizerConfiguration defaultQueryOptimization = {"default_query_optimization", "Default configuration for query optimizations"};
    WorkerNetworkConfiguration network = {"network", "Default configuration for network sources and sinks"};

    /// Total buffer memory budget for this worker, in bytes. The pooled pool and the unpooled budget are both derived
    /// from this single ceiling (see unpooledMemoryFraction), so they cannot independently exceed it. Set this to your
    /// buffer budget, i.e. the container limit minus headroom for runtime/network/stacks, not the raw cgroup limit.
    /// Set to 0 to auto-detect (the cgroup memory limit if running in a container, else physical RAM); note that
    /// auto-detect is unsafe when many workers share one host/container (e.g. the parallel test suite), as each would
    /// size its pools against the full machine and collectively overcommit. The fixed default reproduces the legacy
    /// 32768-buffer pooled pool: 32768 * DEFAULT_OPERATOR_BUFFER_SIZE / (1 - unpooledMemoryFraction) = 447392427.
    UIntOption totalMemoryInBytes
        = {"total_memory_in_bytes",
           "447392427",
           "Total worker buffer memory in bytes (0 = auto-detect: cgroup limit if containerized, else physical RAM).",
           {std::make_shared<NumberValidation>()}};

    /// Share of totalMemoryInBytes reserved for unpooled (variable-sized) operator state (hash maps, paged vectors,
    /// var-sized data); the remainder sizes the pooled pool. Must be in (0, 1). On breach of the unpooled share, the
    /// requesting query fails cleanly (BufferAllocationFailure) instead of the worker OOM-ing.
    FloatOption unpooledMemoryFraction
        = {"unpooled_memory_fraction",
           "0.7",
           "Fraction of total_memory_in_bytes reserved for unpooled operator state; the rest sizes the pooled pool (0..1).",
           {std::make_shared<FloatValidation>()}};

    /// Explicit pooled buffer count. 0 (default) = derive from total_memory_in_bytes / unpooled_memory_fraction (see
    /// resolveMemoryBudgets in NodeEngineBuilder); any value > 0 overrides the budget-derived count. Lets stress configs
    /// (e.g. a tiny pool for buffer-exhaustion tests) pin an exact number of pooled buffers.
    UIntOption numberOfBuffersInGlobalBufferManager
        = {"number_of_buffers_in_global_buffer_manager",
           "0",
           "Explicit pooled buffer count (0 = derive from total_memory_in_bytes).",
           {std::make_shared<NumberValidation>()}};

    /// Indicates how many buffers a single data source can allocate. This property controls the backpressure mechanism as a data source that can't allocate new records can't ingest more data.
    UIntOption defaultMaxInflightBuffers
        = {"default_max_inflight_buffers",
           "64",
           "Number of buffers a source can have inflight before blocking. May be overwritten by a source-specific configuration (see "
           "SourceDescriptor).",
           {std::make_shared<NumberValidation>()}};

    /// #1713: enables adaptive (AIMD) per-source inflight-buffer provisioning -- the inflight cap starts low and grows
    /// toward default_max_inflight_buffers under load, decaying when idle. Off by default (fixed cap).
    BoolOption enableAdaptiveInflightBuffers
        = {"enable_adaptive_inflight_buffers", "false", "Enable adaptive (AIMD) per-source inflight-buffer provisioning."};

    /// Enables disk-backed spilling of stateful operator (window/aggregation) slices when memory pressure is high, so
    /// queries can run with state larger than RAM. Off by default.
    BoolOption enableStateSpilling = {"enable_state_spilling", "false", "Enable disk-backed spilling of operator state."};

    /// Target ceiling (in bytes) for total resident operator-state across all slices before the governor starts
    /// evicting the coldest slices to disk. 0 means no proactive eviction (only relevant when spilling is enabled).
    UIntOption stateMemoryBudgetInBytes
        = {"state_memory_budget_in_bytes",
           "0",
           "Resident operator-state budget in bytes that triggers spilling (0 = unbounded).",
           {std::make_shared<NumberValidation>()}};

    /// Directory under which per-slice spill (arena backing) files are created.
    StringOption spillDirectory = {"spill_directory", "/tmp", "Directory for operator-state spill files."};

    EnumOption<DumpMode::Options> dumpQueryCompilationIR
        = {"dump_compilation_result",
           DumpMode::Options::NONE,
           fmt::format("If and where to dump query compilation results: {}", enumPipeList<DumpMode::Options>())};

    BoolOption dumpGraph = {"dump_graph", "false", "If to dump graph of the compilation results"};

    /// Enables variable-sized pooled buffers served from additional power-of-two size classes (alongside
    /// the default operator_buffer_size class). When disabled the global buffer manager behaves exactly as
    /// before: a single fixed-size pooled class plus the unpooled fallback.
    BoolOption enableBufferSizeClasses
        = {"enable_buffer_size_classes", "false", "Enable variable-sized pooled buffers via power-of-two size classes."};

    UIntOption bufferSizeClassMinBytes
        = {"buffer_size_class_min_bytes",
           "4096",
           "Smallest power-of-two size class in bytes (only used when enable_buffer_size_classes is true).",
           {std::make_shared<PowerOfTwoValidation>()}};

    UIntOption bufferSizeClassMaxBytes
        = {"buffer_size_class_max_bytes",
           "1048576",
           "Largest power-of-two size class in bytes (only used when enable_buffer_size_classes is true).",
           {std::make_shared<PowerOfTwoValidation>()}};

    EnumOption<BufferProvisioningPolicy> bufferSizeClassProvisioning
        = {"buffer_size_class_provisioning",
           BufferProvisioningPolicy::TotalBudgetSplit,
           fmt::format("How the size classes are provisioned: {}", enumPipeList<BufferProvisioningPolicy>())};

    /// TotalBudgetSplit: total bytes distributed across the size classes (0 -> derive from the default pool size).
    UIntOption bufferSizeClassBudgetBytes
        = {"buffer_size_class_budget_bytes",
           "0",
           "Total bytes distributed across size classes for the TotalBudgetSplit policy (0 = derive).",
           {std::make_shared<NumberValidation>()}};

    /// EagerPerClass: buffers preallocated per size class.
    UIntOption bufferSizeClassBuffersPerClass
        = {"buffer_size_class_buffers_per_class",
           "256",
           "Buffers preallocated per size class for the EagerPerClass policy.",
           {std::make_shared<NumberValidation>()}};

private:
    std::vector<BaseOption*> getOptions() override
    {
        return {
            &queryEngine,
            &defaultQueryExecution,
            &defaultQueryOptimization,
            &network,
            &totalMemoryInBytes,
            &unpooledMemoryFraction,
            &numberOfBuffersInGlobalBufferManager,
            &defaultMaxInflightBuffers,
            &enableAdaptiveInflightBuffers,
            &enableStateSpilling,
            &stateMemoryBudgetInBytes,
            &spillDirectory,
            &dumpQueryCompilationIR,
            &dumpGraph,
            &enableBufferSizeClasses,
            &bufferSizeClassMinBytes,
            &bufferSizeClassMaxBytes,
            &bufferSizeClassProvisioning,
            &bufferSizeClassBudgetBytes,
            &bufferSizeClassBuffersPerClass};
    }
};
}
