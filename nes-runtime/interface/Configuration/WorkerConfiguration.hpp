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
    /// from this single ceiling (see unpooledMemoryFraction), so they cannot independently exceed it. 0 = auto-detect
    /// (the cgroup memory limit if running in a container, else physical RAM). Set this to your buffer budget, i.e. the
    /// container limit minus headroom for runtime/network/stacks, not the raw cgroup limit.
    UIntOption totalMemoryInBytes
        = {"total_memory_in_bytes",
           "0",
           "Total worker buffer memory in bytes (0 = auto: cgroup limit if containerized, else physical RAM).",
           {std::make_shared<NumberValidation>()}};

    /// Share of totalMemoryInBytes reserved for unpooled (variable-sized) operator state (hash maps, paged vectors,
    /// var-sized data); the remainder sizes the pooled pool. Must be in (0, 1). On breach of the unpooled share, the
    /// requesting query fails cleanly (BufferAllocationFailure) instead of the worker OOM-ing.
    FloatOption unpooledMemoryFraction
        = {"unpooled_memory_fraction",
           "0.7",
           "Fraction of total_memory_in_bytes reserved for unpooled operator state; the rest sizes the pooled pool (0..1).",
           {std::make_shared<FloatValidation>()}};

    /// Indicates how many buffers a single data source can allocate. This property controls the backpressure mechanism as a data source that can't allocate new records can't ingest more data.
    UIntOption defaultMaxInflightBuffers
        = {"default_max_inflight_buffers",
           "64",
           "Number of buffers a source can have inflight before blocking. May be overwritten by a source-specific configuration (see "
           "SourceDescriptor).",
           {std::make_shared<NumberValidation>()}};

    EnumOption<DumpMode::Options> dumpQueryCompilationIR
        = {"dump_compilation_result",
           DumpMode::Options::NONE,
           fmt::format("If and where to dump query compilation results: {}", enumPipeList<DumpMode::Options>())};

    BoolOption dumpGraph = {"dump_graph", "false", "If to dump graph of the compilation results"};

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
            &defaultMaxInflightBuffers,
            &dumpQueryCompilationIR,
            &dumpGraph};
    }
};
}
