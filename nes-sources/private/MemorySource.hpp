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

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>

namespace NES
{

/// MemorySource pre-parses a CSV file once during `open()` into engine-native binary tuple buffers, then replays those buffers on every `fillTupleBuffer` call.
/// Paired with a source descriptor whose parser type is "Native" so that PipeliningPhase (and LowerToPhysicalProjection) elide the input-formatter stage and the engine consumes the binary buffers directly.
/// Used as the input side of the benchmark fast path: combined with the existing NATIVE-input elide and the DiscardEmit output-side fast path, this eliminates CSV parsing entirely from the steady-state measurement window.
/// Supports fixed-width numeric schemas in this commit; VARSIZED fields will be added via the padding pre-scan in a follow-up.
class MemorySource final : public Source
{
public:
    static constexpr std::string_view NAME = "Memory";

    explicit MemorySource(const SourceDescriptor& sourceDescriptor);
    ~MemorySource() override = default;

    MemorySource(const MemorySource&) = delete;
    MemorySource& operator=(const MemorySource&) = delete;
    MemorySource(MemorySource&&) = delete;
    MemorySource& operator=(MemorySource&&) = delete;

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;

    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    void close() override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    std::string filePath;
    std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>> schema;
    std::shared_ptr<AbstractBufferProvider> bufferProvider;
    uint64_t startRow{0};
    uint64_t endRow{0};
    uint64_t maxBytesPerBuffer{4096};

    /// Pre-parsed binary tuples, laid out contiguously and replayed sequentially by `fillTupleBuffer`.
    /// Using raw storage (`std::vector<uint8_t>`) rather than `std::vector<TupleBuffer>` because the engine's buffer pool would exhaust at 50M tuples (~500k buffers vs pool size in the thousands).
    std::vector<uint8_t> tupleStorage;
    size_t totalTuples = 0;
    size_t replayOffset = 0;
    std::atomic<size_t> totalNumBytesRead{0};
    std::chrono::steady_clock::time_point steadyStateStart{};

    /// Parses the entire file into `storedBuffers`. Called from `open()`.
    void preParseCsvIntoBinaryBuffers();
};

struct ConfigParametersMemory
{
    static inline const DescriptorConfig::ConfigParameter<std::string> FILEPATH{
        "FILE_PATH",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FILEPATH, config); }};

    /// Inclusive start row (0-based) within the attached CSV file. Default 0 = read from the beginning.
    /// Paired with END_ROW to let multiple physical sources each pre-parse a disjoint slice of the same file
    /// (used by the source-side ceiling experiments to measure the multi-producer admission-queue throughput).
    static inline const DescriptorConfig::ConfigParameter<uint64_t> START_ROW{
        "START_ROW",
        uint64_t{0},
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(START_ROW, config); }};

    /// Exclusive end row (0-based). Default 0 = read until EOF (no upper bound).
    static inline const DescriptorConfig::ConfigParameter<uint64_t> END_ROW{
        "END_ROW",
        uint64_t{0},
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(END_ROW, config); }};

    /// Upper bound on the payload bytes written per emitted buffer. The source pool may hand out larger
    /// buffers than the global pool that network receivers rebuild into; filling beyond the global buffer
    /// size would fail on the receive side. ponytail: default matches the engine's default buffer size.
    static inline const DescriptorConfig::ConfigParameter<uint64_t> MAX_BYTES_PER_BUFFER{
        "MAX_BYTES_PER_BUFFER",
        uint64_t{4096},
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(MAX_BYTES_PER_BUFFER, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SourceDescriptor::parameterMap, FILEPATH, START_ROW, END_ROW, MAX_BYTES_PER_BUFFER);
};

}
