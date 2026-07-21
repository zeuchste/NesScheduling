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

#include <MemorySource.hpp>

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <format>
#include <fstream>
#include <ios>
#include <memory>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifier.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <ErrorHandling.hpp>
#include <FileDataRegistry.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

namespace NES
{

namespace
{

/// Parses one CSV-encoded value into its binary representation at `dst`, returning the number of bytes written.
/// Throws on parse error so the user sees a real diagnostic rather than silent corruption.
size_t writeFieldBinary(uint8_t* dst, std::string_view value, const DataType& dt)
{
    const auto* begin = value.data();
    const auto* end = value.data() + value.size();
    std::from_chars_result result{};
    switch (dt.type)
    {
        case DataType::Type::UINT8:
        {
            uint8_t v{};
            result = std::from_chars(begin, end, v);
            std::memcpy(dst, &v, sizeof(v));
            return sizeof(v);
        }
        case DataType::Type::UINT16:
        {
            uint16_t v{};
            result = std::from_chars(begin, end, v);
            std::memcpy(dst, &v, sizeof(v));
            return sizeof(v);
        }
        case DataType::Type::UINT32:
        {
            uint32_t v{};
            result = std::from_chars(begin, end, v);
            std::memcpy(dst, &v, sizeof(v));
            return sizeof(v);
        }
        case DataType::Type::UINT64:
        {
            uint64_t v{};
            result = std::from_chars(begin, end, v);
            std::memcpy(dst, &v, sizeof(v));
            return sizeof(v);
        }
        case DataType::Type::INT8:
        {
            int8_t v{};
            result = std::from_chars(begin, end, v);
            std::memcpy(dst, &v, sizeof(v));
            return sizeof(v);
        }
        case DataType::Type::INT16:
        {
            int16_t v{};
            result = std::from_chars(begin, end, v);
            std::memcpy(dst, &v, sizeof(v));
            return sizeof(v);
        }
        case DataType::Type::INT32:
        {
            int32_t v{};
            result = std::from_chars(begin, end, v);
            std::memcpy(dst, &v, sizeof(v));
            return sizeof(v);
        }
        case DataType::Type::INT64:
        {
            int64_t v{};
            result = std::from_chars(begin, end, v);
            std::memcpy(dst, &v, sizeof(v));
            return sizeof(v);
        }
        case DataType::Type::FLOAT32:
        {
            /// libc++ does not provide floating-point from_chars; strtof needs a NUL-terminated copy.
            const std::string tmp{value};
            char* parseEnd = nullptr;
            const float v = std::strtof(tmp.c_str(), &parseEnd);
            if (parseEnd == tmp.c_str())
            {
                throw InvalidConfigParameter("MemorySource failed to parse CSV value '{}' as FLOAT32", tmp);
            }
            std::memcpy(dst, &v, sizeof(v));
            return sizeof(v);
        }
        case DataType::Type::FLOAT64:
        {
            const std::string tmp{value};
            char* parseEnd = nullptr;
            const double v = std::strtod(tmp.c_str(), &parseEnd);
            if (parseEnd == tmp.c_str())
            {
                throw InvalidConfigParameter("MemorySource failed to parse CSV value '{}' as FLOAT64", tmp);
            }
            std::memcpy(dst, &v, sizeof(v));
            return sizeof(v);
        }
        case DataType::Type::BOOLEAN:
        case DataType::Type::CHAR:
        case DataType::Type::UNDEFINED:
        case DataType::Type::VARSIZED:
            throw InvalidConfigParameter("MemorySource does not yet support data type {} (VARSIZED handling lands in a follow-up commit)", static_cast<int>(dt.type));
    }
    if (result.ec != std::errc{})
    {
        throw InvalidConfigParameter("MemorySource failed to parse CSV value '{}' as data type {}", std::string{value}, static_cast<int>(dt.type));
    }
    return dt.getSizeInBytesWithoutNull();
}

}

MemorySource::MemorySource(const SourceDescriptor& sourceDescriptor)
    : filePath(sourceDescriptor.getFromConfig(ConfigParametersMemory::FILEPATH))
    , schema(sourceDescriptor.getLogicalSource().getSchema())
    , startRow(sourceDescriptor.getFromConfig(ConfigParametersMemory::START_ROW))
    , endRow(sourceDescriptor.getFromConfig(ConfigParametersMemory::END_ROW))
    , maxBytesPerBuffer(sourceDescriptor.getFromConfig(ConfigParametersMemory::MAX_BYTES_PER_BUFFER))
{
}

void MemorySource::open(std::shared_ptr<AbstractBufferProvider> bufProvider)
{
    bufferProvider = std::move(bufProvider);
    const auto t0 = std::chrono::steady_clock::now();
    preParseCsvIntoBinaryBuffers();
    const auto t1 = std::chrono::steady_clock::now();
    const auto setupMs = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
    std::fprintf(stderr, "[MemorySource] pre-parse took %lld ms for %zu tuples (%zu MB binary)\n",
                 static_cast<long long>(setupMs), totalTuples, tupleStorage.size() / (1024 * 1024));
    replayOffset = 0;
}

void MemorySource::close()
{
    tupleStorage.clear();
    tupleStorage.shrink_to_fit();
    totalTuples = 0;
    replayOffset = 0;
    bufferProvider.reset();
}

void MemorySource::preParseCsvIntoBinaryBuffers()
{
    INVARIANT(schema != nullptr, "MemorySource requires a valid schema");
    INVARIANT(bufferProvider != nullptr, "MemorySource::open must receive a buffer provider before parsing");

    /// Open the CSV file and read its entire contents into memory. For large inputs (5 GB+) this is the dominant setup cost; the steady-state benchmark loop touches only `storedBuffers`.
    const auto realPath = std::unique_ptr<char, decltype(std::free)*>{realpath(filePath.c_str(), nullptr), std::free};
    if (!realPath)
    {
        throw InvalidConfigParameter("Could not resolve absolute path for MemorySource file '{}': {}", filePath, std::strerror(errno));
    }
    std::ifstream file(realPath.get(), std::ios::binary | std::ios::ate);
    if (!file)
    {
        throw InvalidConfigParameter("Could not open MemorySource file '{}': {}", filePath, std::strerror(errno));
    }
    const auto fileSize = static_cast<size_t>(file.tellg());
    file.seekg(0, std::ios::beg);
    std::vector<char> csvBuf(fileSize);
    if (fileSize > 0)
    {
        file.read(csvBuf.data(), static_cast<std::streamsize>(fileSize));
    }

    const auto tupleWidth = schema->getSizeInBytes();
    std::vector<DataType> fieldTypes;
    for (const auto& field : *schema)
    {
        if (field.getDataType().nullable)
        {
            throw InvalidConfigParameter("MemorySource supports NOT NULL fields only (field '{}' is nullable)", field.getFullyQualifiedName());
        }
        fieldTypes.push_back(field.getDataType());
    }

    /// Approximate the number of tuples from the file size (assume rows average ~100 bytes of CSV text → ~78 bytes binary; we just want a starting capacity to avoid early reallocations).
    tupleStorage.reserve((fileSize / 100) * tupleWidth);
    totalTuples = 0;

    const std::string_view csv{csvBuf.data(), fileSize};
    size_t lineStart = 0;
    /// 0-based row index of the next non-empty line we'll scan. Used to decide whether to skip (rowIdx < startRow)
    /// or stop (endRow > 0 && rowIdx >= endRow). Empty lines do not advance the counter, matching CSV-row semantics.
    uint64_t rowIdx = 0;
    /// Single scratch slot reused for each tuple to avoid per-tuple allocations.
    std::vector<uint8_t> tupleSlot(tupleWidth);
    while (lineStart < csv.size())
    {
        const auto lineEnd = csv.find('\n', lineStart);
        const auto line = csv.substr(lineStart, (lineEnd == std::string_view::npos ? csv.size() : lineEnd) - lineStart);

        if (!line.empty())
        {
            if (endRow != 0 && rowIdx >= endRow)
            {
                break;
            }
            if (rowIdx < startRow)
            {
                ++rowIdx;
                if (lineEnd == std::string_view::npos) { break; }
                lineStart = lineEnd + 1;
                continue;
            }

            size_t fieldOffset = 0;
            size_t fieldStart = 0;
            size_t fieldIdx = 0;
            for (size_t i = 0; i <= line.size(); ++i)
            {
                const bool atDelim = (i == line.size()) || (line[i] == ',');
                if (atDelim)
                {
                    if (fieldIdx >= fieldTypes.size())
                    {
                        throw InvalidConfigParameter("MemorySource: CSV row has more fields than the schema ({} fields)", fieldTypes.size());
                    }
                    const auto fieldView = line.substr(fieldStart, i - fieldStart);
                    fieldOffset += writeFieldBinary(tupleSlot.data() + fieldOffset, fieldView, fieldTypes.at(fieldIdx));
                    ++fieldIdx;
                    fieldStart = i + 1;
                }
            }
            if (fieldIdx != fieldTypes.size())
            {
                throw InvalidConfigParameter("MemorySource: CSV row has {} fields, schema has {}", fieldIdx, fieldTypes.size());
            }
            tupleStorage.insert(tupleStorage.end(), tupleSlot.begin(), tupleSlot.end());
            ++totalTuples;
            ++rowIdx;
        }

        if (lineEnd == std::string_view::npos)
        {
            break;
        }
        lineStart = lineEnd + 1;
    }
}

Source::FillTupleBufferResult MemorySource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token&)
{
    if (replayOffset == 0)
    {
        steadyStateStart = std::chrono::steady_clock::now();
        std::fprintf(stderr, "[MemorySource] first fillTupleBuffer call (steady-state start)\n");
    }
    if (replayOffset >= tupleStorage.size())
    {
        const auto t1 = std::chrono::steady_clock::now();
        const auto steadyMs = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - steadyStateStart).count();
        std::fprintf(stderr, "[MemorySource] EoS: steady-state %lld ms, %zu tuples = %.2f MTup/s\n",
                     static_cast<long long>(steadyMs), totalTuples,
                     (steadyMs > 0) ? (double(totalTuples) / 1e3 / double(steadyMs)) : 0.0);
        return FillTupleBufferResult::eos();
    }
    const auto tupleWidth = schema->getSizeInBytes();
    const auto bufferCapacity = std::min<size_t>(tupleBuffer.getBufferSize(), maxBytesPerBuffer);
    const auto tuplesPerOutput = bufferCapacity / tupleWidth;
    const auto remainingBytes = tupleStorage.size() - replayOffset;
    const auto bytesToCopy = std::min(remainingBytes, tuplesPerOutput * tupleWidth);
    const auto tuplesThisCall = bytesToCopy / tupleWidth;
    std::memcpy(tupleBuffer.getAvailableMemoryArea<uint8_t>().data(), tupleStorage.data() + replayOffset, bytesToCopy);
    tupleBuffer.setNumberOfTuples(tuplesThisCall);
    replayOffset += bytesToCopy;
    totalNumBytesRead += bytesToCopy;
    /// The SourceThread overwrites numberOfTuples with this result value (raw text sources carry a BYTE count
    /// there for the input formatter to consume). This source emits native buffers with no formatter stage, so
    /// the value must be the TUPLE count that the native scan reads directly.
    return FillTupleBufferResult::withBytes(tuplesThisCall);
}

DescriptorConfig::Config MemorySource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersMemory>(std::move(config), NAME);
}

std::ostream& MemorySource::toString(std::ostream& str) const
{
    str << std::format("MemorySource(filePath: {}, totalTuples: {}, totalNumBytesRead: {})", filePath, totalTuples, totalNumBytesRead.load());
    return str;
}

SourceValidationRegistryReturnType RegisterMemorySourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return MemorySource::validateAndFormat(std::move(sourceConfig.config));
}

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterMemorySource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<MemorySource>(sourceRegistryArguments.sourceDescriptor);
}

FileDataRegistryReturnType FileDataGeneratedRegistrar::RegisterMemoryFileData(FileDataRegistryArguments systestAdaptorArguments)
{
    static const auto FILE_PATH_PARAMETER = Identifier::parse("FILE_PATH");
    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(FILE_PATH_PARAMETER))
    {
        throw InvalidConfigParameter("MemorySource cannot accept an inline FILE_PATH (it is set by the systest framework)");
    }
    systestAdaptorArguments.physicalSourceConfig.sourceConfig.emplace(FILE_PATH_PARAMETER, systestAdaptorArguments.testFilePath.string());
    return systestAdaptorArguments.physicalSourceConfig;
}

}
