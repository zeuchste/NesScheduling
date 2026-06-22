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

#include <Runtime/MemoryUtils.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

TupleBuffer getBuffer(const uint64_t size, AbstractBufferProvider& provider)
{
    /// Route through the provider's size-aware allocation: a fitting pooled size class when one
    /// exists, otherwise the unpooled fallback. The returned buffer is guaranteed to be >= size.
    return provider.getBuffer(size);
}

TupleBuffer deepCopyBuffer(const TupleBuffer& buffer, AbstractBufferProvider& provider)
{
    /// TODO #1582: you may need different size for the copy buffer, not always the default
    auto copiedBuffer = getBuffer(buffer.getBufferSize(), provider);
    PRECONDITION(
        copiedBuffer.getBufferSize() >= buffer.getBufferSize(),
        "Attempt to copy buffer of size: {} into smaller buffer of size: {}",
        copiedBuffer.getBufferSize(),
        buffer.getBufferSize());
    auto bufferData = buffer.getAvailableMemoryArea<std::byte>();
    std::ranges::copy(bufferData, copiedBuffer.getAvailableMemoryArea().begin());
    copiedBuffer.setWatermark(buffer.getWatermark());
    copiedBuffer.setChunkNumber(buffer.getChunkNumber());
    copiedBuffer.setSequenceNumber(buffer.getSequenceNumber());
    copiedBuffer.setCreationTimestampInMS(buffer.getCreationTimestampInMS());
    copiedBuffer.setLastChunk(buffer.isLastChunk());
    copiedBuffer.setOriginId(buffer.getOriginId());
    copiedBuffer.setNumberOfTuples(buffer.getNumberOfTuples());

    for (size_t childIdx = 0; childIdx < buffer.getNumberOfChildBuffers(); ++childIdx)
    {
        const VariableSizedAccess::Index varSizedIndex{childIdx};
        auto childBuffer = buffer.loadChildBuffer(varSizedIndex);
        auto copiedChildBuffer = deepCopyBuffer(childBuffer, provider);
        auto ret = copiedBuffer.storeChildBuffer(copiedChildBuffer);
        INVARIANT(ret == varSizedIndex, "Child buffer index: {}, does not match index: {}", childIdx, ret);
    }

    return copiedBuffer;
}

void copyUsedRecordsInto(TupleBuffer& target, const TupleBuffer& staging, const uint64_t usedBytes, AbstractBufferProvider& provider)
{
    PRECONDITION(target.getBufferSize() >= usedBytes, "right-sized target too small: {} < {}", target.getBufferSize(), usedBytes);
    PRECONDITION(staging.getBufferSize() >= usedBytes, "staging buffer smaller than used bytes: {} < {}", staging.getBufferSize(), usedBytes);
    /// Copy the written fixed part by raw bytes -- getAvailableMemoryArea() spans the whole buffer, but we only copy the
    /// `usedBytes` prefix (the staging buffer's numberOfTuples is not set yet, so we must not derive the length from it).
    if (usedBytes > 0)
    {
        std::memcpy(
            target.getAvailableMemoryArea<std::byte>().data(), staging.getAvailableMemoryArea<std::byte>().data(), usedBytes);
    }
    /// Re-attach var-sized child buffers in the same order so the fixed part's child indices stay valid.
    for (size_t childIdx = 0; childIdx < staging.getNumberOfChildBuffers(); ++childIdx)
    {
        const VariableSizedAccess::Index varSizedIndex{childIdx};
        auto childBuffer = staging.loadChildBuffer(varSizedIndex);
        auto copiedChildBuffer = deepCopyBuffer(childBuffer, provider);
        auto ret = target.storeChildBuffer(copiedChildBuffer);
        INVARIANT(ret == varSizedIndex, "Child buffer index: {}, does not match index: {}", childIdx, ret);
    }
}
}
