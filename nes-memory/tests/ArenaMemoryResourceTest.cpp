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

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <memory>
#include <numeric>
#include <span>
#include <string>
#include <vector>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Spill/ArenaMemoryResource.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <gtest/gtest.h>

namespace NES
{

/// Tests for ArenaMemoryResource, the per-slice spill unit. The core guarantees are:
/// (1) after an evict/reload cycle the data is intact at the *same* virtual address (so live pointers survive), and
/// (2) live-byte accounting reflects allocations/deallocations. Both the Anon (vmcache-faithful) and FileBacked modes
/// are exercised.
class ArenaMemoryResourceTest : public ::testing::TestWithParam<ArenaMemoryResource::Mode>
{
protected:
    std::filesystem::path backingFile;

    void SetUp() override
    {
        /// Unique backing file per test so parallel test execution does not collide. The parameterized suite/test
        /// names contain '/' (e.g. "Modes/ArenaMemoryResourceTest"), so sanitize them into a flat file name.
        const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
        std::string name = std::string("nes-arena-") + info->test_suite_name() + "-" + info->name() + ".spill";
        std::ranges::replace(name, '/', '_');
        backingFile = std::filesystem::temp_directory_path() / name;
    }

    void TearDown() override
    {
        std::error_code ec;
        std::filesystem::remove(backingFile, ec);
    }
};

INSTANTIATE_TEST_SUITE_P(
    Modes,
    ArenaMemoryResourceTest,
    ::testing::Values(ArenaMemoryResource::Mode::Anon, ArenaMemoryResource::Mode::FileBacked),
    [](const auto& info) { return info.param == ArenaMemoryResource::Mode::Anon ? "Anon" : "FileBacked"; });

/// Data written into an allocation survives an evict/reload cycle and stays at the same address.
TEST_P(ArenaMemoryResourceTest, DataAndAddressSurviveEvictReload)
{
    ArenaMemoryResource arena(GetParam(), backingFile.string());

    constexpr size_t numBytes = 64 * 1024;
    auto* region = static_cast<uint8_t*>(arena.allocate(numBytes, 64));
    ASSERT_NE(region, nullptr);

    /// Write a deterministic pattern.
    for (size_t i = 0; i < numBytes; ++i)
    {
        region[i] = static_cast<uint8_t>((i * 31 + 7) & 0xFF);
    }

    EXPECT_FALSE(arena.isEvicted());
    arena.evict();
    EXPECT_TRUE(arena.isEvicted());
    arena.reload();
    EXPECT_FALSE(arena.isEvicted());

    /// Same virtual address, identical contents.
    for (size_t i = 0; i < numBytes; ++i)
    {
        ASSERT_EQ(region[i], static_cast<uint8_t>((i * 31 + 7) & 0xFF)) << "mismatch at byte " << i;
    }
    EXPECT_GT(arena.bytesWritten(), 0u);
    EXPECT_GT(arena.bytesRead(), 0u);

    arena.deallocate(region, numBytes, 64);
}

/// Multiple allocations all survive a single evict/reload, each at its own stable address.
TEST_P(ArenaMemoryResourceTest, MultipleRegionsSurvive)
{
    ArenaMemoryResource arena(GetParam(), backingFile.string());
    std::vector<uint8_t*> regions;
    constexpr size_t numRegions = 5;
    constexpr size_t regionBytes = 8 * 1024;
    for (size_t r = 0; r < numRegions; ++r)
    {
        auto* p = static_cast<uint8_t*>(arena.allocate(regionBytes, 64));
        std::memset(p, static_cast<int>(r + 1), regionBytes);
        regions.push_back(p);
    }

    arena.evict();
    arena.reload();

    for (size_t r = 0; r < numRegions; ++r)
    {
        for (size_t i = 0; i < regionBytes; ++i)
        {
            ASSERT_EQ(regions[r][i], static_cast<uint8_t>(r + 1)) << "region " << r << " byte " << i;
        }
    }
    for (auto* p : regions)
    {
        arena.deallocate(p, regionBytes, 64);
    }
}

/// Live-byte accounting tracks allocations and deallocations (page-rounded).
TEST_P(ArenaMemoryResourceTest, LiveByteAccounting)
{
    ArenaMemoryResource arena(GetParam(), backingFile.string());
    EXPECT_EQ(arena.liveBytes(), 0u);

    auto* a = arena.allocate(1000, 64); /// rounds up to one page
    const auto afterA = arena.liveBytes();
    EXPECT_GE(afterA, 1000u);

    auto* b = arena.allocate(9000, 64);
    EXPECT_GT(arena.liveBytes(), afterA);

    arena.deallocate(a, 1000, 64);
    arena.deallocate(b, 9000, 64);
    EXPECT_EQ(arena.liveBytes(), 0u);
}

/// evict()/reload() are idempotent: redundant calls are no-ops and do not corrupt state.
TEST_P(ArenaMemoryResourceTest, EvictReloadIdempotent)
{
    ArenaMemoryResource arena(GetParam(), backingFile.string());
    auto* p = static_cast<uint8_t*>(arena.allocate(4096, 64));
    std::iota(p, p + 256, uint8_t{0});

    arena.evict();
    arena.evict(); /// no-op
    arena.reload();
    arena.reload(); /// no-op

    for (int i = 0; i < 256; ++i)
    {
        ASSERT_EQ(p[i], static_cast<uint8_t>(i));
    }
    arena.deallocate(p, 4096, 64);
}

/// A BufferManager backed by the arena keeps its buffers' contents and addresses across an evict/reload, proving the
/// arena works as the per-slice allocation backend (the real integration path for spilling operator state).
TEST_P(ArenaMemoryResourceTest, BufferManagerBackedByArenaSurvivesEvictReload)
{
    auto arena = std::make_shared<ArenaMemoryResource>(GetParam(), backingFile.string());
    constexpr uint32_t bufferSize = 4096;
    constexpr uint32_t numBuffers = 4;
    auto bm = BufferManager::create(bufferSize, numBuffers, arena, 64);

    auto buffer = bm->getBufferBlocking();
    auto* raw = buffer.getAvailableMemoryArea<uint8_t>().data();
    auto mem = buffer.getAvailableMemoryArea<uint8_t>();
    for (size_t i = 0; i < mem.size(); ++i)
    {
        mem[i] = static_cast<uint8_t>((i * 13 + 5) & 0xFF);
    }

    arena->evict();
    arena->reload();

    /// Same backing address; contents intact.
    EXPECT_EQ(buffer.getAvailableMemoryArea<uint8_t>().data(), raw);
    auto memAfter = buffer.getAvailableMemoryArea<uint8_t>();
    for (size_t i = 0; i < memAfter.size(); ++i)
    {
        ASSERT_EQ(memAfter[i], static_cast<uint8_t>((i * 13 + 5) & 0xFF)) << "byte " << i;
    }

    /// Release before destroying the BufferManager (its destructor asserts no outstanding buffers).
    buffer = TupleBuffer();
    bm->destroy();
}

}
