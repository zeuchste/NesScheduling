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

#include <cstdint>

namespace NES
{

/// The three storage variants (S1-S3) of the stream-join design space: how build-side tuples
/// are laid out in the engine's tuple buffers.
enum class JoinStorageVariant : uint8_t
{
    /// S2 (default, current main): chained hash map whose entries hold a per-key PagedVector;
    /// scanning the matches of one key is sequential within its pages.
    PER_KEY_PAGED,
    /// S1: one entry per tuple, values inline in the shared entry pages; maximal buffer filling,
    /// chain traversal touches many pages.
    SHARED_CHAINS,
    /// S3: fixed-size bucket array sized from an estimated key cardinality (join_fixed_buckets),
    /// with the existing chains as the hybrid overflow backup; no adaptive resizing.
    FIXED_ARRAY
};

/// The four processing variants (P1-P4): how the probe work of a window is mapped onto worker threads.
enum class JoinProcessingVariant : uint8_t
{
    /// P1 (default, current main): one probe task per window carrying all left x right hash maps.
    SINGLE_TASK,
    /// P2: one probe task per (left map, right map) pair; the probe of one window scales with the cores.
    TASK_PER_PAIR,
    /// P3: all threads build one shared hash table per window side; probe is a single task over the pair.
    SHARED_TABLE,
    /// P4: one probe task per left map, each carrying the full opposite side (replication-based broadcast).
    BROADCAST
};

/// The two trigger variants (T1/T2): when join work happens.
enum class JoinTriggerVariant : uint8_t
{
    /// T1 (default): buffer arriving tuples, probe in bulk once the window closes.
    LAZY,
    /// T2: probe the opposite side on every tuple arrival (not implemented yet; scoped as follow-up).
    EAGER
};

}
