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

/// The building knob (B1/B2): how many build tables exist per window side, i.e., who writes the build state.
enum class JoinBuildVariant : uint8_t
{
    /// B1 (default, current main): one table per worker thread and side; unsynchronized inserts.
    LOCAL_TABLES,
    /// B2: one table per side shared by all worker threads; synchronized inserts.
    SHARED_TABLE
};

/// The probing knob (P1-P4): how the immutable window state is cut into probe tasks, ordered by granularity.
enum class JoinProbeVariant : uint8_t
{
    /// P1 (default, current main): one probe task per window carrying all left x right tables.
    SINGLE_TASK,
    /// P2: one probe task per left table, each carrying the full right side (replication-based broadcast).
    TABLE_BROADCAST,
    /// P3: one probe task per (left table, right table) pair.
    TASK_PER_PAIR,
    /// P4: one probe task per (pair, bucket-page range of the right table) - the finest granularity; the
    /// only level that still parallelizes the probe under the SHARED_TABLE build, where the pair space
    /// collapses to 1x1. Range count: join_probe_ranges (0 = number of worker threads).
    BUCKET_RANGES
};

/// The directory-sides knob: whether the trigger-time join kernels (sort-merge, compact-hash, run-merge)
/// build their directory over both sides or over the left side only, streaming the right side against it.
enum class JoinDirectorySides : uint8_t
{
    /// Default: directory (sorted run / bucket grouping) built over both sides, merged symmetrically.
    BOTH,
    /// Asymmetric: directory over the left side only; the right side is scanned once, each entry
    /// probing the left directory. Halves the directory-build cost of the trigger.
    ONE_SIDED
};

/// The directory-scope knob: whether the trigger-time join kernels build their directory per window
/// (rebuilt for every window and slice pair) or per slice (built once per slice, shared by all
/// overlapping windows — the join analogue of window slicing for aggregation).
enum class JoinStateScope : uint8_t
{
    /// Default: directory state lives per probe task (per window / slice pair).
    PER_WINDOW,
    /// Sorted (hash, position) runs are cached on the slice itself: built once by the first probe task
    /// touching a (slice, side), reused by every slice pair of every overlapping window.
    PER_SLICE
};

/// The pre-filter knob: an auxiliary negative directory checked before the real directory probe.
enum class JoinPrefilter : uint8_t
{
    NONE,
    /// Blocked Bloom filter over the left side (~8 bits/key, 2 probes); right entries whose bits miss
    /// skip the directory probe. One-sided trigger-time kernels only; pays off at low match rates.
    BLOOM
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
