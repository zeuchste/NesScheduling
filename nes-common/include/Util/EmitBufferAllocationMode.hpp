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

/// Strategy the Emit operator uses to allocate its output buffer (#1711). Selectable so the evaluation can benchmark
/// the strategies head-to-head. The mode is fixed at query-compile time and specialised into the generated code.
enum class EmitBufferAllocationMode : uint8_t
{
    /// Allocate a full operator-buffer-size buffer up front and emit it even when empty. Current/default behaviour.
    EagerFull,
    /// Size the output buffer from the input record count (an upper bound for map/filter/projection), capped at the
    /// operator buffer size, allocated up front. One pass, no copy. Needs runtime capacity (#1703) + size classes (#1706).
    InputSized,
    /// Write into a temporary staging buffer, then copy the written records into an exactly-right-sized final buffer
    /// (using the staging buffer directly when it is already right-sized). Exact sizing at a copy cost.
    StageAndCopy,
    /// Keep full-size buffers but carry a partially-filled output buffer across pipeline invocations, flushing only
    /// when full (or at query end), to amortise allocation and reduce buffer count.
    ReuseAcrossRuns
};

}
