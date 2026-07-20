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

#include <Join/JoinTriggerStrategy.hpp>

#include <memory>
#include <vector>
#include <Join/StreamJoinUtil.hpp>
#include <SliceStore/Slice.hpp>

namespace NES
{

std::vector<ProbeWorkItem> InnerJoinTriggerStrategy::collectProbeWorkItems(const std::vector<std::shared_ptr<Slice>>& allSlices)
{
    std::vector<ProbeWorkItem> workItems;
    workItems.reserve(allSlices.size() * allSlices.size());
    for (const auto& sliceLeft : allSlices)
    {
        for (const auto& sliceRight : allSlices)
        {
            workItems.emplace_back(ProbeWorkItem{{sliceLeft}, {sliceRight}, ProbeTaskType::MATCH_PAIRS});
        }
    }
    return workItems;
}

template <bool EmitLeftNullFill, bool EmitRightNullFill>
std::vector<ProbeWorkItem>
OuterJoinTriggerStrategy<EmitLeftNullFill, EmitRightNullFill>::collectProbeWorkItems(const std::vector<std::shared_ptr<Slice>>& allSlices)
{
    std::vector<ProbeWorkItem> workItems;

    /// 1) NxN MATCH_PAIRS — same as inner join
    for (const auto& sliceLeft : allSlices)
    {
        for (const auto& sliceRight : allSlices)
        {
            workItems.emplace_back(ProbeWorkItem{{sliceLeft}, {sliceRight}, ProbeTaskType::MATCH_PAIRS});
        }
    }

    /// 2) LEFT_NULL_FILL: each left slice vs ALL right slices
    if constexpr (EmitLeftNullFill)
    {
        for (const auto& slice : allSlices)
        {
            workItems.emplace_back(ProbeWorkItem{{slice}, allSlices, ProbeTaskType::LEFT_NULL_FILL});
        }
    }

    /// 3) RIGHT_NULL_FILL: ALL left slices vs each right slice
    if constexpr (EmitRightNullFill)
    {
        for (const auto& slice : allSlices)
        {
            workItems.emplace_back(ProbeWorkItem{allSlices, {slice}, ProbeTaskType::RIGHT_NULL_FILL});
        }
    }

    return workItems;
}

/// Explicit instantiations for all used combinations
template struct OuterJoinTriggerStrategy<true, false>;
template struct OuterJoinTriggerStrategy<false, true>;
template struct OuterJoinTriggerStrategy<true, true>;

}
