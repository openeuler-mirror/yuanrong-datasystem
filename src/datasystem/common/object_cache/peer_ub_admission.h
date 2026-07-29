/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/** Description: Track process-local UB data-provider admission state. */

#ifndef DATASYSTEM_COMMON_OBJECT_CACHE_PEER_UB_ADMISSION_H
#define DATASYSTEM_COMMON_OBJECT_CACHE_PEER_UB_ADMISSION_H

#include <cstdint>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <unordered_map>

#include "datasystem/common/object_cache/ub_failure_classifier.h"

namespace datasystem {

enum class UbAdmissionState { AVAILABLE, SUSPECT, UNAVAILABLE, PROBING };

struct UbPathState {
    UbAdmissionState state = UbAdmissionState::AVAILABLE;
    Status lastStatus;
    UbFailureClass lastFailureClass = UbFailureClass::SUCCESS;
    uint64_t epoch = 0;
};

class PeerUbAdmission {
public:
    PeerUbAdmission() = default;
    ~PeerUbAdmission() = default;

    Status CheckWriteTarget(const HostPort &peer, UbOperationKind op) const;
    Status CheckReadSource(const HostPort &peer) const;
    void ReportOutcome(const UbOpOutcome &outcome);
    std::optional<UbPathState> GetState(const HostPort &peer) const;

private:
    static bool ShouldBlock(const UbPathState &state);
    static Status BuildUnavailableStatus(const HostPort &peer, StatusCode code);

    mutable std::shared_mutex mutex_;
    std::unordered_map<HostPort, UbPathState> states_;
    UbFailureClassifier classifier_;
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_OBJECT_CACHE_PEER_UB_ADMISSION_H
