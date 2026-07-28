/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

#ifndef DATASYSTEM_COMMON_OBJECT_CACHE_PROVIDER_UB_FAILURE_DETAIL_H
#define DATASYSTEM_COMMON_OBJECT_CACHE_PROVIDER_UB_FAILURE_DETAIL_H

#include <cstdint>
#include <optional>
#include <string>

#include "datasystem/common/object_cache/peer_ub_admission.h"
#include "datasystem/protos/object_posix.pb.h"

namespace datasystem {
constexpr const char *PROVIDER_LOCAL_UB_WRITE_FAILURE_SIDE = "provider_local_ub_write";

inline void FillProviderUbFailureDetail(const Status &status, const std::string &failedEndpoint,
                                        const std::string &operatorWorker, std::optional<int> providerStatus,
                                        std::optional<int> cqeStatus, ProviderUbFailureDetailPb &detail)
{
    detail.Clear();
    detail.set_status_code(status.GetCode());
    detail.set_message(status.GetMsg());
    detail.set_failed_endpoint(failedEndpoint);
    detail.set_failure_side(PROVIDER_LOCAL_UB_WRITE_FAILURE_SIDE);
    detail.set_operator_worker(operatorWorker);
    if (providerStatus.has_value()) {
        detail.set_has_provider_status(true);
        detail.set_provider_status(*providerStatus);
    }
    if (cqeStatus.has_value()) {
        detail.set_has_cqe_status(true);
        detail.set_cqe_status(*cqeStatus);
    }
}

inline void UpdateProviderUbFailureDetailForWrappedStatus(const Status &sourceStatus, const Status &wrappedStatus,
                                                          ProviderUbFailureDetailPb &detail)
{
    if (detail.status_code() != static_cast<int32_t>(sourceStatus.GetCode())
        || detail.message() != sourceStatus.GetMsg()) {
        return;
    }
    detail.set_status_code(static_cast<int32_t>(wrappedStatus.GetCode()));
    detail.set_message(wrappedStatus.GetMsg());
}

inline void ReportProviderLocalUbWriteFailure(PeerUbAdmission *admission, const HostPort &operatorWorker,
                                              UbOperationKind operation, const Status &status,
                                              std::optional<int> providerStatus, std::optional<int> cqeStatus)
{
    if (admission == nullptr) {
        return;
    }
    UbOpOutcome outcome(operatorWorker, operation, status);
    outcome.providerStatus = providerStatus;
    outcome.cqeStatus = cqeStatus;
    outcome.learnedFrom = PROVIDER_LOCAL_UB_WRITE_FAILURE_SIDE;
    admission->ReportOutcome(outcome);
}

inline std::optional<UbOpOutcome> DecodeProviderUbFailureDetail(const ProviderUbFailureDetailPb &detail,
                                                                const HostPort &provider, UbOperationKind operation,
                                                                const std::string &learnedFrom)
{
    if (detail.failure_side() != PROVIDER_LOCAL_UB_WRITE_FAILURE_SIDE || detail.failed_endpoint().empty()
        || detail.operator_worker() != provider.ToString() || detail.status_code() == K_OK) {
        return std::nullopt;
    }
    UbOpOutcome outcome(provider, operation, Status(static_cast<StatusCode>(detail.status_code()), detail.message()));
    if (detail.has_provider_status()) {
        outcome.providerStatus = detail.provider_status();
    }
    if (detail.has_cqe_status()) {
        outcome.cqeStatus = detail.cqe_status();
    }
    outcome.learnedFrom = learnedFrom;
    return outcome;
}
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_OBJECT_CACHE_PROVIDER_UB_FAILURE_DETAIL_H
