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
#include "datasystem/common/rdma/fast_transport_base.h"
#include "datasystem/protos/object_posix.pb.h"

namespace datasystem {
constexpr const char *PROVIDER_LOCAL_UB_WRITE_FAILURE_SIDE = "provider_local_ub_write";
constexpr const char *MIGRATION_LOCAL_UB_READ_FAILURE_SIDE = "migration_local_ub_read";
constexpr const char *REMOTE_UB_ACK_TIMEOUT_FAILURE_SIDE = "remote_ub_ack_timeout";

inline bool HasRawUrmaStatus(std::optional<int> providerStatus, std::optional<int> cqeStatus, int expected)
{
    return (providerStatus.has_value() && *providerStatus == expected)
           || (cqeStatus.has_value() && *cqeStatus == expected);
}

inline bool IsRemoteUbAckTimeout(std::optional<int> providerStatus, std::optional<int> cqeStatus)
{
    return !HasRawUrmaStatus(providerStatus, cqeStatus, URMA_PORT_UNAVAILABLE_STATUS)
           && cqeStatus.has_value() && *cqeStatus == URMA_REMOTE_ACK_TIMEOUT_STATUS;
}

inline void FillProviderUbFailureDetail(const Status &status, const std::string &failedEndpoint,
                                        const std::string &operatorWorker, std::optional<int> providerStatus,
                                        std::optional<int> cqeStatus, ProviderUbFailureDetailPb &detail)
{
    detail.Clear();
    detail.set_status_code(status.GetCode());
    detail.set_message(status.GetMsg());
    detail.set_failed_endpoint(failedEndpoint);
    detail.set_failure_side(IsRemoteUbAckTimeout(providerStatus, cqeStatus)
                                ? REMOTE_UB_ACK_TIMEOUT_FAILURE_SIDE
                                : PROVIDER_LOCAL_UB_WRITE_FAILURE_SIDE);
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

inline void FillMigrationUbReadFailureDetail(const Status &status, const std::string &failedEndpoint,
                                             const std::string &operatorWorker, const UrmaWriteFailure &failure,
                                             ProviderUbFailureDetailPb &detail)
{
    detail.Clear();
    detail.set_status_code(status.GetCode());
    detail.set_message(status.GetMsg());
    detail.set_failed_endpoint(failedEndpoint);
    detail.set_failure_side(IsRemoteUbAckTimeout(failure.providerStatus, failure.cqeStatus)
                                ? REMOTE_UB_ACK_TIMEOUT_FAILURE_SIDE
                                : MIGRATION_LOCAL_UB_READ_FAILURE_SIDE);
    detail.set_operator_worker(operatorWorker);
    if (failure.providerStatus.has_value()) {
        detail.set_has_provider_status(true);
        detail.set_provider_status(*failure.providerStatus);
    }
    if (failure.cqeStatus.has_value()) {
        detail.set_has_cqe_status(true);
        detail.set_cqe_status(*failure.cqeStatus);
    }
}

inline void ReportLocalUbOperationFailure(PeerUbAdmission *admission, const HostPort &operatorWorker,
                                          const HostPort &remoteEndpoint, UbOperationKind operation,
                                          const Status &status, std::optional<int> providerStatus,
                                          std::optional<int> cqeStatus)
{
    if (admission == nullptr) {
        return;
    }
    const bool remoteAckTimeout = IsRemoteUbAckTimeout(providerStatus, cqeStatus);
    if (remoteAckTimeout && (operation == UbOperationKind::CLIENT_GET_WRITEBACK || remoteEndpoint.Empty())) {
        return;
    }
    UbOpOutcome outcome(remoteAckTimeout ? remoteEndpoint : operatorWorker, operation, status);
    outcome.providerStatus = providerStatus;
    outcome.cqeStatus = cqeStatus;
    if (remoteAckTimeout) {
        outcome.learnedFrom = REMOTE_UB_ACK_TIMEOUT_FAILURE_SIDE;
    } else if (operation == UbOperationKind::MIGRATION_READ) {
        outcome.learnedFrom = MIGRATION_LOCAL_UB_READ_FAILURE_SIDE;
    } else {
        outcome.learnedFrom = PROVIDER_LOCAL_UB_WRITE_FAILURE_SIDE;
    }
    admission->ReportOutcome(outcome);
}

inline std::optional<UbOpOutcome> DecodeProviderUbFailureDetail(const ProviderUbFailureDetailPb &detail,
                                                                const HostPort &provider, UbOperationKind operation,
                                                                const std::string &learnedFrom)
{
    const bool migrationOperation =
        operation == UbOperationKind::MIGRATION_READ || operation == UbOperationKind::MIGRATION_WRITE;
    const bool remoteAckTimeout = detail.failure_side() == REMOTE_UB_ACK_TIMEOUT_FAILURE_SIDE;
    const bool supportedSide = detail.failure_side() == PROVIDER_LOCAL_UB_WRITE_FAILURE_SIDE
                               || remoteAckTimeout
                               || (migrationOperation && detail.failure_side() == MIGRATION_LOCAL_UB_READ_FAILURE_SIDE);
    if (!supportedSide || detail.failed_endpoint().empty()
        || detail.operator_worker() != provider.ToString() || detail.status_code() == K_OK) {
        return std::nullopt;
    }
    HostPort attributedPeer = provider;
    if (remoteAckTimeout) {
        if (operation == UbOperationKind::CLIENT_GET_WRITEBACK
            || attributedPeer.ParseString(detail.failed_endpoint()).IsError()) {
            return std::nullopt;
        }
    }
    UbOpOutcome outcome(attributedPeer, operation,
                        Status(static_cast<StatusCode>(detail.status_code()), detail.message()));
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
