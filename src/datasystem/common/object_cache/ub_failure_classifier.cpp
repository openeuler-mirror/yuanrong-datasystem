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

/**
 * Description: Classify UB dataplane operation outcomes for worker isolation.
 */

#include "datasystem/common/object_cache/ub_failure_classifier.h"

namespace datasystem {
namespace {
constexpr int URMA_PROVIDER_PORT_UNAVAILABLE = 4;
constexpr int URMA_REMOTE_ACK_TIMEOUT = 9;
}  // namespace

UbFailureClass UbFailureClassifier::Classify(const UbOpOutcome &outcome) const
{
    if (outcome.status.IsOk()) {
        return UbFailureClass::SUCCESS;
    }
    if (HasProviderStatus(outcome, URMA_PROVIDER_PORT_UNAVAILABLE)) {
        return UbFailureClass::PORT_UNAVAILABLE_ERROR4;
    }
    if (outcome.cqeStatus.has_value() && *outcome.cqeStatus == URMA_REMOTE_ACK_TIMEOUT) {
        return UbFailureClass::REMOTE_UNAVAILABLE_ERROR9;
    }
    if (outcome.status.GetCode() == StatusCode::K_URMA_WAIT_TIMEOUT
        || outcome.status.GetCode() == StatusCode::K_RPC_DEADLINE_EXCEEDED
        || outcome.status.GetCode() == StatusCode::K_RPC_UNAVAILABLE) {
        return UbFailureClass::TIMEOUT_SUSPECT;
    }
    if (outcome.status.GetCode() == StatusCode::K_TRY_AGAIN
        || outcome.status.GetCode() == StatusCode::K_URMA_TRY_AGAIN) {
        return UbFailureClass::LOCAL_RESOURCE_PRESSURE;
    }
    const bool hasRawUrmaEvidence = outcome.providerStatus.has_value() || outcome.cqeStatus.has_value();
    if ((outcome.status.GetCode() == StatusCode::K_URMA_ERROR && hasRawUrmaEvidence)
        || outcome.status.GetCode() == StatusCode::K_URMA_NEED_CONNECT
        || outcome.status.GetCode() == StatusCode::K_URMA_CONNECT_FAILED) {
        return UbFailureClass::CONNECT_OR_PATH_FAILURE;
    }
    return UbFailureClass::NON_UB_FAILURE;
}

bool UbFailureClassifier::HasProviderStatus(const UbOpOutcome &outcome, int status)
{
    return (outcome.providerStatus.has_value() && outcome.providerStatus.value() == status)
           || (outcome.cqeStatus.has_value() && outcome.cqeStatus.value() == status);
}
}  // namespace datasystem
