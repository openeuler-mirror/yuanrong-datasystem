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

#ifndef DATASYSTEM_COMMON_OBJECT_CACHE_UB_FAILURE_CLASSIFIER_H
#define DATASYSTEM_COMMON_OBJECT_CACHE_UB_FAILURE_CLASSIFIER_H

#include <cstdint>
#include <optional>
#include <string>
#include <utility>

#include "datasystem/common/util/net_util.h"
#include "datasystem/utils/status.h"

namespace datasystem {

enum class UbOperationKind {
    CLIENT_PUT,
    CLIENT_GET_WRITEBACK,
    WORKER_REMOTE_GET_WRITEBACK,
    MIGRATION_READ,
    MIGRATION_WRITE
};

enum class UbFailureClass {
    SUCCESS,
    PORT_UNAVAILABLE_ERROR4,
    TIMEOUT_SUSPECT,
    CONNECT_OR_PATH_FAILURE,
    LOCAL_RESOURCE_PRESSURE,
    NON_UB_FAILURE
};

struct UbOpOutcome {
    UbOpOutcome(HostPort peer, UbOperationKind op, Status status)
        : peer(std::move(peer)), op(op), status(std::move(status))
    {
    }

    HostPort peer;
    UbOperationKind op;
    Status status;
    std::optional<int> providerStatus;
    std::optional<int> cqeStatus;
    uint64_t payloadSize = 0;
    std::string learnedFrom;
};

class UbFailureClassifier {
public:
    ~UbFailureClassifier() = default;

    UbFailureClass Classify(const UbOpOutcome &outcome) const;

private:
    static bool HasProviderStatus(const UbOpOutcome &outcome, int status);
};
}  // namespace datasystem

#endif
