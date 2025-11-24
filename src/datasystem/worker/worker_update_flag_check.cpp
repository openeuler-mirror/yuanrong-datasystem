/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
#include "datasystem/worker/worker_update_flag_check.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/validator.h"

DS_DECLARE_string(worker_address);
DS_DECLARE_uint32(node_timeout_s);

namespace datasystem {

bool StrToUint32(const std::string &str, uint32_t &result)
{
    try {
        size_t pos = 0;
        unsigned long value = std::stoul(str, &pos);
        if (pos != str.size()) {
            return false;
        }
        if (value > std::numeric_limits<uint32_t>::max()) {
            return false;
        }

        result = static_cast<uint32_t>(value);
        return true;
    } catch (const std::invalid_argument &) {
        return false;
    } catch (const std::out_of_range &) {
        return false;
    }
    return true;
}

bool WorkerFlagValidateSpecial(const std::string &flagName, const std::string &newVal)
{
    uint32_t result = 0;
    if (flagName == "node_dead_timeout_s" && StrToUint32(newVal, result) && !WorkerValidateNodeDeadTimeoutS(result)) {
        return true;
    }
    if (flagName == "heartbeat_interval_ms" && StrToUint32(newVal, result)
        && !WorkerValidateHeartbeatIntervalMs(result)) {
        return true;
    }
    return false;
}

bool WorkerValidateNodeDeadTimeoutS(const uint32_t value)
{
    if (value <= FLAGS_node_timeout_s) {
        LOG(ERROR) << "The value of node_dead_timeout_s must be greater than the value of node_timeout_s.";
        return false;
    }
    return true;
}

bool WorkerValidateHeartbeatIntervalMs(const uint32_t value)
{
    if (value >= FLAGS_node_timeout_s * MS_PER_SECOND) {
        LOG(ERROR)
            << "The value of heartbeat_interval_ms must be a thousand times smaller than the value of node_timeout_s.";
        return false;
    }
    return true;
}
}  // namespace datasystem