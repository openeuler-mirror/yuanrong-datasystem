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
 * Description: Strict node-timeout flag validation helpers called explicitly at server startup /
 * dynamic update, plus validator implementations shared by the DS_DEFINE_validator registrations in
 * common_flags_validate.cpp and by explicit startup checks. Kept in a TU free of DS_DEFINE_validator
 * registrations so referencing these symbols does not pull the flag-validator static initializers
 * (e.g. encrypt_kit) into shared libraries such as libdatasystem_coordinator.so.
 */
#include "datasystem/common/flags/common_flags.h"

#include <cmath>
#include <limits>
#include <stdexcept>
#include <string>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/validator.h"

namespace datasystem {
namespace {
constexpr uint32_t K_MIN_NODE_TIMEOUT_S = 3;
constexpr uint32_t K_MIN_NODE_DEAD_TIMEOUT_S = 3;
}

bool ValidateNodeTimeoutS(const uint32_t value)
{
    if (value >= K_MIN_NODE_TIMEOUT_S) {
        return true;
    }
    LOG(ERROR) << FormatString(
        "The value of node_timeout_s flag is %u, which must be greater than or equal to %u.", value,
        K_MIN_NODE_TIMEOUT_S);
    return false;
}

bool ValidateNodeDeadTimeoutS(const uint32_t value)
{
    if (value < K_MIN_NODE_DEAD_TIMEOUT_S) {
        LOG(ERROR) << FormatString(
            "The value of node_dead_timeout_s flag is %u, which must be greater than or equal to %u.", value,
            K_MIN_NODE_DEAD_TIMEOUT_S);
        return false;
    }
    if (value <= FLAGS_node_timeout_s) {
        LOG(ERROR) << FormatString(
            "The value of node_dead_timeout_s flag is %u, which must be greater than the value of node_timeout_s "
            "flag (%u).",
            value, FLAGS_node_timeout_s);
        return false;
    }
    return true;
}

bool ValidateNodeTimeoutFlags()
{
    const bool timeoutOk = ValidateNodeTimeoutS(FLAGS_node_timeout_s);
    const bool deadTimeoutOk = ValidateNodeDeadTimeoutS(FLAGS_node_dead_timeout_s);
    return timeoutOk && deadTimeoutOk;
}

bool NodeDeadTimeoutFlagValidateSpecial(const std::string &flagName, const std::string &newVal)
{
    uint32_t result = 0;
    if (flagName != "node_dead_timeout_s") {
        return false;
    }
    try {
        size_t pos = 0;
        const unsigned long value = std::stoul(newVal, &pos);
        if (pos != newVal.size() || value > std::numeric_limits<uint32_t>::max()) {
            return false;
        }
        result = static_cast<uint32_t>(value);
    } catch (const std::logic_error &) {
        return false;
    }
    return !ValidateNodeDeadTimeoutS(result);
}

bool ValidateSampleRateRange(const char *flagName, double value)
{
    if (!std::isfinite(value) || value < 0.0 || value > 1.0) {
        LOG(ERROR) << FormatString("The %s flag is %f, which must be a finite value in [0.0, 1.0].", flagName, value);
        return false;
    }
    return true;
}

bool ValidateOptionalLogName(const char *flagName, const std::string &value)
{
    if (value.empty() || Validator::ValidateLogName(value)) {
        return true;
    }
    LOG(ERROR) << FormatString("The value of %s flag is %s, which must contain only a-z, A-Z, 0-9, and underscore.",
                               flagName, value);
    return false;
}

bool ValidateCommonStartupFlags()
{
    bool ok = true;
    ok = ValidateSampleRateRange("request_sample_rate", FLAGS_request_sample_rate) && ok;
    ok = ValidateSampleRateRange("access_sample_rate", FLAGS_access_sample_rate) && ok;
    ok = ValidateSampleRateRange("diagnostic_sample_rate", FLAGS_diagnostic_sample_rate) && ok;
    ok = ValidateOptionalLogName("log_filename", FLAGS_log_filename) && ok;
    ok = Validator::ValidateRpcThreadNum("brpc_server_num_threads", FLAGS_brpc_server_num_threads) && ok;
    ok = Validator::ValidateInt32("log_monitor_interval_ms", FLAGS_log_monitor_interval_ms) && ok;
    return ok;
}
}  // namespace datasystem
