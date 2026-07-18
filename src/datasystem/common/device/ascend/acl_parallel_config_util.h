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
 * Description: Helpers shared by the parallel memcpy executors' config loading (executor-local).
 */
#ifndef DATASYSTEM_COMMON_DEVICE_ASCEND_ACL_PARALLEL_CONFIG_UTIL_H
#define DATASYSTEM_COMMON_DEVICE_ASCEND_ACL_PARALLEL_CONFIG_UTIL_H

#include <cstdint>
#include <cstdlib>

#include "datasystem/common/flags/string_to_long.h"
#include "datasystem/common/util/format.h"
#include "datasystem/utils/status.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
/**
 * @brief Parse an environment variable as a uint64 in [minValue, maxValue].
 * @note If unset, value is left untouched. Returns K_INVALID on parse failure or out-of-range.
 */
inline Status ParseUint64FromEnv(const char *key, uint64_t minValue, uint64_t maxValue, uint64_t &value)
{
    auto strValue = std::getenv(key);
    RETURN_OK_IF_TRUE(strValue == nullptr);
    try {
        auto parsedValue = StrToUnsignedLong(strValue);
        CHECK_FAIL_RETURN_STATUS(
            parsedValue >= minValue && parsedValue <= maxValue, K_INVALID,
            FormatString("Env %s value %s is out of range [%llu, %llu]", key, strValue,
                         static_cast<unsigned long long>(minValue), static_cast<unsigned long long>(maxValue)));
        value = parsedValue;
    } catch (const std::invalid_argument &) {
        RETURN_STATUS(K_INVALID, FormatString("Env %s value %s is not an unsigned integer", key, strValue));
    } catch (const std::out_of_range &) {
        RETURN_STATUS(K_INVALID, FormatString("Env %s value %s is out of uint64 range", key, strValue));
    }
    return Status::OK();
}
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_DEVICE_ASCEND_ACL_PARALLEL_CONFIG_UTIL_H
