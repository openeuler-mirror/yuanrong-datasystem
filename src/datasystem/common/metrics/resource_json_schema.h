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
 * Description: Schema mapping ResMetricName to JSON field names and the ods record whitelist.
 *              Single source of truth for how a handler's "/"-delimited string is split into
 *              named JSON sub-fields and which sub-fields/groups are emitted to kv_resource.log.
 */

#ifndef DATASYSTEM_COMMON_METRICS_RESOURCE_JSON_SCHEMA_H
#define DATASYSTEM_COMMON_METRICS_RESOURCE_JSON_SCHEMA_H

#include <array>
#include <cstddef>
#include <string>
#include <vector>

#include "datasystem/common/metrics/res_metric_name.h"

namespace datasystem {
/**
 * @brief Describes one ResMetricName group for JSON serialization.
 *
 * fieldNames: sub-field names in res_metrics.def order (snake_case). The handler returns a
 *             "/"-delimited string whose tokens line up with these names position-by-position.
 * recordMask: ods "record?" flag per sub-field; false sub-fields are dropped from JSON.
 * sep:        sub-field separator inside one handler string ('/' for multi-field, '\0' for single).
 * recordGroup: ods group-level flag; when false the whole group is omitted from JSON.
 * groupName:  snake_case enum name, used as the JSON object key for the group.
 */
struct ResourceFieldDesc {
    std::vector<const char *> fieldNames;
    std::vector<bool> recordMask;
    char sep = '/';
    bool recordGroup = true;
    const char *groupName = "";
};

/**
 * @brief Get the schema descriptor for a given ResMetricName.
 * @param[in] name The metric group enum.
 * @return Const reference to the descriptor. Returns a recordGroup=false descriptor for
 *         names outside the recorded range, so callers can uniformly skip them.
 */
const ResourceFieldDesc &GetResourceFieldDesc(ResMetricName name);

/**
 * @brief Split a "/"-delimited handler string into tokens.
 * @param[in] raw The handler return string (e.g. "73814/80000/1073741824/0.069/0/0").
 * @param[in] sep The separator ('/' for multi-field, '\0' means the whole string is one token).
 * @param[out] tokens Output tokens in order.
 */
void SplitResourceFields(const std::string &raw, char sep, std::vector<std::string> &tokens);
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_METRICS_RESOURCE_JSON_SCHEMA_H
