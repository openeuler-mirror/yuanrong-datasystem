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

#ifndef DATASYSTEM_COMMON_LOG_BUTIL_LOG_SINK_LEASE_H
#define DATASYSTEM_COMMON_LOG_BUTIL_LOG_SINK_LEASE_H

namespace datasystem {

class ButilLogSinkLease final {
public:
    ButilLogSinkLease();
    ~ButilLogSinkLease();

    ButilLogSinkLease(const ButilLogSinkLease &) = delete;
    ButilLogSinkLease &operator=(const ButilLogSinkLease &) = delete;
    ButilLogSinkLease(ButilLogSinkLease &&) = delete;
    ButilLogSinkLease &operator=(ButilLogSinkLease &&) = delete;
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_LOG_BUTIL_LOG_SINK_LEASE_H
