/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: some thread_local object.
 */

#ifndef DATASYSTEM_COMMON_UTIL_THREAD_LOCAL_H
#define DATASYSTEM_COMMON_UTIL_THREAD_LOCAL_H

#include <string>

#include "datasystem/common/log/time_cost.h"
#include "datasystem/common/rpc/timeout_duration.h"
#include "datasystem/common/rpc/zmq/zmq_message.h"
namespace datasystem {
extern thread_local TimeoutDuration timeoutDuration;
extern thread_local TimeoutDuration scTimeoutDuration;
extern thread_local TimeoutDuration reqTimeoutDuration;
extern thread_local ZmqMessage g_SerializedMessage;
extern thread_local TimeCost workerOperationTimeCost;
extern thread_local TimeCost masterOperationTimeCost;
extern thread_local std::string g_MetaRocksDbName;
extern thread_local std::string g_ContextTenantId;
extern thread_local std::string g_ReqAk;
extern thread_local std::string g_ReqSignature;
extern thread_local uint64_t g_ReqTimestamp;
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_THREAD_LOCAL_H