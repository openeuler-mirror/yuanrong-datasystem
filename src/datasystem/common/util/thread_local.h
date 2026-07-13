/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Per-request context type bundle.
 *
 * Historically this header declared 8 global ScopedBthreadLocal<T> variables
 * (timeoutDuration, scTimeoutDuration, reqTimeoutDuration, g_SerializedMessage,
 * g_ContextTenantId, g_ReqAk, g_ReqSignature, g_ReqTimestamp). Those globals
 * have been migrated INTO the RequestContext struct — see
 * common/util/request_context.h — and are accessed via GetRequestContext()->field.
 *
 * This header now only forwards the per-request type definitions
 * (TimeoutDuration, ZmqMessage, ScopedBthreadLocal) so the many translation
 * units that historically included it for those types continue to build. New
 * code should include request_context.h directly and use GetRequestContext().
 */

#ifndef DATASYSTEM_COMMON_UTIL_THREAD_LOCAL_H
#define DATASYSTEM_COMMON_UTIL_THREAD_LOCAL_H

#include <string>

#include "datasystem/common/log/time_cost.h"
#include "datasystem/common/rpc/scoped_bthread_local.h"
#include "datasystem/common/rpc/timeout_duration.h"
#include "datasystem/common/rpc/zmq/zmq_message.h"

namespace datasystem {

// The 8 per-request variables formerly declared here now live as fields of
// RequestContext. Access them via GetRequestContext()->xxxTimeoutDuration /
// ->serializedMessage / ->tenantId / ->reqAk / ->reqSignature / ->reqTimestamp
// (see common/util/request_context.h). ScopedBthreadLocal<T> is retained as a
// standalone template for direct unit testing (tests/ut/common/rpc/bthread_local_test.cpp).

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_UTIL_THREAD_LOCAL_H
