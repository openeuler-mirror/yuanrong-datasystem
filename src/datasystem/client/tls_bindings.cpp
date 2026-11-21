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
// These optional TLS initializers belong to the pinned SDK dependencies. Hidden weak references
// resolve within the bridge and remain null when the dependency has no initializer.
#define DS_BIND_TLS_INIT(name)                                                                \
    extern "C" void name() __attribute__((weak, visibility("hidden")));                       \
    namespace {                                                                               \
    __attribute__((used)) auto *const ref##name = &name;                                      \
    }

DS_BIND_TLS_INIT(_ZTHN3re25hooks7contextE)
DS_BIND_TLS_INIT(_ZTHN4absl12lts_2024072213cord_internal17cordz_next_sampleE)
DS_BIND_TLS_INIT(_ZTHN5butil18tls_stringmap_tempE)
DS_BIND_TLS_INIT(_ZTHN9grpc_core20PerCpuShardingHelper6state_E)
DS_BIND_TLS_INIT(_ZTHN9grpc_core26ApplicationCallbackExecCtx18callback_exec_ctx_E)
DS_BIND_TLS_INIT(_ZTHN9grpc_core7ExecCtx9exec_ctx_E)
DS_BIND_TLS_INIT(_ZTHN9grpc_core8Activity19g_current_activity_E)
DS_BIND_TLS_INIT(_ZTHN9grpc_core9Timestamp25thread_local_time_source_E)

#undef DS_BIND_TLS_INIT
