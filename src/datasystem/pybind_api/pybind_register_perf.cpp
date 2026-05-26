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
 * Description: Register perf client to python.
 */
#include <cstdint>
#include <memory>
#include <unordered_map>

#ifdef ENABLE_PERF
#include "datasystem/common/log/trace.h"
#include "datasystem/perf_client.h"
#endif

#include "datasystem/pybind_api/pybind_register.h"

namespace datasystem {
#ifdef ENABLE_PERF
using datasystem::ConnectOptions;

PybindDefineRegisterer g_pybind_define_f_PerfClient("PerfClient", PRIORITY_LOW, [](const py::module *m) {
    py::class_<PerfClient, std::shared_ptr<PerfClient>>(*m, "PerfClient")
        .def(py::init([](const std::string &host, int32_t port, int32_t connectTimeoutMs, const std::string &accessKey,
                         const std::string &secretKey) {
            ConnectOptions connectOpts{
                .host = host,
                .port = port,
                .connectTimeoutMs = connectTimeoutMs,
                .accessKey = accessKey,
                .secretKey = secretKey,
            };
            return std::make_unique<PerfClient>(connectOpts);
        }))
        .def("init", [](PerfClient &client) {
            TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
            return client.Init();
        })
        .def("get_perf_log", [](PerfClient &client, const std::string &type) {
            TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
            std::unordered_map<std::string, std::unordered_map<std::string, uint64_t>> perfLog;
            auto rc = client.GetPerfLog(type, perfLog);
            return py::make_tuple(rc, perfLog);
        })
        .def("reset_perf_log", [](PerfClient &client, const std::string &type) {
            TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
            return client.ResetPerfLog(type);
        });
});
#endif
}  // namespace datasystem
