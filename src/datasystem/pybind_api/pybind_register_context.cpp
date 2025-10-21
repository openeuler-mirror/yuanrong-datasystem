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
 * Description: Register Context to python.
 */
#include <map>
#include <memory>

#include <pybind11/numpy.h>

#include "datasystem/context/context.h"
#include "datasystem/pybind_api/pybind_register.h"

namespace datasystem {
PybindDefineRegisterer g_pybind_define_f_Context("Context", PRIORITY_LOW, ([](const py::module *m) {
                                                    (void)py::class_<Context, std::shared_ptr<Context>>(*m, "Context")
                                                        .def(py::init<>())
                                                        .def_static("set_trace_id", &Context::SetTraceId);
                                                }));
}  // namespace datasystem
