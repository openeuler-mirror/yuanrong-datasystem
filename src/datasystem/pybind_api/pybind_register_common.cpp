/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: Register common function to python.
 */
#include <map>
#include <memory>

#include <pybind11/numpy.h>

#include "datasystem/common/log/log.h"
#include "datasystem/pybind_api/pybind_register.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
PybindDefineRegisterer g_pybind_define_f_Status("Status", PRIORITY_LOW, ([](const py::module *m) {
                                                    (void)py::class_<Status, std::shared_ptr<Status>>(*m, "Status")
                                                        .def(py::init<>())
                                                        .def("is_ok", &Status::IsOk)
                                                        .def("to_string", &Status::ToString)
                                                        .def("is_error", &Status::IsError)
                                                        .def("get_msg", &Status::GetMsg);
                                                }));
}  // namespace datasystem
