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
 * Description: Register function to python.
 */
#include "datasystem/pybind_api/pybind_register.h"

#include <map>

#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <securec.h>

namespace datasystem {
PybindDefinedFunctionRegister &PybindDefinedFunctionRegister::GetSingleton()
{
    static PybindDefinedFunctionRegister instance;
    return instance;
}

// This is where we externalize the C logic as python modules.
// Import all functions with priority = 0 as *client_lib.
PYBIND11_MODULE(libds_client_py, m)
{
    m.doc() = "pybind11 for object_cache client";

    auto all_fns = datasystem::PybindDefinedFunctionRegister::AllFunctions();

    py::register_exception<FutureTimeoutException>(m, "FutureTimeoutException");
    py::register_local_exception<FutureTimeoutException>(m, "FutureTimeoutException");

    for (auto &item : all_fns) {
        if (item.first == 0) {
            for (auto &func : item.second) {
                func.second(&m);
            }
        }
    }
}
}  // namespace datasystem
