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
#ifndef DATASYSTEM_PYTHON_API_PYBIND_REGISTER_H
#define DATASYSTEM_PYTHON_API_PYBIND_REGISTER_H

#include <functional>
#include <stdexcept>
#include <string>

#include "pybind11/pybind11.h"
#include "pybind11/stl.h"

namespace py = pybind11;
namespace datasystem {
using PybindDefineFunc = std::function<void(py::module *)>;
#define PYBIND_EXPORT __attribute__((visibility("default")))
constexpr int PRIORITY_LOW = 0;
constexpr int PRIORITY_MID = 1;

class PYBIND_EXPORT PybindDefinedFunctionRegister {
public:
    /**
     * @brief Register the name, priority, and implementation of the function to be registered.
     * @param[in] name The name of the registered function.
     * @param[in] priority The priority of the registered function.
     * @param[in] fn The implementation of the registered function.
     */
    static void Register(const std::string &name, const uint8_t &priority, const PybindDefineFunc &fn)
    {
        return GetSingleton().RegisterFn(name, priority, fn);
    }

    PybindDefinedFunctionRegister(const PybindDefinedFunctionRegister &) = delete;

    PybindDefinedFunctionRegister &operator=(const PybindDefinedFunctionRegister &) = delete;

    /**
     * @brief Get all the registered functions.
     * @return The map which stores the priority, name, and implementation of the registered functions.
     */
    static std::map<uint8_t, std::map<std::string, PybindDefineFunc>> &AllFunctions()
    {
        return GetSingleton().moduleFns_;
    }

    /**
     * @brief Get all the registered functions with corresponding priority.
     * @return The map which stores the name and implementation of the registered functions.
     */
    static std::map<std::string, PybindDefineFunc> &LevelFunctions(const uint8_t &priority)
    {
        return GetSingleton().moduleFns_[priority];
    }

protected:
    PybindDefinedFunctionRegister() = default;

    virtual ~PybindDefinedFunctionRegister() = default;

    static PybindDefinedFunctionRegister &GetSingleton();

    void RegisterFn(const std::string &name, const uint8_t &priority, const PybindDefineFunc &fn)
    {
        moduleFns_[priority][name] = fn;
    }

    // It is used to store the priority, name, and implementation of the registered functions.
    std::map<uint8_t, std::map<std::string, PybindDefineFunc>> moduleFns_;
};

class PybindDefineRegisterer {
public:
    /**
     * @brief Register functions by variable declarations
     * @param[in] name The name of the registered function.
     * @param[in] priority The priority of the registered function.
     * @param[in] fn The implementation of the registered function.
     */
    PybindDefineRegisterer(const std::string &name, const uint8_t &priority, const PybindDefineFunc &fn)
    {
        PybindDefinedFunctionRegister::Register(name, priority, fn);
    }
    ~PybindDefineRegisterer() = default;
};

class FutureTimeoutException : public std::runtime_error {
public:
    explicit FutureTimeoutException(const std::string &message) : std::runtime_error(message)
    {
    }
};
}  // namespace datasystem
#endif  // DATASYSTEM_PYTHON_API_PYBIND_REGISTER_H
