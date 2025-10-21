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
 * Description: A general framework for creating a ZMQ app.
 */
#ifndef DATASYSTEM_COMMON_RPC_RPC_HELPER_H
#define DATASYSTEM_COMMON_RPC_RPC_HELPER_H

#include <atomic>
#include <cstring>
#include <future>
#include <sstream>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>

#include <semaphore.h>
#include <sys/types.h>
#include <unistd.h>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
/**
 * @brief Interruptible abstract class.
 */
class Interruptible {
    /**
     * @brief Virtual function. Set interrupt flag.
     */
    virtual void Interrupt() = 0;

    /**
     * @brief Virtual function. Check if interrupted.
     */
    virtual bool IsInterrupted() const = 0;
};

/**
 * @brief Callable abstract class.
 */
class Callable : public Interruptible {
public:
    Callable() : interruptFlag_(false)
    {
    }
    ~Callable() = default;

    /**
     * @brief Virtual function.
     * @return Status of call.
     */
    virtual Status operator()() = 0;

    /**
     * @brief Set interrupt flag.
     */
    void Interrupt() override
    {
        interruptFlag_ = true;
    }

    /**
     * @brief Check if interrupted.
     */
    bool IsInterrupted() const override
    {
        return interruptFlag_;
    }

protected:
    std::atomic<bool> interruptFlag_;
};

/**
 * @brief If T is callable, we start an async thread to call its entry function.
 * @param[in] obj A functor.
 * @return A future status object.
 */
template <typename T, typename std::enable_if<std::is_base_of<Callable, T>::value, T>::type* = nullptr>
inline std::future<Status> loadFunctor(T &obj)
{
    return std::async(std::launch::async, std::ref(obj));
}

/**
 * @brief If T is not callable, we return a dummy.
 * @param[in] obj A functor.
 * @return A future status object. This future never blocks.
 */
template <typename T, typename std::enable_if<!std::is_base_of<Callable, T>::value, T>::type* = nullptr>
inline std::future<Status> loadFunctor(T &obj)
{
    (void)obj;
    return {};
}

/**
 *
 * @tparam T Typename.
 * @return Whether T is a callable type.
 */
template <typename T>
inline bool IsCallable()
{
    return std::is_base_of<Callable, T>::value;
}

/**
 * @brief Call the interrupt function of base class if it is derived from Interruptible.
 * @tparam T Typename.
 * @param[in] obj Interruptible service object.
 */
template <typename T, typename std::enable_if<std::is_base_of<Interruptible, T>::value, T>::type *P = nullptr>
inline void InterruptService(T &obj)
{
    obj.Interrupt();
}

template <typename T, typename std::enable_if<!std::is_base_of<Interruptible, T>::value, T>::type *P = nullptr>
inline void InterruptService(T &obj)
{
    (void)obj;
}

/**
 *
 * @tparam T Typename.
 * @return Whether T is a Interruptible service type.
 */
template <typename T>
inline bool IsInterruptible()
{
    return std::is_base_of<Interruptible, T>::value;
}
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_RPC_RPC_HELPER_H
