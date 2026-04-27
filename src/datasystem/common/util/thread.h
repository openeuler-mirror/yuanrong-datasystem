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
 * Description: Thread.
 */
#ifndef DATASYSTEM_COMMON_UTIL_THREAD_H
#define DATASYSTEM_COMMON_UTIL_THREAD_H

#include <sys/resource.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <cerrno>

#include <pthread.h>
#include <string>
#include <thread>

#include "datasystem/common/flags/flags.h"

DS_DECLARE_int32(io_thread_nice);

namespace datasystem {
class Thread {
public:
    Thread() noexcept = default;
    Thread(Thread &) = delete;
    Thread(const Thread &) = delete;
    Thread(const Thread &&) = delete;

    Thread(Thread &&other) noexcept
    {
        swap(other);
    }
    ~Thread() = default;

    template <typename F, typename... Args>
    Thread(F &&f, Args &&...args)
        : thread_(WrapFn<std::decay_t<F>, std::decay_t<Args>...>, std::forward<F>(f), std::forward<Args>(args)...)
    {
    }

    Thread &operator=(const Thread &) = delete;

    Thread &operator=(Thread &&other) noexcept
    {
        thread_ = std::move(other.thread_);
        return *this;
    }

    std::thread::id get_id() const noexcept
    {
        return thread_.get_id();
    }

    bool joinable() const noexcept
    {
        return thread_.joinable();
    }

    void join()
    {
        thread_.join();
    }

    void detach()
    {
        thread_.detach();
    }

    void swap(Thread &other) noexcept
    {
        thread_.swap(other.thread_);
    }

    void set_name(const std::string &name)
    {
        const size_t taskCommLen = 15;
        auto truncateName = name.substr(0, taskCommLen);
        auto handle = thread_.native_handle();
        (void)pthread_setname_np(handle, truncateName.c_str());
    }

    static bool SetCurrentThreadNice(int nice)
    {
        if (FLAGS_io_thread_nice == 0) {
            return true;
        }
        return SetNiceByTid(static_cast<pid_t>(syscall(SYS_gettid)), nice);
    }

private:
    // If an unhandled exception occurs in an std::thread, the stack is unwound before std::terminate is called, which
    // makes it impossible to find the location of the exception. The supposed fix was to use noexcept on the internal
    // thread main function
    template <typename F, typename... Args>
    static auto WrapFn(F &&f, Args &&...args) noexcept -> decltype(std::ref(f)(std::forward<Args>(args)...))
    {
        return std::ref(f)(std::forward<Args>(args)...);
    }

    static bool SetNiceByTid(pid_t tid, int nice)
    {
        return setpriority(PRIO_PROCESS, tid, nice) == 0;
    }

    std::thread thread_;
};
}  // namespace datasystem
#endif
