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
 * Description: Inject action for ut.
 */
#ifndef DATASYSTEM_COMMON_INJECT_POINT_H
#define DATASYSTEM_COMMON_INJECT_POINT_H

#include <atomic>
#include <condition_variable>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/log/log.h"

namespace datasystem {
namespace inject {
#ifdef WITH_TESTS
enum class TaskType { NONE, OFF, RETURN, CALL, PRINT, SLEEP, YIELD, BUSY, PAUSE, ABORT };

/**
 * @brief Inject task define.
 */
class Task {
public:
    /**
     * @brief Construct a new Task object.
     * @param[in] type Task type.
     */
    explicit Task(TaskType type) : type_(type), str_(""), num_(0)
    {
    }

    /**
     * @brief Construct a new Task object.
     * @param[in] type Task type.
     * @param[in] str  The task argument.
     */
    Task(TaskType type, std::string str) : type_(type), str_(std::move(str)), num_(0)
    {
    }

    /**
     * @brief Construct a new Task object.
     * @param[in] type Task type.
     * @param[in] num The task argument.
     */
    Task(TaskType type, uint64_t num) : type_(type), str_(""), num_(num)
    {
    }

    ~Task() = default;

    /**
     * @brief Create task object from task string.
     * @param[in] taskStr The task string.
     * @param[out] task The task object.
     * @return Status of the call.
     */
    static Status ParseFromStr(const std::string &taskStr, std::unique_ptr<Task> &task);

    /**
     * @brief Check if the task can be executed
     * @return True can be executed.
     */
    bool IsExecutable();

    /**
     * @brief Run task.
     * @param[in] name Inject Point.
     * @param[out] value The output for return task.
     */
    void Run(const std::string &name, std::string &value);

    /**
     * @brief Get current task type.
     * @return TaskType.
     */
    TaskType GetType() const
    {
        return type_;
    }

private:
    TaskType type_;
    uint32_t freq_;
    std::atomic<uint64_t> count_;
    std::string str_;
    uint64_t num_;
};

/**
 * @brief Inject Point object.
 */
class InjectPoint {
public:
    /**
     * @brief Construct a new Inject Point object.
     * @param[in] actionStr The action string.
     * @param[in] tasks The task object list.
     */
    InjectPoint(std::string actionStr, std::vector<std::unique_ptr<Task>> tasks)
        : actionStr_(std::move(actionStr)), tasks_(std::move(tasks)), pause_(false)
    {
    }

    ~InjectPoint() = default;

    /**
     * @brief Create inject point object from action string.
     * @param[in] actionStr The action string.
     * @param[out] injectPoint The inject point object.
     * @return Status of the call.
     */
    static Status ParseFromStr(std::string actionStr, std::shared_ptr<InjectPoint> &injectPoint);

    /**
     * @brief Execute the task.
     * @param[in] name Inject point name.
     * @param[out] value The output value.
     * @return TaskType.
     */
    TaskType Execute(const std::string &name, std::string &value);

    /**
     * @brief Clear the pause status.
     */
    void Clear();

    /**
     * @brief Get the Action Str object
     * @return The action string.
     */
    const std::string &GetActionStr() const
    {
        return actionStr_;
    }

    /**
     * @brief Get the task count.
     * @return The task count.
     */
    size_t GetTaskCount() const
    {
        return tasks_.size();
    }

    /**
     * @brief Get the inject point execute count.
     * @return The inject point execute count.
     */
    uint64_t GetExecuteCount() const
    {
        return executeCount_;
    }

private:
    std::string actionStr_;
    std::vector<std::unique_ptr<Task>> tasks_;
    bool pause_;
    std::atomic<uint64_t> executeCount_{ 0 };
    std::mutex mutex_;
    std::condition_variable cv_;
};

/**
 * @brief Inject point manager object.
 */
class InjectPointManager {
public:
    /**
     * @brief Singleton mode, get InjectPointManager instance.
     * @return Reference of InjectPointManager.
     */
    static InjectPointManager &Instance();

    ~InjectPointManager();

    /**
     * @brief Get inject point object by name.
     * @param[in] name The inject point name.
     * @param[out] InjectPoint The inject point object.
     * @return True if injection point object found.
     */
    bool Get(const std::string &name, std::shared_ptr<InjectPoint> &injectPoint);

    /**
     * @brief Set inject point action.
     * @param[in] name The inject point name.
     * @param[in] action The action string.
     * @return Status of the call.
     */
    Status SetAction(const std::string &name, const std::string &action);

    /**
     * @brief Get the inject point execute count.
     * @param[in] name The inject point name.
     * @return The inject point execute count.
     */
    uint64_t GetExecuteCount(const std::string &name);

    /**
     * @brief Clear the inject point action.
     * @param name The inject point name.
     * @return Status of the call.
     */
    Status ClearAction(const std::string &name);

    /**
     * @brief Clear all inject point action.
     * @return Status of the call.
     */
    Status ClearAllAction();

private:
    InjectPointManager() = default;
    InjectPointManager(const InjectPointManager &) = delete;
    InjectPointManager &operator=(const InjectPointManager &) = delete;

    std::shared_timed_mutex mutex_;
    std::unordered_map<std::string, std::shared_ptr<InjectPoint>> injectPointMap_;
};

template <typename T>
class Handle {
public:
    constexpr Handle() = default;

    Handle(const Handle &) = default;
    Handle &operator=(const Handle &) = default;

    Handle(Handle &&other) noexcept = default;
    Handle &operator=(Handle &&) noexcept = default;

    template <typename... Args>
    explicit Handle(Args &&...args) : needReturn_(true), val_(std::forward<Args>(args)...)
    {
    }

    ~Handle() = default;

    T &Get()
    {
        return val_;
    }

    bool NeedReturn() const
    {
        return needReturn_;
    }

private:
    bool needReturn_ = false;
    T val_;
};

template <>
class Handle<void> {
public:
    explicit constexpr Handle(bool needReturn = false) : needReturn_(needReturn)
    {
    }

    ~Handle() = default;

    void Get()
    {
    }

    bool NeedReturn() const
    {
        return needReturn_;
    }

private:
    bool needReturn_;
};

/**
 * @brief Call inject function and return the handle object.
 * @tparam F Function type.
 * @tparam R The function return type.
 * @param[in] f The inject function and the return value is not void.
 * @param[int] param The parameter of the inject function.
 * @return Handle<R> The handle object.
 */
template <typename F, typename R = typename std::result_of<F(const std::string &)>::type,
          typename std::enable_if<!std::is_void<R>::value>::type* = nullptr>
auto ReturnHelper(F &&f, const std::string &param) -> Handle<R>
{
    return Handle<R>(f(param));
}

/**
 * @brief Call inject function and return the handle object.
 * @tparam F Function type.
 * @tparam R The function return type.
 * @param[in] f The inject function and the return value is void.
 * @param[int] param The parameter of the inject function.
 * @return Handle<R> The handle object.
 */
template <typename F, typename R = typename std::result_of<F(const std::string &)>::type,
          typename std::enable_if<std::is_void<R>::value>::type* = nullptr>
auto ReturnHelper(F &&f, const std::string &param) -> Handle<R>
{
    f(param);
    return Handle<R>(true);
}

/**
 * @brief Execute the inject point.
 * @tparam F Function type.
 * @param[in] lineOfCode The line of inject code.
 * @param[in] fileName The code file name.
 * @param[in] name The inject point name.
 * @param[in] f The inject action.
 * @return std::unique_ptr<typename std::result_of<F(const std::string &)>::type>
 * @note
 * @verbatim
 * Below inject point will call this function, so the closure
 * must return the same type with the injected function.
 * INJECT_POINT("point", [] (const std::string &arg) {return arg;});
 * @endverbatim
 */
template <typename F, typename R = typename std::result_of<F(const std::string &)>::type>
auto Execute(int lineOfCode, const std::string &fileName, const std::string &name, F &&f) -> Handle<R>
{
    std::shared_ptr<InjectPoint> injectPoint;
    if (!InjectPointManager::Instance().Get(name, injectPoint)) {
        return Handle<R>();
    }

    const size_t maxSize = 1024;
    std::string value;
    LOG(INFO) << "[Inject] Execute " << name << ", action \"" << injectPoint->GetActionStr().substr(0, maxSize)
              << "\", location:" << fileName << ":" << lineOfCode;

    auto taskType = injectPoint->Execute(name, value);
    if (taskType == TaskType::RETURN) {
        return ReturnHelper(f, value);
    } else if (taskType == TaskType::CALL) {
        f(value);
    }
    return Handle<R>();
}

/**
 * @brief Execute the inject action and return Status according to the argument of return task.
 * @param[in] lineOfCode The line of inject code.
 * @param[in] fileName The code file name.
 * @param[in] name The inject point name.
 * @return std::unique_ptr<Status>
 * @note
 * @verbatim
 * Below inject point will call this function, so the injected function must return Status.
 * INJECT_POINT("point");
 * @endverbatim
 */
Handle<Status> Execute(int lineOfCode, const std::string &fileName, const std::string &name);

/**
 * @brief Split string.
 * @param[in] param String to be split.
 * @return Split string vector
 */
std::vector<std::string> ParamSplit(const std::string &param);

/**
 * @brief Convert string parameter to target type.
 * @tparam T The target type.
 * @param[in] param The string parameter.
 * @return T The target value.
 */
template <class T>
T StringInto(const std::string &param);

template <>
inline int64_t StringInto<int64_t>(const std::string &param)
{
    return std::stoll(param);
}

template <>
inline std::string StringInto<std::string>(const std::string &param)
{
    return param;
}

template <typename F, class... Args, size_t... S>
auto CallFunctionImpl(F &&f, const std::vector<std::string> &params, std::index_sequence<S...>)
{
    return f(StringInto<Args>(params[S])...);
}

template <typename F, typename... Args>
auto CallFunction(const std::string &name, F &&f, const std::string &param) -> typename std::result_of<F(Args...)>::type
{
    static const size_t ARGS_SIZE = sizeof...(Args);
    auto params = ParamSplit(param);
    if (params.size() != ARGS_SIZE) {
        LOG(FATAL) << "Execute " << name << " failed, the action parameters (" << param
                   << ") not match with the inject function.";
    }
    return CallFunctionImpl<F, Args...>(std::move(f), params, std::make_index_sequence<ARGS_SIZE>{});
}

#define INJECT_PARAM_EXT(...)                                                                 \
    template <typename F>                                                                     \
    auto Execute(int lineOfCode, const std::string &fileName, const std::string &name, F &&f) \
        ->Handle<typename std::result_of<F(__VA_ARGS__)>::type>                              \
    {                                                                                         \
        return Execute(lineOfCode, fileName, name, [&name, &f](const std::string &arg) {      \
            return CallFunction<F, ##__VA_ARGS__>(name, std::move(f), arg);                   \
        });                                                                                   \
    }

/**
 * @brief Extend supported parameters for inject function.
 * INJECT_POINT("point", [] {return 0;});
 * INJECT_POINT("point", [](int64_t){return 0;});
 * INJECT_POINT("point", [](int64_t, int64_t){return 0;});
 * INJECT_POINT("point", [](int64_t, int64_t, int64_t, int64_t){return 0;});
 * INJECT_POINT("point", [](int64_t, std::string){return 0;});
 * INJECT_POINT("point", [](std::string, int64_t){return 0;});
 * INJECT_POINT("point", [](std::string, std::string){return 0;});
 */
INJECT_PARAM_EXT()
INJECT_PARAM_EXT(int64_t)
INJECT_PARAM_EXT(int64_t, int64_t)
INJECT_PARAM_EXT(int64_t, int64_t, int64_t, int64_t)
INJECT_PARAM_EXT(int64_t, std::string)
INJECT_PARAM_EXT(std::string, int64_t)
INJECT_PARAM_EXT(std::string, std::string)
INJECT_PARAM_EXT(std::string, std::string, std::string)

/**
 * @brief Set the inject point action.
 * @param[in] name The inject point name.
 * @param[in] action The action string. Format [freq%][count*]task[(arg)][-> more terms]
 * @return Status of the call.
 * @note
 * @verbatim
 * task can be one of:
 *   off     no action.
 *   return  call the specified function, and return at this inject point.
 *   call    call the specified function, but not return at this inject point.
 *   print   print message at this inject point.
 *   sleep   sleep the specified number of milliseconds.
 *   yield   thread yields the cpu when the inject point execute.
 *   busy    similar to sleep, but busy waits the cpu.
 *   pause   thread sleep at the inject point until the inject point is change or clear.
 *   abort   aborts the current process.
 * @endverbatim
 */
Status Set(const std::string &name, const std::string &action);

/**
 * @brief Get the inject point execute count.
 * @param[in] name The inject point name.
 * @return The inject point execute count.
 */
uint64_t GetExecuteCount(const std::string &name);

/**
 * @brief Clear the inject point action.
 * @param[in] name The inject point name.
 * @return Status of the call.
 */
Status Clear(const std::string &name);

/**
 * @brief Set inject actions.
 * @param[in] param The inject action string.
 * @return Status of the call.
 */
Status SetByString(const std::string &param);

/**
 * @brief Clear all inject point action.
 * @return Status of the call.
 */
Status ClearAll();

#define INJECT_POINT(...)                                                              \
    do {                                                                               \
        auto _handle = ::datasystem::inject::Execute(__LINE__, __FILE__, __VA_ARGS__); \
        if (_handle.NeedReturn()) {                                                    \
            return _handle.Get();                                                      \
        }                                                                              \
    } while (false)

#define INJECT_POINT_NO_RETURN(...)                                     \
    do {                                                                \
        ::datasystem::inject::Execute(__LINE__, __FILE__, __VA_ARGS__); \
    } while (false)

#else
#define INJECT_POINT(...)
#define INJECT_POINT_NO_RETURN(...)
#endif
}  // namespace inject
}  // namespace datasystem
#endif
