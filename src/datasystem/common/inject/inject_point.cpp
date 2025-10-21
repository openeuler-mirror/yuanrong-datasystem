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
#include "datasystem/common/inject/inject_point.h"

#include <cstdlib>
#include <random>
#include <thread>
#include "datasystem/common/util/net_util.h"

namespace datasystem {
namespace inject {
#ifdef WITH_TESTS
constexpr uint32_t HUNDRED_PERCENT = 100;
constexpr uint32_t PARENTHESES_LEN = 2;

void Task::Run(const std::string &name, std::string &value)
{
    switch (type_) {
        case TaskType::RETURN:  // no break
        case TaskType::CALL:
            value = str_;
            break;
        case TaskType::PRINT:
            LOG(INFO) << "[Inject] " << name << " print:" << str_;
            break;
        case TaskType::SLEEP:
            std::this_thread::sleep_for(std::chrono::milliseconds(num_));
            break;
        case TaskType::YIELD:
            std::this_thread::yield();
            break;
        case TaskType::BUSY: {
            auto timeOut = std::chrono::steady_clock::now() + std::chrono::milliseconds(num_);
            while (std::chrono::steady_clock::now() < timeOut) {
            }
            break;
        }

        case TaskType::ABORT:
            LOG(ERROR) << "[Inject] " << name << " abort." << str_;
            std::abort();
            break;

        case TaskType::OFF:    // no break
        case TaskType::NONE:   // no break
        case TaskType::PAUSE:  // no break
        default:
            break;
    };
}

namespace {
Status StrToULL(const std::string &str, uint64_t &num)
{
    try {
        num = StrToUnsignedLongLong(str);
    } catch (std::invalid_argument &invalidArgument) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Parse number failed, invalid argument");
    } catch (std::out_of_range &outOfRange) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Parse number failed, out of range");
    }
    return Status::OK();
}
Status StrToInt(const std::string &str, uint32_t &num)
{
    int tempNum;
    CHECK_FAIL_RETURN_STATUS(StringToInt(str, tempNum), K_RUNTIME_ERROR, "Str to int failed");
    CHECK_FAIL_RETURN_STATUS(tempNum >= 0, K_RUNTIME_ERROR, "Negative number can't be converted to unsigned number");
    num = static_cast<uint32_t>(tempNum);
    return Status::OK();
}

Status CreateTask(std::string taskType, std::string argument, std::unique_ptr<Task> &task)
{
    if (taskType == "off") {
        task = std::make_unique<Task>(TaskType::OFF);
    } else if (taskType == "return") {
        task = std::make_unique<Task>(TaskType::RETURN, std::move(argument));
    } else if (taskType == "call") {
        task = std::make_unique<Task>(TaskType::CALL, std::move(argument));
    } else if (taskType == "print") {
        task = std::make_unique<Task>(TaskType::PRINT, std::move(argument));
    } else if (taskType == "sleep") {
        uint64_t ms = 0;
        RETURN_IF_NOT_OK_APPEND_MSG(StrToULL(argument, ms), "invalid sleep argument");
        task = std::make_unique<Task>(TaskType::SLEEP, ms);
    } else if (taskType == "yield") {
        task = std::make_unique<Task>(TaskType::YIELD);
    } else if (taskType == "busy") {
        uint64_t ms = 0;
        RETURN_IF_NOT_OK_APPEND_MSG(StrToULL(argument, ms), "invalid busy argument");
        task = std::make_unique<Task>(TaskType::BUSY, ms);
    } else if (taskType == "pause") {
        task = std::make_unique<Task>(TaskType::PAUSE);
    } else if (taskType == "abort") {
        task = std::make_unique<Task>(TaskType::ABORT, std::move(argument));
    } else {
        RETURN_STATUS(StatusCode::K_INVALID, "Invalid task type [\" + taskType + \"].");
    }
    return Status::OK();
}
}  // namespace

Status Task::ParseFromStr(const std::string &taskStr, std::unique_ptr<Task> &task)
{
    // Format 50%10*return(10).
    size_t pos = 0;
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!taskStr.empty(), K_RUNTIME_ERROR, "empty task str");

    // Parse frequency.
    auto size = taskStr.size();
    auto index = taskStr.find('%', pos);
    uint32_t freq = HUNDRED_PERCENT;
    if (index != std::string::npos) {
        RETURN_IF_NOT_OK_APPEND_MSG(StrToInt(taskStr.substr(pos, index - pos), freq), "invalid frequency");
        pos = index + 1;
    }

    // Parse count.
    index = taskStr.find('*', pos);
    uint64_t count = std::numeric_limits<uint64_t>::max();
    if (index != std::string::npos) {
        RETURN_IF_NOT_OK_APPEND_MSG(StrToULL(taskStr.substr(pos, index - pos), count), "invalid count");
        pos = index + 1;
    }

    // Parse argument.
    index = taskStr.find('(', pos);
    std::string argument;
    auto taskTypeLen = std::string::npos;
    if (index != std::string::npos) {
        if (taskStr[size - 1] == ')') {
            argument = taskStr.substr(index + 1, size - index - PARENTHESES_LEN);
        } else {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "parentheses do not match!");
        }
        taskTypeLen = index - pos;
    }

    auto typeStr = taskStr.substr(pos, taskTypeLen);
    RETURN_IF_NOT_OK(CreateTask(typeStr, std::move(argument), task));
    task->freq_ = freq;
    task->count_ = count;
    return Status::OK();
}

bool Task::IsExecutable()
{
    uint64_t cnt = count_.load(std::memory_order_acquire);
    if (freq_ < HUNDRED_PERCENT && cnt != 0) {
        std::random_device rd;
        if (rd() % HUNDRED_PERCENT >= freq_) {
            return false;
        }
        cnt = count_.load(std::memory_order_acquire);
    }

    if (cnt != std::numeric_limits<uint64_t>::max()) {
        // Make sure only one thread can decrease the count to 0.
        while (true) {
            if (cnt == 0) {
                return false;
            }
            auto newCnt = cnt - 1;
            // Retry if other thread change the count.
            if (count_.compare_exchange_weak(cnt, newCnt, std::memory_order_release, std::memory_order_acquire)) {
                break;
            }
        }
    }
    return true;
}

Status InjectPoint::ParseFromStr(std::string actionStr, std::shared_ptr<InjectPoint> &injectPoint)
{
    auto taskStrList = Split(actionStr, "->");
    std::vector<std::unique_ptr<Task>> tasks;
    for (const auto &taskStr : taskStrList) {
        std::unique_ptr<Task> task;
        RETURN_IF_NOT_OK(Task::ParseFromStr(taskStr, task));
        tasks.emplace_back(std::move(task));
    }

    injectPoint = std::make_shared<InjectPoint>(actionStr, std::move(tasks));
    return Status::OK();
}

TaskType InjectPoint::Execute(const std::string &name, std::string &value)
{
    Task *taskPtr = nullptr;
    for (auto &task : tasks_) {
        if (task->IsExecutable()) {
            taskPtr = task.get();
            break;
        }
    }
    if (taskPtr == nullptr) {
        return TaskType::NONE;
    }
    executeCount_.fetch_add(1, std::memory_order_relaxed);
    auto taskType = taskPtr->GetType();
    if (taskType == TaskType::PAUSE) {
        std::unique_lock<std::mutex> locker(mutex_);
        pause_ = true;
        LOG(INFO) << "[Inject] Execute " << name << ",thread pause!";
        cv_.wait(locker, [this] { return !pause_; });
        LOG(INFO) << "[Inject] Execute " << name << ",thread running!";
        return taskType;
    }

    // Execute task.
    taskPtr->Run(name, value);
    return taskType;
}

void InjectPoint::Clear()
{
    std::unique_lock<std::mutex> locker(mutex_);
    pause_ = false;
    cv_.notify_all();
}

InjectPointManager &InjectPointManager::Instance()
{
    static InjectPointManager manager;
    return manager;
}

InjectPointManager::~InjectPointManager()
{
    ClearAllAction();
}

bool InjectPointManager::Get(const std::string &name, std::shared_ptr<InjectPoint> &injectPoint)
{
    std::shared_lock<std::shared_timed_mutex> locker(mutex_);
    auto it = injectPointMap_.find(name);
    if (it == injectPointMap_.end()) {
        return false;
    }
    injectPoint = it->second;
    return true;
}

Status InjectPointManager::SetAction(const std::string &name, const std::string &actionStr)
{
    std::shared_ptr<InjectPoint> injectPoint;
    RETURN_IF_NOT_OK(InjectPoint::ParseFromStr(actionStr, injectPoint));

    std::unique_lock<std::shared_timed_mutex> locker(mutex_);
    auto it = injectPointMap_.find(name);
    if (it == injectPointMap_.end()) {
        auto ret = injectPointMap_.emplace(name, std::move(injectPoint));
        if (!ret.second) {
            RETURN_STATUS(K_RUNTIME_ERROR, "SetAction failed.");
        }
    } else {
        // Clear the pause status in case of other thread still wait for it.
        it->second->Clear();
        it->second = injectPoint;
    }
    return Status::OK();
}

uint64_t InjectPointManager::GetExecuteCount(const std::string &name)
{
    std::shared_ptr<InjectPoint> injectPoint;
    if (!Get(name, injectPoint)) {
        return 0;
    }
    return injectPoint->GetExecuteCount();
}

Status InjectPointManager::ClearAction(const std::string &name)
{
    std::unique_lock<std::shared_timed_mutex> locker(mutex_);
    auto it = injectPointMap_.find(name);
    if (it != injectPointMap_.end()) {
        it->second->Clear();
        injectPointMap_.erase(name);
    }
    return Status::OK();
}

Status InjectPointManager::ClearAllAction()
{
    std::unique_lock<std::shared_timed_mutex> locker(mutex_);
    for (auto &kv : injectPointMap_) {
        kv.second->Clear();
    }
    injectPointMap_.clear();
    return Status::OK();
}

Handle<Status> Execute(int lineOfCode, const std::string &fileName, const std::string &name)
{
    return Execute(lineOfCode, fileName, name, [lineOfCode, &fileName, &name](const std::string &codeName) {
        return Status(GetStatusCodeByName(codeName), lineOfCode, fileName, "Status inject by " + name);
    });
}

std::vector<std::string> ParamSplit(const std::string &param)
{
    if (param.empty()) {
        return {};
    }
    return Split(param, ",");
}

Status Set(const std::string &name, const std::string &action)
{
    return InjectPointManager::Instance().SetAction(name, action);
}

uint64_t GetExecuteCount(const std::string &name)
{
    return InjectPointManager::Instance().GetExecuteCount(name);
}

Status Clear(const std::string &name)
{
    return InjectPointManager::Instance().ClearAction(name);
}

Status SetByString(const std::string &param)
{
    LOG(INFO) << "set inject actions:" << param;
    if (param.empty()) {
        return Status::OK();
    }
    std::vector<std::string> items = Split(param, ";");
    for (const auto &item : items) {
        auto index = item.find(':');
        CHECK_FAIL_RETURN_STATUS(index != std::string::npos, K_INVALID, "invalid inject string: " + item);
        auto name = item.substr(0, index);
        auto action = item.substr(index + 1);
        RETURN_IF_NOT_OK(Set(name, action));
    }
    return Status::OK();
}

Status ClearAll()
{
    return InjectPointManager::Instance().ClearAllAction();
}
#endif
}  // namespace inject
}  // namespace datasystem
