/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
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

#include "datasystem/client/object_cache/device/hccl_comm_magr.h"

#include "datasystem/client/object_cache/device/hccl_comm_factory.h"

namespace datasystem {

ThreadCommRecord::ThreadCommRecord()
{
    pool_ = std::make_shared<ThreadPool>(1);
}

size_t ThreadCommRecord::GetThreadLoadScore()
{
    return pool_->GetWaitingTasksNum() + threadCommIds_.size();
}

bool ThreadCommRecord::CheckCommConflict(const std::string &waitCheckConflictCommId)
{
    if (threadCommIds_.find(waitCheckConflictCommId) != threadCommIds_.end()) {
        return true;
    }
    return false;
}

void ThreadCommRecord::AddNewCommIdRecord(const std::string &commId)
{
    (void)threadCommIds_.insert(commId);
}

std::shared_ptr<ThreadPool> ThreadCommRecord::GetThreadPool()
{
    return pool_;
}
void ThreadCommRecord::RemoveCommIdRecord(const std::string &commId)
{
    (void)threadCommIds_.erase(commId);
}

HcclCommMagr::HcclCommMagr()
{
    int minThreadPoolSzie = 2;
    if (THREADPOOL_SIZE < minThreadPoolSzie) {
        LOG(ERROR) << "The maximum number of HCCL threads must be greater than 2. Otherwise, thread deadlock may "
                      "occur. Currently, the maximum number of HCCL threads is "
                   << THREADPOOL_SIZE;
    }
}

std::tuple<int, std::shared_ptr<ThreadPool>> HcclCommMagr::AssignThreadToComm(const std::string &commId)
{
    std::string negDirectionCommId = FindNegDirectionCommKey(commId);
    if (negDirectionCommId.empty()) {
        return { -1, nullptr };
    }

    std::vector<std::tuple<int, uint64_t>> scoreList;
    GetThreadPoolBusyScore(scoreList);

    int chosenThreadId = GetOptimalThreadId(scoreList, negDirectionCommId);
    UpdateThreadDetailRecord(chosenThreadId, commId);

    LOG(INFO) << "The commId: " << commId << " select tid: " << chosenThreadId;
    return GetThreadPool(chosenThreadId);
}

Status HcclCommMagr::RemoveThreadPoolCommRecord(int tid, const std::string &commId)
{
    if (!(tid >= 0 && tid < THREADPOOL_SIZE)) {
        std::string errormsg =
            "The input tid " + std::to_string(tid) + " is not between 0 to " + std::to_string(THREADPOOL_SIZE);
        LOG(ERROR) << errormsg;
        return Status(K_RUNTIME_ERROR, errormsg);
    }
    TbbThreadDetailsTable::accessor acc;
    if (threadDetails_.find(acc, tid) && acc->second != nullptr) {
        acc->second->RemoveCommIdRecord(commId);
    }
    return Status::OK();
}

std::tuple<int, std::shared_ptr<ThreadPool>> HcclCommMagr::GetThreadPool(int threadId)
{
    TbbThreadDetailsTable::const_accessor acc;
    if (threadDetails_.find(acc, threadId)) {
        return { threadId, acc->second->GetThreadPool() };
    }
    return { -1, nullptr };
}

std::string HcclCommMagr::FindNegDirectionCommKey(const std::string &commId)
{
    size_t minCommIdLength = 2;
    if (commId.length() < minCommIdLength) {
        LOG(ERROR) << "CommId is empty or CommId length less than 2";
        return "";
    }

    std::string commTag = commId.substr(1);
    std::string commWaitCheckType = commId.substr(0, 1);
    if (commWaitCheckType != "0" && commWaitCheckType != "1") {
        LOG(ERROR) << "The P2PEventType part of commId is error with " << commWaitCheckType;
        return "";
    }
    
    P2PEventType commDirection = static_cast<P2PEventType>(std::stoi(commWaitCheckType));
    P2PEventType commNegDirection = (commDirection == P2PEventType::SEND) ? P2PEventType::RECV : P2PEventType::SEND;
    return std::to_string(static_cast<int>(commNegDirection)) + commTag;
}

void HcclCommMagr::GetThreadPoolBusyScore(std::vector<std::tuple<int, uint64_t>> &scoreList)
{
    scoreList.reserve(THREADPOOL_SIZE);
    // Stuff all scores into the array first
    TbbThreadDetailsTable::const_accessor threadControlAcc;

    for (int i = 0; i != THREADPOOL_SIZE; i++) {
        if (threadDetails_.find(threadControlAcc, i) && threadControlAcc->second != nullptr) {
            auto currentScore = threadControlAcc->second->GetThreadLoadScore();
            // The value true indicates that a thread exists.
            scoreList.push_back({ i, currentScore });
        } else {
            scoreList.push_back({ i, 0 });
        }
    }
    // Sort from Low to High
    std::sort(scoreList.begin(), scoreList.end(),
              [](const std::tuple<int, uint64_t> &threadScoreA, const std::tuple<int, uint64_t> &threadScoreB) {
                  return std::get<1>(threadScoreA) < std::get<1>(threadScoreB);
              });
}

int HcclCommMagr::GetOptimalThreadId(const std::vector<std::tuple<int, uint64_t>> &scoreList,
                                     const std::string &negDirectionCommId)
{
    // Choose the smallest
    int threadId = std::get<0>(scoreList[0]);
    auto injectTest = [&threadId] {
        INJECT_POINT("client.GetFreeTaskThreadpool.SetThreadId", [&threadId](int setThreadId) {
            LOG(INFO) << "client.GetFreeTaskThreadpool.SetThreadId set the threadId = " << setThreadId;
            threadId = setThreadId;
        });
    };
    injectTest();

    TbbThreadDetailsTable::accessor threadControlAcc;
    if (threadDetails_.find(threadControlAcc, threadId) && threadControlAcc->second != nullptr) {
        if (threadControlAcc->second->CheckCommConflict(negDirectionCommId)) {
            threadId = std::get<0>(scoreList[1]);
            return threadId;
        }
    }
    return threadId;
}

void HcclCommMagr::UpdateThreadDetailRecord(int threadId, const std::string &commId)
{
    TbbThreadDetailsTable::accessor threadControlAcc;
    if (threadDetails_.find(threadControlAcc, threadId) && (threadControlAcc->second != nullptr)) {
        (void)threadControlAcc->second->AddNewCommIdRecord(commId);
        return;
    }
    (void)threadDetails_.insert(threadControlAcc, threadId);
    threadControlAcc->second = std::make_shared<ThreadCommRecord>();
    threadControlAcc->second->AddNewCommIdRecord(commId);
}
}  // namespace datasystem