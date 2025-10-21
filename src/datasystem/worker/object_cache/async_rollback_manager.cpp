/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: Implementation of AsyncRollbackManager.
 */
#include "datasystem/worker/object_cache/async_rollback_manager.h"

#include <memory>
#include <mutex>
#include <unordered_map>

#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"

namespace datasystem {
namespace object_cache {

using namespace datasystem::worker;
using namespace datasystem::master;

static constexpr int ROLLBACK_INTERNAL_MS = 100;
static constexpr int ASYNC_ROLLBACK_RPC_TIMEOUT = 10'000;

AsyncRollbackManager::~AsyncRollbackManager()
{
    LOG(INFO) << "AsyncRollbackManager exit";
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        if (!pendingObject_.empty() || !processingObject_.empty()) {
            LOG(WARNING) << "There is still data that has not been rolled back, size: "
                         << pendingObject_.size() + processingObject_.size();
        }
    }
    running_ = false;
    emptyCond_.notify_all();
    if (thread_.joinable()) {
        thread_.join();
    }
}

void AsyncRollbackManager::Init(
    HostPort &localAddress,
    std::shared_ptr<WorkerMasterApiManagerBase<WorkerMasterOCApi>> apiManager,
    EtcdClusterManager *etcdCM)
{
    localAddress_ = localAddress;
    apiManager_ = std::move(apiManager);
    etcdCM_ = etcdCM;
    running_ = true;
    thread_ = Thread(&AsyncRollbackManager::Rollback, this);
}

void AsyncRollbackManager::RollbackToMaster(const std::vector<std::string> &objs, std::vector<std::string> &failedObjs)
{
    INJECT_POINT("AsyncRollbackToMaster.delay", [](uint32_t sec) { sleep(sec); });
    LOG(INFO) << "Start async rollback objectKeys: " << VectorToString(objs);
    std::unordered_map<std::string, std::vector<std::string>> objGroup;
    for (const auto &obj : objs) {
        std::shared_ptr<WorkerMasterOCApi> api;
        if (apiManager_->GetWorkerMasterApi(obj, etcdCM_, api).IsError()) {
            failedObjs.emplace_back(obj);
            continue;
        }
        objGroup[api->GetHostPort()].emplace_back(obj);
    }
    for (const auto &[addr, objs] : objGroup) {
        if (!running_) {
            return;
        }
        HostPort masterAddr;
        if (masterAddr.ParseString(addr).IsError()) {
            LOG(WARNING) << "Parse master address failed, address: " << addr;
            std::move(objs.begin(), objs.end(), std::back_inserter(failedObjs));
            continue;
        }
        std::shared_ptr<WorkerMasterOCApi> api;
        Status rc = apiManager_->GetWorkerMasterApi(masterAddr, api);
        if (rc.IsError()) {
            LOG(WARNING) << "GetWorkerMasterApiByAddr failed, rc: " << rc.ToString();
            std::move(objs.begin(), objs.end(), std::back_inserter(failedObjs));
            continue;
        }
        RollbackMultiMetaReqPb req;
        req.set_address(localAddress_.ToString());
        req.set_redirect(true);
        for (auto &obj : objs) {
            req.add_object_keys(obj);
        }
        RollbackMultiMetaRspPb rsp;
        reqTimeoutDuration.Init(ASYNC_ROLLBACK_RPC_TIMEOUT);
        rc = api->RollbackMultiMeta(req, rsp);
        if (rc.IsError() || !rsp.info().empty()) {
            LOG(WARNING) << "RollbackMultiMeta failed, meta maybe migrating, rc: " << rc.ToString();
            std::move(objs.begin(), objs.end(), std::back_inserter(failedObjs));
            continue;
        }
        std::move(rsp.failed_object_keys().begin(), rsp.failed_object_keys().end(), std::back_inserter(failedObjs));
    }
}

void AsyncRollbackManager::Rollback()
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    while (running_) {
        std::vector<std::string> objs;
        {
            std::unique_lock<std::shared_mutex> lock(mutex_);
            emptyCond_.wait(lock,
                            [this]() { return !pendingObject_.empty() || !processingObject_.empty() || !running_; });
            if (!running_) {
                return;
            }
            processingObject_.insert(pendingObject_.begin(), pendingObject_.end());
            pendingObject_.clear();
            for (const auto &obj : processingObject_) {
                objs.emplace_back(obj);
            }
        }
        std::vector<std::string> failedObjs;
        RollbackToMaster(objs, failedObjs);
        {
            std::lock_guard<std::shared_mutex> lock(mutex_);
            processingObject_.clear();
            processingObject_.insert(failedObjs.begin(), failedObjs.end());
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(ROLLBACK_INTERNAL_MS));
    }
}
}  // namespace object_cache
}  // namespace datasystem