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
 * Description: Defines the worker class to communicate with the worker service.
 */
#include "datasystem/master/object_cache/master_worker_oc_local_api.h"

#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/common/util/gflag/common_gflags.h"
#include "datasystem/common/log/log_helper.h"
#include "datasystem/common/rpc/rpc_stub_base.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/worker/object_cache/master_worker_oc_service_impl.h"

namespace datasystem {
namespace master {
std::atomic<int64_t> MasterLocalWorkerOCApi::g_localTagGen_{1};
// MasterLocalWorkerOCApi methods

MasterLocalWorkerOCApi::MasterLocalWorkerOCApi(object_cache::MasterWorkerOCServiceImpl *service,
                                               const HostPort &localHostPort, std::shared_ptr<AkSkManager> akSkManager)
    : MasterWorkerOCApi(localHostPort, akSkManager), workerOC_(service)
{
}

Status MasterLocalWorkerOCApi::Init()
{
    RETURN_RUNTIME_ERROR_IF_NULL(workerOC_);
    return workerOC_->WaitWorkerOCServiceImplInit();
}

Status MasterLocalWorkerOCApi::PublishMeta(PublishMetaReqPb &req, PublishMetaRspPb &resp)
{
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    return workerOC_->PublishMeta(req, resp);
}

Status MasterLocalWorkerOCApi::UpdateNotification(UpdateObjectReqPb &req, UpdateObjectRspPb &rsp)
{
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    return workerOC_->UpdateNotification(req, rsp);
}

Status MasterLocalWorkerOCApi::DeleteNotification(std::unique_ptr<DeleteObjectReqPb> req, DeleteObjectRspPb &rsp)
{
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(*req));
    return workerOC_->DeleteNotification(*req, rsp);
}

Status MasterLocalWorkerOCApi::DeleteNotificationSend(std::unique_ptr<DeleteObjectReqPb> req, int64_t &tag)
{
    // The remote version of DeleteNotification is an async call that sends to remote location with a "send",
    // and then later it does a "receive" to get the response.
    // For the local same-node version, since there is no rpc layer it does not have any rpc "tag" and write/read
    // versions of the call (pre-packaged in the rpc layer).
    // To provide an equivalent behaviour to keep the calling code consistent, the async behaviour will be
    // simulated by postponing the execution of the local operation until the async recv call.
    // The logic is:
    // To simulate the rpc "send", it saves the request locally but does not execute it yet. This will be a quick
    // success call similar to an async send behaviour.
    // 1. store the request in localReqMap_ (simulates the "send");
    // 2. return immediately, giving the caller a tag;
    // 3. execute the real delete later, inside DeleteNotificationReceive,
    //    which pulls the request from the map and calls workerOC_->DeleteNotification.
    // A shared_mutex keeps concurrent Send/Receive operations on the same API
    // instance serialized.
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(*req));
    tag = g_localTagGen_.fetch_add(1);

    std::unique_lock<std::shared_mutex> lock(localReqMutex_);
    localReqMap_[tag] = std::move(req);
    return Status::OK();
}

Status MasterLocalWorkerOCApi::DeleteNotificationReceive(int64_t tag, DeleteObjectRspPb &rsp)
{
    std::unique_ptr<DeleteObjectReqPb> req;
    {
        std::unique_lock<std::shared_mutex> lock(localReqMutex_);
        auto it = localReqMap_.find(tag);
        if (it == localReqMap_.end() || !it->second) {
            RETURN_STATUS(StatusCode::K_NOT_FOUND, FormatString("Local tag %ld not found", tag));
        }
        req = std::move(it->second);
        localReqMap_.erase(it);
    }
    return workerOC_->DeleteNotification(*req, rsp);
}

Status MasterLocalWorkerOCApi::QueryGlobalRefNumOnWorker(QueryGlobalRefNumReqPb &req, QueryGlobalRefNumRspPb &rsp)
{
    req.set_timeout(MasterGetRequestTimeout(timeoutDuration.CalcRemainingTime()));
    LOG(INFO) << "QueryGlobalRefNumOnWorker " << masterHostPort_.ToString() << " : " << LogHelper::IgnoreSensitive(req);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    RETURN_IF_NOT_OK(workerOC_->QueryGlobalRefNumOnWorker(req, rsp));
    LOG(INFO) << "QueryGlobalRefNumOnWorker " << masterHostPort_.ToString() << " success."
              << LogHelper::IgnoreSensitive(rsp);
    return Status::OK();
}

Status MasterLocalWorkerOCApi::PushMetaToWorker(PushMetaToWorkerReqPb &req, PushMetaToWorkerRspPb &rsp)
{
    LOG(INFO) << "Send PushMetaToWorker to worker " << masterHostPort_.ToString() << " : "
              << LogHelper::IgnoreSensitive(req);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    RETURN_IF_NOT_OK(workerOC_->PushMetaToWorker(req, rsp));
    LOG(INFO) << "Send PushMetaToWorker to worker " << masterHostPort_.ToString() << " success."
              << LogHelper::IgnoreSensitive(rsp);
    return Status::OK();
}

Status MasterLocalWorkerOCApi::RequestMetaFromWorker(RequestMetaFromWorkerReqPb &req, RequestMetaFromWorkerRspPb &rsp)
{
    LOG(INFO) << "Send RequestMetaFromWorker to LOCAL worker.";
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    RETURN_IF_NOT_OK(workerOC_->RequestMetaFromWorker(req, rsp));
    LOG(INFO) << "Send RequestMetaFromWorker to LOCAL worker success." << LogHelper::IgnoreSensitive(rsp);
    return Status::OK();
}

Status MasterLocalWorkerOCApi::ChangePrimaryCopy(ChangePrimaryCopyReqPb &req, ChangePrimaryCopyRspPb &rsp)
{
    LOG(INFO) << "Send ChangePrimaryCopy to worker " << masterHostPort_.ToString() << " : "
              << LogHelper::IgnoreSensitive(req);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    RETURN_IF_NOT_OK(workerOC_->ChangePrimaryCopy(req, rsp));
    LOG(INFO) << "Send ChangePrimaryCopy to worker " << masterHostPort_.ToString() << " success."
              << LogHelper::IgnoreSensitive(rsp);
    return Status::OK();
}

Status MasterLocalWorkerOCApi::NotifyMasterIncNestedRefs(NotifyMasterIncNestedReqPb &req,
                                                         NotifyMasterIncNestedResPb &rsp)
{
    LOG(INFO) << "Send NotifyMasterIncNestedRefs to worker " << masterHostPort_.ToString() << " : "
              << LogHelper::IgnoreSensitive(req);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    RETURN_IF_NOT_OK(workerOC_->NotifyMasterIncNestedRefs(req, rsp));
    LOG(INFO) << "Send NotifyMasterIncNestedRefs to worker " << masterHostPort_.ToString() << " success."
              << LogHelper::IgnoreSensitive(rsp);
    return Status::OK();
}

Status MasterLocalWorkerOCApi::ClearData(ClearDataReqPb &req, ClearDataRspPb &rsp)
{
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    return workerOC_->ClearData(req, rsp);
}

Status MasterLocalWorkerOCApi::NotifyMasterDecNestedRefs(NotifyMasterDecNestedReqPb &req,
                                                         NotifyMasterDecNestedResPb &rsp)
{
    LOG(INFO) << "Send NotifyMasterDecNestedRefs to worker " << masterHostPort_.ToString() << " : "
              << LogHelper::IgnoreSensitive(req);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    RETURN_IF_NOT_OK(workerOC_->NotifyMasterDecNestedRefs(req, rsp));
    LOG(INFO) << "Send NotifyMasterDecNestedRefs to worker " << masterHostPort_.ToString() << " success."
              << LogHelper::IgnoreSensitive(rsp);
    return Status::OK();
}
}  // namespace master
}  // namespace datasystem
