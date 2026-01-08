/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: Defines the worker service Expire process, for set expiration time.
 */
#include "datasystem/worker/object_cache/service/worker_oc_service_expire_impl.h"

#include <cstdint>
#include <utility>

#include "datasystem/common/log/log.h"
#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/authenticate.h"

DS_DECLARE_string(other_cluster_names);
DS_DECLARE_string(cluster_name);
DS_DECLARE_bool(cross_cluster_get_data_from_worker);
DS_DECLARE_bool(cross_cluster_get_meta_from_worker);

using namespace datasystem::master;
namespace datasystem {
namespace object_cache {

WorkerOcServiceExpireImpl::WorkerOcServiceExpireImpl(WorkerOcServiceCrudParam &initParam, EtcdClusterManager *etcdCM,
                                                     std::shared_ptr<AkSkManager> akSkManager)
    : WorkerOcServiceCrudCommonApi(initParam), etcdCM_(etcdCM), akSkManager_(std::move(akSkManager))
{
    for (const auto &azName : Split(FLAGS_other_cluster_names, ",")) {
        if (azName != FLAGS_cluster_name) {
            otherAZNames_.emplace_back(azName);
        }
    }
}

Status WorkerOcServiceExpireImpl::Expire(const ExpireReqPb &req, ExpireRspPb &rsp)
{
    workerOperationTimeCost.Clear();
    Timer timer;
    int64_t realTimeoutMs = reqTimeoutDuration.CalcRealRemainingTime();
    AccessRecorder posixPoint(AccessRecorderKey::DS_POSIX_EXPIRE);
    RequestParam reqParam;
    reqParam.objectKey = objectKeysToString({ req.object_keys().begin(), req.object_keys().end() });
    LOG(INFO) << "Expire start from client:" << req.client_id();
    std::string tenantId;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker::Authenticate(akSkManager_, req, tenantId), "Authenticate failed.");
    auto objectKeys = TenantAuthManager::ConstructNamespaceUriWithTenantId(tenantId, req.object_keys());

    std::unordered_map<MetaAddrInfo, std::vector<std::string>> objKeysGrpByMaster;
    std::unordered_map<std::string, std::unordered_set<std::string>> objKeysUndecidedMaster;
    etcdCM_->GroupObjKeysByMasterHostPort(objectKeys, objKeysGrpByMaster, objKeysUndecidedMaster);

    std::unordered_set<std::string> objKeysExpireFailed;
    std::vector<std::string> absentObjectKeys;
    uint32_t ttlSeconds = req.ttl_second();
    std::vector<std::future<Status>> futures;
    std::string traceID = Trace::Instance().GetTraceID();
    Status rc;
    size_t threadNum = std::min<size_t>(objKeysGrpByMaster.size(), FLAGS_rpc_thread_num);
    auto batchExpireThreadPool_ = std::make_unique<ThreadPool>(1, threadNum, "BatchExpireMeta");
    futures.reserve(objKeysGrpByMaster.size());
    for (auto &item : objKeysGrpByMaster) {
        futures.emplace_back(batchExpireThreadPool_->Submit([&, item, timer]() {
            TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
            int64_t elapsed = static_cast<int64_t>(timer.ElapsedMilliSecond());
            reqTimeoutDuration.Init(realTimeoutMs - elapsed);

            HostPort workerAddr = item.first.GetAddressAndSaveDbName();
            const std::vector<std::string> &currentIds = item.second;
            rc = ExpireFromMaster(currentIds, workerAddr, ttlSeconds, absentObjectKeys, objKeysExpireFailed, rsp);
            if (rc.IsError()) {
                posixPoint.Record(rc.GetCode(), std::to_string(0), reqParam, rc.GetMsg());
                return rc;
            }
            return Status::OK();
        }));
    }

    for (auto &func : futures) {
        func.wait();
        RETURN_IF_NOT_OK(func.get());
    }
    for (const auto &kv : objKeysUndecidedMaster) {
        absentObjectKeys.insert(absentObjectKeys.end(), kv.second.begin(), kv.second.end());
    }

    std::unordered_set<std::string> objectKeysMayInOtherAz;
    if (!absentObjectKeys.empty() &&
        FLAGS_cross_cluster_get_data_from_worker &&
        FLAGS_cross_cluster_get_meta_from_worker) {
        rc = TryExpireObjKeyFromOtherAZ(objectKeysMayInOtherAz, ttlSeconds, absentObjectKeys, objKeysExpireFailed, rsp);
    }
    objKeysExpireFailed.insert(absentObjectKeys.begin(), absentObjectKeys.end());
    *rsp.mutable_failed_object_keys() = { objKeysExpireFailed.begin(), objKeysExpireFailed.end() };
    posixPoint.Record(rc.GetCode(), std::to_string(0), reqParam, rc.GetMsg());
    workerOperationTimeCost.Append("Total Expire", static_cast<int64_t>(timer.ElapsedMilliSecond()));
    LOG(INFO) << FormatString("The operations of Expire %s", workerOperationTimeCost.GetInfo());
    return rc;
}

Status WorkerOcServiceExpireImpl::TryExpireObjKeyFromOtherAZ(std::unordered_set<std::string> objectKeys,
                                                             uint32_t ttlSeconds, std::vector<std::string> &absentObj,
                                                             std::unordered_set<std::string> &objExpireFailed,
                                                             ExpireRspPb &rsp)
{
    for (auto iter = absentObj.begin(); iter != absentObj.end();) {
        if (!HasWorkerId(*iter)) {
            objectKeys.insert(std::move(*iter));
            iter = absentObj.erase(iter);
        } else {
            ++iter;
        }
    }
    RETURN_OK_IF_TRUE(objectKeys.empty());
    LOG(INFO) << "Try expire some miss objs from other az: " << VectorToString(objectKeys);
    for (const auto &otherAZName : otherAZNames_) {
        for (const auto &objectKey : objectKeys) {
            MetaAddrInfo metaAddrInfo;
            auto rc = etcdCM_->QueryMasterAddrInOtherAz(otherAZName, objectKey, metaAddrInfo);
            if (rc.IsError()) {
                LOG(WARNING) << "QueryMasterAddrInOtherAz failed, msg: " << rc.ToString();
                continue;
            }
            auto masterAddr = metaAddrInfo.GetAddressAndSaveDbName();
            RETURN_IF_NOT_OK(ExpireFromMaster({ objectKey }, masterAddr, ttlSeconds, absentObj, objExpireFailed, rsp));
            objectKeys.erase(objectKey);
        }
    }
    return Status::OK();
}

Status WorkerOcServiceExpireImpl::ExpireFromMaster(std::vector<std::string> objectKeys, const HostPort &masterAddr,
                                                   uint32_t ttlSeconds, std::vector<std::string> &absentObj,
                                                   std::unordered_set<std::string> &objExpireFailed, ExpireRspPb &rsp)
{
    master::ExpireReqPb req;
    master::ExpireRspPb resp;
    *req.mutable_object_keys() = { objectKeys.begin(), objectKeys.end() };
    req.set_redirect(true);
    req.set_ttl_second(ttlSeconds);
    INJECT_POINT("worker.expire_failed");
    auto workerMasterApi = workerMasterApiManager_->GetWorkerMasterApi(masterAddr);
    CHECK_FAIL_RETURN_STATUS(workerMasterApi != nullptr, K_RUNTIME_ERROR, "hash master get failed, Exipire failed");
    std::function<Status(master::ExpireReqPb &, master::ExpireRspPb &)> func =
        [&workerMasterApi](master::ExpireReqPb &req, master::ExpireRspPb &resp) {
            return workerMasterApi->Expire(req, resp);
        };

    auto rc = RedirectRetryWhenMetasMoving(req, resp, func);
    if (rc.IsError()) {
        LOG(WARNING) << "Expire meta from master[" << masterAddr.ToString() << "] failed, msg: " << rc.ToString();
        rsp.mutable_last_rc()->set_error_code(rc.GetCode());
        rsp.mutable_last_rc()->set_error_msg(rc.GetMsg());
        objExpireFailed.insert(objectKeys.begin(), objectKeys.end());
        return Status::OK();
    }
    absentObj.insert(absentObj.end(), resp.absent_object_keys().begin(), resp.absent_object_keys().end());
    objExpireFailed.insert(resp.failed_object_keys().begin(), resp.failed_object_keys().end());
    *rsp.mutable_last_rc() = resp.last_rc();
    RETURN_OK_IF_TRUE(resp.info().empty());
    for (const auto &redirectInfo : resp.info()) {
        master::ExpireReqPb redirectReq;
        master::ExpireRspPb redirectRsp;
        *redirectReq.mutable_object_keys() = { redirectInfo.change_meta_ids().begin(),
                                                redirectInfo.change_meta_ids().end() };
        redirectReq.set_ttl_second(ttlSeconds);
        HostPort redirectMasterAddr;
        RETURN_IF_NOT_OK(GetPrimaryReplicaAddr(redirectInfo.redirect_meta_address(), redirectMasterAddr));
        auto redirectWorkerMasterApi = workerMasterApiManager_->GetWorkerMasterApi(redirectMasterAddr);
        CHECK_FAIL_RETURN_STATUS(redirectWorkerMasterApi != nullptr, K_RUNTIME_ERROR,
                                    "hash master get failed, ExpireFromMaster failed");
        rc = redirectWorkerMasterApi->Expire(redirectReq, redirectRsp);
        if (rc.IsError()) {
            LOG(WARNING) << "Expire meta from master[" << masterAddr.ToString()
                            << "] failed, msg: " << rc.ToString();
            rsp.mutable_last_rc()->set_error_code(rc.GetCode());
            rsp.mutable_last_rc()->set_error_msg(rc.GetMsg());
            objExpireFailed.insert(redirectReq.object_keys().begin(), redirectReq.object_keys().end());
            continue;
        }
        absentObj.insert(absentObj.end(), redirectRsp.absent_object_keys().begin(),
                            redirectRsp.absent_object_keys().end());
        objExpireFailed.insert(redirectRsp.failed_object_keys().begin(), redirectRsp.failed_object_keys().end());
        *rsp.mutable_last_rc() = redirectRsp.last_rc();
    }
    return Status::OK();
}

}  // namespace object_cache
}  // namespace datasystem
