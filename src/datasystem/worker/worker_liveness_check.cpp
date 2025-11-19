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

#include "datasystem/worker/worker_liveness_check.h"

#include <fstream>
#include <memory>
#include <vector>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/common/rpc/rpc_credential.h"
#include "datasystem/common/rpc/rpc_options.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/protos/object_posix.pb.h"
#include "datasystem/protos/share_memory.pb.h"
#include "datasystem/common/rpc/rpc_auth_keys.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/worker_oc_server.h"

DS_DECLARE_bool(enable_distributed_master);
DS_DECLARE_string(master_address);
DS_DECLARE_uint32(node_timeout_s);
DS_DECLARE_int32(sc_regular_socket_num);
DS_DECLARE_int32(sc_stream_socket_num);

namespace datasystem {
namespace worker {
WorkerLivenessCheck::WorkerLivenessCheck(WorkerOCServer *workerOcServer, std::string probeFileName,
                                         uint32_t probeTimeoutMs, HostPort hostPort, std::string workerUuid,
                                         std::shared_ptr<AkSkManager> akSkManager)
    : workerOcServer_(workerOcServer),
      probeFileName_(std::move(probeFileName)),
      probeTimeoutMs_(probeTimeoutMs),
      policies_{ { "CheckWorkerServices", std::bind(&WorkerLivenessCheck::CheckWorkerServices, this) } },
      hostPort_(std::move(hostPort)),
      workerUuid_(std::move(workerUuid)),
      akSkManager_(std::move(akSkManager))
{
    if (IsMasterNode()) {
        policies_.emplace_back(
            Policy{ "CheckRocksDbService", std::bind(&WorkerLivenessCheck::CheckRocksDbService, this) });
    }
}

inline bool EnableSCService()
{
    return FLAGS_sc_regular_socket_num > 0 && FLAGS_sc_stream_socket_num > 0;
}

Status WorkerLivenessCheck::Init()
{
    livenessKey_ = FormatString("liveness-%s;%s", GetStringUuid(), workerUuid_);
    LOG(INFO) << "liveness probe key: " << livenessKey_;
    checkThread_ = std::make_unique<Thread>(&WorkerLivenessCheck::Run, this);
    checkThread_->set_name("livenessCheck");
    servicesNames_.emplace_back("WorkerOCService");
    servicesNames_.emplace_back("WorkerService");
    servicesNames_.emplace_back("WorkerWorkerOCService");
    servicesNames_.emplace_back("MasterWorkerOCService");
    if (IsMasterNode()) {
        servicesNames_.emplace_back("MasterOCService");
    }
    if (EnableSCService()) {
        servicesNames_.emplace_back("ClientWorkerSCService");
        servicesNames_.emplace_back("WorkerWorkerSCService");
        servicesNames_.emplace_back("MasterWorkerSCService");
        if (IsMasterNode()) {
            servicesNames_.emplace_back("MasterSCService");
        }
    }
    LivenessHealthCheckEvent::GetInstance().AddSubscriber(
        "WORKER_LIVENESS_CHECK",
        [this](Timer &timer, const Status &lastStatus) { return CheckLivenessProbeFile(timer, lastStatus); });
    return Status::OK();
}

void WorkerLivenessCheck::Stop()
{
    if (checkThread_ == nullptr) {
        return;
    }
    LOG(INFO) << "Stop WorkerLivenessCheck and try delete liveness probe file.";
    LOG_IF_ERROR(ResetLivenessProbe(), "ResetLivenessProbe failed");
    exitFlag_ = true;
    checkThread_->join();
    checkThread_.reset();
}

WorkerLivenessCheck::~WorkerLivenessCheck()
{
    LivenessHealthCheckEvent::GetInstance().RemoveSubscriber("WORKER_LIVENESS_CHECK");
    Stop();
}

void WorkerLivenessCheck::Run()
{
    LOG(INFO) << "Start worker liveiness check thread.";
    const uint32_t intervalMs = 100;             // short interval for quick shutdown.
    const uint32_t minProbeCount = 3;            // at lease probe 3 times.
    const uint32_t maxProbeIntervalMs = 10'000;  // max probe interval in ms.
    uint32_t probeIntervalMs =
        std::min({ probeTimeoutMs_ / minProbeCount, maxProbeIntervalMs, FLAGS_node_timeout_s / minProbeCount });
    Timer timer;
    Timer failedTimer;
    Status rc;
    bool logFlag = false;
    bool isFirst = true;
    while (!exitFlag_) {
        TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
        INJECT_POINT("worker.LivenessProbe", [] {});
        std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));
        if (timer.ElapsedMilliSecond() < probeIntervalMs && !isFirst) {
            continue;
        }
        timer.Reset();
        isFirst = false;
        rc = DoLivenessCheck();
        LOG(INFO) << FormatString("DoLivenessCheck, Status: %s", rc.ToString());

        if (rc.IsOk()) {
            logFlag = false;
            LOG_IF_ERROR(SetLivenessProbe(true), "SetLivenessProbe failed");
            continue;
        }
        LOG_IF(ERROR, !logFlag) << "liveness probe failed, try delete liveness probe file!";
        LOG_IF_ERROR(SetLivenessProbe(false), "SetLivenessProbe failed");
        logFlag = true;
    }
    LOG(INFO) << "Terminating worker liveness check thread.";
}

Status WorkerLivenessCheck::SetLivenessProbe(bool success)
{
    std::string fileDir = probeFileName_.substr(0, probeFileName_.find_last_of('/'));
    if (!FileExist(fileDir)) {
        const mode_t per = 0700;
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(CreateDir(fileDir, true, per), "Create liveness probe file failed");
    }
    std::ofstream outfile(probeFileName_);
    if (success) {
        outfile << "liveness check success\n" << std::endl;
    } else {
        outfile << "liveness check failed\n" << std::endl;
    }
    outfile.close();
    const mode_t per = 0600;
    return ChangeFileMod(probeFileName_, per);
}

Status WorkerLivenessCheck::CheckLivenessProbeFile(Timer &timer, const Status &lastStatus)
{
    if (probeFileName_.empty()) {
        return Status::OK();
    }
    const int checkIntervalS = 5;
    if (timer.ElapsedSecond() < checkIntervalS && lastStatus.IsOk()) {
        return Status::OK();
    }
    timer.Reset();
    struct stat fileStat;
    if (stat(probeFileName_.c_str(), &fileStat) != 0) {
        return Status(K_RUNTIME_ERROR, FormatString("%s not exist", probeFileName_.c_str()));
    }

    // Check for "liveness check failed" in file
    std::ifstream file(probeFileName_.c_str());
    if (file.is_open()) {
        std::string line;
        std::stringstream buffer;
        buffer << file.rdbuf();
        file.close();
        if (buffer.str().find("liveness check failed") != std::string::npos) {
            return Status(K_RUNTIME_ERROR, "liveness check failed");
        }
    } else {
        LOG(WARNING) << "file open failed";
        return Status::OK();
    }

    // Check last update time
    time_t probeLastUpdateTime = fileStat.st_mtime;
    time_t currentTime = time(nullptr);
    if ((currentTime - probeLastUpdateTime) > probeTimeoutMs_ / TIME_UNIT_CONVERSION) {
        return Status(K_RUNTIME_ERROR,
                      FormatString("liveness file not update for %d s", currentTime - probeLastUpdateTime));
    }

    return Status::OK();
}

Status WorkerLivenessCheck::ResetLivenessProbe()
{
    if (FileExist(probeFileName_)) {
        RETURN_IF_NOT_OK(DeleteFile(probeFileName_));  // Delete file if exist
    }
    return Status::OK();
}

bool WorkerLivenessCheck::IsMasterNode()
{
    return FLAGS_enable_distributed_master || FLAGS_master_address == hostPort_.ToString();
}

Status WorkerLivenessCheck::DoLivenessCheck()
{
    for (auto &policy : policies_) {
        Status rc = policy.func();
        if (rc.IsOk() || rc.GetCode() == K_INVALID) {
            continue;
        }
        LOG(ERROR) << "liveness probe for " << policy.name << " failed with " << rc.ToString();
        if (rc.GetCode() == K_WORKER_ABNORMAL || rc.GetCode() == K_KVSTORE_ERROR) {
            return rc;
        }
    }
    return Status::OK();
}

Status WorkerLivenessCheck::CheckWorkerServices()
{
    INJECT_POINT("WorkerLivenessCheck.GetServicesName.failed");
    for (auto &name : servicesNames_) {
        auto threadPoolUsage = workerOcServer_->GetRpcServicesUsage(name);
        auto usageRate = threadPoolUsage.threadPoolUsage;
        auto lastTime = threadPoolUsage.taskLastFinishTime;
        std::time_t currentTime = GetSteadyClockTimeStampUs();
        if (int(usageRate) == 1 && currentTime - lastTime > probeTimeoutMs_ * TIME_UNIT_CONVERSION) {
            std::string errMsg = FormatString("Liveness check failed, service of %s is failed.", name);
            RETURN_STATUS_LOG_ERROR(K_WORKER_ABNORMAL, errMsg);
        }
    }
    return Status::OK();
}

Status WorkerLivenessCheck::CheckRocksDbService()
{
    INJECT_POINT("WorkerLivenessCheck.CheckRocksDbService.failed");
    if (!IsMasterNode()) {
        return Status::OK();
    }
    master::CreateMetaReqPb req;
    master::CreateMetaRspPb rsp;
    datasystem::ObjectMetaPb *metadata = req.mutable_meta();
    metadata->set_object_key(livenessKey_);
    metadata->set_data_size(1);
    metadata->set_life_state(static_cast<uint32_t>(ObjectLifeState::OBJECT_PUBLISHED));
    metadata->set_ttl_second(0);
    ObjectMetaPb::ConfigPb *configPb = metadata->mutable_config();
    configPb->set_write_mode(static_cast<uint32_t>(WriteMode::NONE_L2_CACHE));
    configPb->set_data_format(static_cast<uint32_t>(DataFormat::BINARY));
    configPb->set_consistency_type(static_cast<uint32_t>(ConsistencyType::PRAM));
    req.set_address(hostPort_.ToString());
    g_MetaRocksDbName = workerUuid_;
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    auto rc = workerOcServer_->GetOcMetaSvc()->CreateMeta(req, rsp);
    // ignore error if the primary replica aready switch to other worker
    if (rc.IsOk() || rc.GetCode() == K_REPLICA_NOT_READY || lastSuccessTimeUs_ == 0) {
        lastSuccessTimeUs_ = GetSteadyClockTimeStampUs();
    }
    if (rc.IsError()) {
        LOG(WARNING) << "CheckRocksDbService failed in allowed time. " << rc.ToString();
    }
    auto currentTime = GetSteadyClockTimeStampUs();
    if (currentTime - lastSuccessTimeUs_ > probeTimeoutMs_ * TIME_UNIT_CONVERSION) {
        RETURN_STATUS_LOG_ERROR(K_WORKER_ABNORMAL, "CheckRocksDbService timeout.");
    }
    return Status::OK();
}
}  // namespace worker
}  // namespace datasystem
