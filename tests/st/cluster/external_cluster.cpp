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
 * Description: ExternalCluster means the clusters are started by processes.
 */
#include "cluster/external_cluster.h"

#include <csignal>
#include <fstream>
#include <sys/prctl.h>
#include <sys/time.h>
#include <wait.h>

#include <securec.h>

#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/log/log.h"
#include "datasystem/utils/status.h"
#include "datasystem/common/log/trace.h"

DS_DECLARE_string(unix_domain_socket_dir);
DS_DECLARE_bool(enable_etcd_auth);

namespace datasystem {
namespace st {
Status BaseCluster::ExecuteCmd(const std::string &cmd, std::string &result, int *exitCode)
{
    const std::string modifiedCmd = cmd + " 2>&1";
    FILE *ptr = popen(modifiedCmd.c_str(), "r");
    CHECK_FAIL_RETURN_STATUS(ptr != nullptr, StatusCode::K_RUNTIME_ERROR, "Execute cmd:" + cmd + " error.");
    const int masBufLen = 1024;
    char buffer[masBufLen] = { 0 };
    while (fgets(buffer, masBufLen, ptr) != nullptr) {
        result.append(buffer);
        CHECK_FAIL_RETURN_STATUS(memset_s(buffer, masBufLen, 0, masBufLen) == EOK, K_RUNTIME_ERROR,
                                 "Memset failed, execute cmd failed");
    }
    if (exitCode) {
        *exitCode = pclose(ptr);
    } else {
        pclose(ptr);
    }
    return Status::OK();
}

ExternalCluster::ExternalCluster(const ExternalClusterOptions &opt) : opts_(opt)
{
    time_t seed =
        std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now()).time_since_epoch().count();
    randomData_ = RandomData(seed);
    if (opts_.rootDir.empty()) {
        opts_.rootDir = "./ds";
    }
    if (opts_.socketDir.empty()) {
        const int randomLen = 12;
        opts_.socketDir = "/tmp/" + randomData_.GetRandomString(randomLen);
        FLAGS_unix_domain_socket_dir = opts_.socketDir;
    }
    // Add random prefixes to tables in etcd to prevent conflicts when multiple cases are running at the same time.
    const int etcdPrefixLen = 6;
    opts_.etcdPrefix = randomData_.GetRandomString(etcdPrefixLen);
    if (opts_.numWorkers > 0) {
        for (uint32_t i = 0; i < opts_.numWorkers; ++i) {
            workerProcesses_.emplace_back(nullptr);
        }
    }
}

Status ExternalCluster::ShutdownNodes(ClusterNodeType nodeType)
{
    Status rc = Status::OK();
    if (!opts_.isObjectCache) {
        /* 2rd shutdown master. */
        if (ClusterNodeType::ALL == nodeType || ClusterNodeType::MASTER == nodeType) {
            ASSIGN_IF_NOT_OK(rc, ShutdownMasters());
        }
    }
    /* 3nd shutdown worker. */
    if (ClusterNodeType::ALL == nodeType || ClusterNodeType::WORKER == nodeType) {
        ASSIGN_IF_NOT_OK(rc, ShutdownWorkers());
    }
    /* 4th shutdown etcd. */
    if (ClusterNodeType::ALL == nodeType || ClusterNodeType::ETCD == nodeType) {
        ASSIGN_IF_NOT_OK(rc, ShutdownEtcds());
    }
    /* 5th shutdown obs. */
    if (ClusterNodeType::ALL == nodeType || ClusterNodeType::OBS == nodeType) {
        ASSIGN_IF_NOT_OK(rc, ShutdownOBSs());
    }
    return rc;
}

Status ExternalCluster::ShutdownWorkers()
{
    if (opts_.skipWorkerPreShutdown) {
        for (uint32_t i = 0; i < opts_.numWorkers; i++) {
            if (workerProcesses_[i] != nullptr && workerProcesses_[i]->IsProcessAlive()) {
                SetInjectAction(ClusterNodeType::WORKER, i, "worker.PreShutDown.skip", "return(K_OK)");
            }
        }
    }
    std::lock_guard<std::mutex> lock(mutex_worker_);
    Status rc = Status::OK();
    if (workerProcesses_.empty()) {
        return rc;
    }
    ThreadPool pool(workerProcesses_.size());
    std::vector<std::future<Status>> futs;
    if (workerForkPid_ > 0) {
        return FinishForkWorkerProcess();
    }
    for (auto &workerProcess : workerProcesses_) {
        if (workerProcess != nullptr && workerProcess->IsProcessAlive()) {
            futs.emplace_back(pool.Submit([&workerProcess] {
                RETURN_IF_NOT_OK_APPEND_MSG(
                    workerProcess->Shutdown(),
                    FormatString("Worker process: %s shutdown failed.", workerProcess->GetAddr().ToString()));
                workerProcess.reset();
                return Status::OK();
            }));
        }
    }
    for (auto &fut : futs) {
        ASSIGN_IF_NOT_OK_PRINT_ERROR_MSG(rc, fut.get(), "Shutdown worker failed");
    }
    return rc;
}

Status ExternalCluster::AddNode(const HostPort &masterAddress, const std::string workerAddress, int workerDirectPort)
{
    const int num = 1;
    CHECK_FAIL_RETURN_STATUS(!masterAddress.Empty() && !workerAddress.empty(), K_INVALID, "address is empty");
    opts_.numWorkers += 1;
    HostPort workerAddr;
    RETURN_IF_NOT_OK(workerAddr.ParseString(workerAddress));
    opts_.workerConfigs.emplace_back(workerAddr);
    opts_.workerOcDirectPorts.emplace_back(workerDirectPort);
    workerProcesses_.emplace_back(nullptr);
    return StartWorker(opts_.numWorkers - num, masterAddress);
}

Status ExternalCluster::ShutdownMasters()
{
    CHECK_FAIL_RETURN_STATUS(!opts_.isObjectCache, K_INVALID, "No master in object cache.");
    std::lock_guard<std::mutex> lock(mutex_master_);
    Status rc = Status::OK();
    for (auto &masterProcess : masterProcesses_) {
        if (masterProcess->IsProcessAlive()) {
            ASSIGN_IF_NOT_OK_PRINT_ERROR_MSG(
                rc, masterProcess->Shutdown(),
                FormatString("Master process: %s shutdown failed.", masterProcess->GetAddr().ToString()));
        }
    }
    masterProcesses_.clear();
    return rc;
}

Status ExternalCluster::ShutdownOBSs()
{
    Status rc = Status::OK();
    for (auto &csProcess : OBSProcesses_) {
        if (csProcess->IsProcessAlive()) {
            ASSIGN_IF_NOT_OK_PRINT_ERROR_MSG(
                rc, csProcess->Shutdown(),
                FormatString("OBS process: %s shutdown failed.", csProcess->GetAddr().ToString()));
            csProcess->Shutdown();
        }
    }
    OBSProcesses_.clear();
    return rc;
}

Status ExternalCluster::ShutdownEtcds()
{
    Status rc = Status::OK();
    for (auto &etcdProcess : etcdProcesses_) {
        if (etcdProcess->IsProcessAlive()) {
            auto addrs = etcdProcess->GetAddrs();
            ASSIGN_IF_NOT_OK_PRINT_ERROR_MSG(rc, etcdProcess->Shutdown(),
                                             FormatString("Etcd process: %s -> %s shutdown failed.",
                                                          addrs.first.ToString(), addrs.second.ToString()));
            etcdProcess->Shutdown();
        }
    }
    etcdProcesses_.clear();
    return rc;
}

bool ExternalCluster::CheckWorkerProcess(uint32_t idx)
{
    auto workerProcess = workerProcesses_[idx].get();
    if (workerProcess->IsProcessAlive()) {
        return true;
    }
    return false;
}

Status ExternalCluster::KillWorker(uint32_t idx)
{
    auto workerProcess = workerProcesses_[idx].get();
    if (workerProcess->IsProcessAlive()) {
        return workerProcess->ShutdownByKill();
    }
    return Status(K_NOT_FOUND, "worker process is not alive.");
}

Status ExternalCluster::QuicklyShutdownWorker(uint32_t idx)
{
    Status rc = Status::OK();
    if (opts_.skipWorkerPreShutdown) {
        SetInjectAction(ClusterNodeType::WORKER, idx, "worker.PreShutDown.skip", "return(K_OK)");
    }
    auto workerProcess = workerProcesses_[idx].get();
    if (workerProcess->IsProcessAlive()) {
        ASSIGN_IF_NOT_OK(rc, workerProcess->Shutdown());
    }
    return rc;
}

Status ExternalCluster::ShutdownNode(ClusterNodeType nodeType, uint32_t idx)
{
    Status rc = Status::OK();

    /* 2nd shutdown worker. */
    if (ClusterNodeType::WORKER == nodeType) {
        auto workerProcess = workerProcesses_[idx].get();
        if (workerProcess->IsProcessAlive()) {
            ASSIGN_IF_NOT_OK(rc, workerProcess->Shutdown());
        }
    }

    /* 3rd shutdown master. */
    if (ClusterNodeType::MASTER == nodeType) {
        CHECK_FAIL_RETURN_STATUS(!opts_.isObjectCache, K_INVALID, "No master in object cache.");
        auto masterProcess = masterProcesses_[idx].get();
        if (masterProcess->IsProcessAlive()) {
            ASSIGN_IF_NOT_OK(rc, masterProcess->Shutdown());
        }
    }

    /* 4th shutdown etcd. */
    if (ClusterNodeType::ETCD == nodeType) {
        auto etcdProcess = etcdProcesses_[idx].get();
        if (etcdProcess->IsProcessAlive()) {
            ASSIGN_IF_NOT_OK(rc, etcdProcess->Shutdown());
        }
    }
    return rc;
}

Status ExternalCluster::StartNode(ClusterNodeType nodeType, uint32_t idx, const std::string &params)
{
    Subprocess *process = nullptr;
    RETURN_IF_NOT_OK(GetProcess(nodeType, idx, process));
    CHECK_FAIL_RETURN_STATUS(process != nullptr, K_NOT_FOUND, "Process is nullptr");
    if (!params.empty()) {
        process->AppendCmdParams(params);
    }
    RETURN_IF_NOT_OK(process->Start());
    return Status::OK();
}

Status ExternalCluster::GetProcess(ClusterNodeType nodeType, uint32_t idx, Subprocess *&process)
{
    switch (nodeType) {
        case ClusterNodeType::MASTER: {
            CHECK_FAIL_RETURN_STATUS(!opts_.isObjectCache, K_INVALID, "No master in object cache.");
            CHECK_FAIL_RETURN_STATUS(idx < masterProcesses_.size(), K_INVALID, "Invalid index.");
            process = masterProcesses_[idx].get();
            if (process->IsProcessAlive()) {
                process->Shutdown();
            }
            break;
        }
        case ClusterNodeType::WORKER: {
            CHECK_FAIL_RETURN_STATUS(idx < workerProcesses_.size(), K_INVALID, "Invalid index.");
            process = workerProcesses_[idx].get();
            if (process->IsProcessAlive()) {
                process->Shutdown();
            }
            break;
        }
        case ClusterNodeType::ETCD: {
            CHECK_FAIL_RETURN_STATUS(idx < etcdProcesses_.size(), K_INVALID, "Invalid index.");
            process = etcdProcesses_[idx].get();
            if (process->IsProcessAlive()) {
                process->Shutdown();
            }
            break;
        }
        default:
            RETURN_STATUS(K_INVALID, "Invalid node type.");
    }
    return Status::OK();
}

Status ExternalCluster::GetProcess(ClusterNodeType nodeType, uint32_t idx, ServerProcess *&process)
{
    switch (nodeType) {
        case ClusterNodeType::MASTER: {
            CHECK_FAIL_RETURN_STATUS(!opts_.isObjectCache, K_INVALID, "No master in object cache.");
            CHECK_FAIL_RETURN_STATUS(idx < masterProcesses_.size(), K_INVALID, "Invalid index.");
            process = masterProcesses_[idx].get();
            break;
        }
        case ClusterNodeType::WORKER: {
            CHECK_FAIL_RETURN_STATUS(idx < workerProcesses_.size(), K_INVALID, "Invalid index.");
            process = workerProcesses_[idx].get();
            break;
        }
        default:
            RETURN_STATUS(K_INVALID, "Invalid node type.");
    }
    CHECK_FAIL_RETURN_STATUS(process != nullptr, K_NOT_FOUND, "Process is nullptr");
    return Status::OK();
}

Status ExternalCluster::SetInjectAction(ClusterNodeType nodeType, uint32_t idx, const std::string &name,
                                        const std::string &action)
{
    ServerProcess *process = nullptr;
    RETURN_IF_NOT_OK(GetProcess(nodeType, idx, process));
    return process->SetInjectAction(name, action);
}

Status ExternalCluster::ClearInjectAction(ClusterNodeType nodeType, uint32_t idx, const std::string &name)
{
    ServerProcess *process = nullptr;
    RETURN_IF_NOT_OK(GetProcess(nodeType, idx, process));
    return process->ClearInjectAction(name);
}

Status ExternalCluster::GetInjectActionExecuteCount(ClusterNodeType nodeType, uint32_t idx, const std::string &name,
                                                    uint64_t &executCount)
{
    ServerProcess *process = nullptr;
    RETURN_IF_NOT_OK(GetProcess(nodeType, idx, process));
    return process->GetInjectActionExecuteCount(name, executCount);
}

size_t ExternalCluster::GetEtcdNum() const
{
    return etcdProcesses_.size();
}

size_t ExternalCluster::GetMasterNum() const
{
    if (opts_.isObjectCache) {
        return 0;
    }
    return masterProcesses_.size();
}

size_t ExternalCluster::GetGCSNum() const
{
    if (opts_.isObjectCache) {
        return 0;
    }
    return gcsProcesses_.size();
}

size_t ExternalCluster::GetWorkerNum() const
{
    return workerProcesses_.size();
}

Status ExternalCluster::GetEtcdAddrs(uint32_t idx, std::pair<HostPort, HostPort> &addrs) const
{
    CHECK_FAIL_RETURN_STATUS(idx < etcdProcesses_.size(), StatusCode::K_INVALID, "Invalid idx.");
    addrs = etcdProcesses_[idx]->GetAddrs();
    return Status::OK();
}

std::string ExternalCluster::GetEtcdAddrs() const
{
    std::string etcdAddress;
    for (size_t i = 0; i < GetEtcdNum(); ++i) {
        std::pair<HostPort, HostPort> addrs;
        GetEtcdAddrs(i, addrs);
        if (!etcdAddress.empty()) {
            etcdAddress += ",";
        }
        etcdAddress += addrs.first.ToString();
    }
    return etcdAddress;
}

Status ExternalCluster::GetMasterAddr(uint32_t idx, HostPort &addr) const
{
    CHECK_FAIL_RETURN_STATUS(!opts_.isObjectCache, K_INVALID, "No master in object cache.");
    CHECK_FAIL_RETURN_STATUS(idx < masterProcesses_.size(), StatusCode::K_INVALID, "Invalid idx.");
    addr = masterProcesses_[idx]->GetAddr();
    return Status::OK();
}

Status ExternalCluster::GetGCSAddr(uint32_t idx, HostPort &addr) const
{
    CHECK_FAIL_RETURN_STATUS(!opts_.isObjectCache, K_INVALID, "No GCS in object cache.");
    CHECK_FAIL_RETURN_STATUS(idx < gcsProcesses_.size(), StatusCode::K_INVALID, "Invalid idx.");
    addr = gcsProcesses_[idx].first->GetAddr();
    return Status::OK();
}

Status ExternalCluster::GetWorkerAddr(uint32_t idx, HostPort &addr) const
{
    CHECK_FAIL_RETURN_STATUS(idx < workerProcesses_.size(), StatusCode::K_INVALID, "Invalid idx.");
    RETURN_RUNTIME_ERROR_IF_NULL(workerProcesses_[idx]);
    addr = workerProcesses_[idx]->GetAddr();
    return Status::OK();
}

Status ExternalCluster::GetOBSAddr(uint32_t idx, HostPort &addr) const
{
    CHECK_FAIL_RETURN_STATUS(idx < OBSProcesses_.size(), StatusCode::K_INVALID, "Invalid idx.");
    addr = OBSProcesses_[idx]->GetAddr();
    return Status::OK();
}

Status ExternalCluster::GetMetaServerAddr(HostPort &addr) const
{
    CHECK_FAIL_RETURN_STATUS(opts_.isObjectCache, K_INVALID, "No meta server in file cache.");
    CHECK_FAIL_RETURN_STATUS(opts_.workerConfigs.size() > 0, K_INVALID, "No worker address provided.");
    addr = opts_.workerConfigs[0];
    return Status::OK();
}

pid_t ExternalCluster::GetEtcdPid(uint32_t idx) const
{
    if (idx < etcdProcesses_.size()) {
        return etcdProcesses_[idx]->Pid();
    } else {
        return static_cast<pid_t>(-1);
    }
}

pid_t ExternalCluster::GetMasterPid(uint32_t idx) const
{
    if (!opts_.isObjectCache && idx < masterProcesses_.size()) {
        return masterProcesses_[idx]->Pid();
    } else {
        return static_cast<pid_t>(-1);
    }
}

pid_t ExternalCluster::GetGCSPid(uint32_t idx) const
{
    if (!opts_.isObjectCache && idx < gcsProcesses_.size()) {
        return gcsProcesses_[idx].first->Pid();
    } else {
        return static_cast<pid_t>(-1);
    }
}

pid_t ExternalCluster::GetWorkerPid(uint32_t idx) const
{
    if (idx < workerProcesses_.size()) {
        return workerProcesses_[idx]->Pid();
    } else {
        return static_cast<pid_t>(-1);
    }
}

Status ExternalCluster::WaitForExpectedResult(std::function<Status()> function, int timeoutSecs,
                                              StatusCode expectedStatusCode)
{
    timeval now;
    gettimeofday(&now, NULL);
    time_t deadline = now.tv_sec + timeoutSecs;
    int intervals = 200;
    Status res;
    while (true) {
        res = function();
        if (res.GetCode() == expectedStatusCode) {
            break;
        }
        timeval curr;
        gettimeofday(&curr, NULL);
        CHECK_FAIL_RETURN_STATUS(curr.tv_sec < deadline, StatusCode::K_RUNTIME_ERROR,
                                 "wait for expected result timeout");
        std::this_thread::sleep_for(std::chrono::milliseconds(intervals));
    }
    return Status::OK();
}

Status ExternalCluster::KillLeaderEtcd()
{
    std::string clusterUrl;
    for (size_t i = 0; i < opts_.etcdIpAddrs.size(); ++i) {
        if (!clusterUrl.empty()) {
            clusterUrl += ",";
        }
        clusterUrl = clusterUrl + "http://" + opts_.etcdIpAddrs[i].first.ToString();
    }

    std::string cmd = "etcdctl --endpoints " + clusterUrl + " endpoint status";
    std::string result;
    std::string leaderAddr;
    RETURN_IF_NOT_OK(ExecuteCmd(cmd, result));
    if (!result.empty()) {
        auto vecResult = Split(result, "\n");
        for (auto &r : vecResult) {
            auto vecRow = Split(r, ", ");
            // Example result: http://xxx:xx, 6c5e3c4cd93d8e, 3.5.1, 29 kB, false, false, 2, 24, 24,
            // We need to obtain the fifth parameter.
            if (vecRow.size() > 4 && vecRow[4] == "true") {
                leaderAddr = vecRow[0];
                break;
            }
        }
    }
    if (leaderAddr.empty()) {
        RETURN_STATUS_LOG_ERROR(K_UNKNOWN_ERROR, "The etcd cluster does not have a leader.");
    }
    for (size_t i = 0; i < opts_.etcdIpAddrs.size(); ++i) {
        if (leaderAddr.find(opts_.etcdIpAddrs[i].first.ToString()) != std::string::npos) {
            ShutdownNode(ClusterNodeType::ETCD, i);
            LOG(INFO) << "Shutdown the etcd leader node: " << leaderAddr;
            break;
        }
    }
    return Status::OK();
}

std::string ExternalCluster::ConstructEtcdCheckHealthCmd() const
{
    std::string clusterUrl;
    for (size_t i = 0; i < opts_.etcdIpAddrs.size(); ++i) {
        if (!clusterUrl.empty()) {
            clusterUrl += ",";
        }
        if (FLAGS_enable_etcd_auth == true) {
            clusterUrl = clusterUrl + "https://" + opts_.etcdIpAddrs[i].first.ToString();
        } else {
            clusterUrl = clusterUrl + "http://" + opts_.etcdIpAddrs[i].first.ToString();
        }
    }

    // Check whether the RPC service is started based on whether the process listens to the IP port.
    std::string cmd = "etcdctl --endpoints " + clusterUrl + " endpoint health";
    if (FLAGS_enable_etcd_auth == true) {
        cmd = "etcdctl --endpoints " + clusterUrl + " --cert=./certs/etcd-server.crt" + " --key=./certs/etcd-server.key"
              + " --cacert=./certs/ca.crt" + " endpoint health";
    }
    cmd += " 2>&1";
    return cmd;
}

Status ExternalCluster::WaitEtcdReadyOrTimeout(int timeoutSecs)
{
    timeval now;
    gettimeofday(&now, NULL);
    time_t deadLine = now.tv_sec + timeoutSecs;
    std::string cmd = ConstructEtcdCheckHealthCmd();

    while (true) {
        for (size_t i = 0; i < opts_.etcdIpAddrs.size(); ++i) {
            // process does not exist
            if (kill(GetEtcdPid(i), 0) != 0) {
                return Status(K_NOT_READY, "Etcd not exist.");
            }
            // process is defunct
            int status;
            if (waitpid(GetEtcdPid(i), &status, WNOHANG) != 0) {
                return Status(K_NOT_READY, "Etcd is abnormal.");
            }
        }
        std::string result;
        RETURN_IF_NOT_OK(ExecuteCmd(cmd, result));
        if (!result.empty()) {
            auto vecResult = Split(result, "\n");
            size_t successNum = 0;
            for (auto &r : vecResult) {
                if (r.find("is healthy") != std::string::npos) {
                    successNum++;
                }
            }
            if (opts_.etcdIpAddrs.size() == successNum) {
                break;
            }
        }
        timeval curr;
        gettimeofday(&curr, NULL);
        CHECK_FAIL_RETURN_STATUS(curr.tv_sec < deadLine, StatusCode::K_RUNTIME_ERROR,
                                 "Process startup etcd cluster timed out.");
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    LOG(INFO) << "Etcd cluster started successfully.";
    return Status::OK();
}

Status ExternalCluster::StartEtcdCluster()
{
    RETURN_OK_IF_TRUE(opts_.numEtcd == 0);

    CHECK_FAIL_RETURN_STATUS(opts_.numEtcd <= opts_.etcdIpAddrs.size(), StatusCode::K_RUNTIME_ERROR,
                             "The number of etcdIpAddrs is less than numEtcd.");

    for (size_t i = 0; i < opts_.numEtcd; ++i) {
        RETURN_IF_NOT_OK(StartEtcdNode(i));
    }
    return WaitEtcdReadyOrTimeout(WAIT_TIMEOUT_SECS);
}

Status ExternalCluster::StartMasters()
{
    CHECK_FAIL_RETURN_STATUS(!opts_.isObjectCache, K_INVALID, "No master in object cache.");
    if (opts_.numMasters == 0) {
        LOG(INFO) << "The value of numMasters is 0. Therefore, the master is not started.";
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(opts_.numMasters <= opts_.masterIpAddrs.size(), StatusCode::K_RUNTIME_ERROR,
                             "The number of masterIpAddrs is less than numMasters.");
    for (size_t i = 0; i < opts_.numMasters; ++i) {
        RETURN_IF_NOT_OK(StartMaster(i));
    }
    return Status::OK();
}

Status ExternalCluster::StartWorkers()
{
    if (opts_.numWorkers == 0) {
        LOG(INFO) << "The value of numWorkers is 0. Therefore, the worker is not started.";
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(opts_.numWorkers <= opts_.workerConfigs.size(), StatusCode::K_RUNTIME_ERROR,
                             "The number of workerConfigs is less than numWorkers.");
    if (!opts_.isObjectCache && opts_.masterIpAddrs.empty()) {
        LOG(WARNING) << "master addresses is empty in file cache,  worker cannot find master!";
        return Status::OK();
    }

    bool isWait = opts_.waitWorkerReady;
    opts_.waitWorkerReady = false;
    HostPort address;
    if (opts_.isObjectCache) {
        CHECK_FAIL_RETURN_STATUS(opts_.masterIdx < (int32_t)opts_.workerConfigs.size(), StatusCode::K_INVALID,
                                 "The index for master is out of range.");
        address = opts_.masterIdx < 0 ? HostPort() : opts_.workerConfigs[opts_.masterIdx];
        // Start the worker with master first
        if (opts_.masterIdx != -1) {
            RETURN_IF_NOT_OK(StartWorker(opts_.masterIdx, address));
        }
    } else {
        CHECK_FAIL_RETURN_STATUS(opts_.masterIpAddrs.size() > 0, StatusCode::K_INVALID,
                                 "There are no master address provided.");
        address = opts_.masterIpAddrs[0];
    }
    // Start the remaining masters (object cache) or all masters (file cache)
    for (size_t i = 0; i < opts_.numWorkers; ++i) {
        if (opts_.isObjectCache && (int32_t)i == opts_.masterIdx) {
            continue;
        }
        RETURN_IF_NOT_OK(StartWorker(i, address));
    }
    if (isWait) {
        opts_.waitWorkerReady = true;
        if (opts_.masterIdx != -1) {
            RETURN_IF_NOT_OK(WaitNodeReady(WORKER, opts_.masterIdx, WAIT_TIMEOUT_SECS));
        }
        for (size_t i = 0; i < opts_.numWorkers; ++i) {
            if (opts_.isObjectCache && (int32_t)i == opts_.masterIdx) {
                continue;
            }
            RETURN_IF_NOT_OK(WaitNodeReady(WORKER, i, WAIT_TIMEOUT_SECS));
        }
    }
    return Status::OK();
}

Status ExternalCluster::WaitUntilClusterReadyOrTimeout(int timeoutSecs)
{
    timeval now;
    gettimeofday(&now, NULL);
    time_t deadLine = now.tv_sec + timeoutSecs;

    std::vector<std::string> allAddrs;
    for (size_t i = 0; i < opts_.numWorkers; ++i) {
        allAddrs.push_back(workerProcesses_[i]->GetAddr().ToString());
    }
    if (!opts_.isObjectCache) {
        for (size_t i = 0; i < opts_.numMasters; ++i) {
            allAddrs.push_back(masterProcesses_[i]->GetAddr().ToString());
        }
    }

    // Check whether the RPC service is started based on whether the process listens to the IP port.
    for (auto addr : allAddrs) {
        timeval curr;
        gettimeofday(&curr, NULL);
        int timeout = deadLine - curr.tv_sec;
        RETURN_IF_NOT_OK(CheckIpPortListen(addr, timeout));
    }

    LOG(INFO) << "All processes are started successfully. numMasters:" << opts_.numMasters
              << " numWorkers:" << opts_.numWorkers;

    return Status::OK();
}

Status ExternalCluster::CheckIpPortListen(const std::string &addr, int timeoutSecs)
{
    timeval now;
    gettimeofday(&now, nullptr);
    time_t deadLine = now.tv_sec + timeoutSecs;
    // netstat -nat | grep ip:port
    std::string cmd = "netstat -nat | grep " + addr;
    while (true) {
        std::string result;
        RETURN_IF_NOT_OK(ExecuteCmd(cmd, result));
        if (!result.empty() && result.find("LISTEN") != std::string::npos) {
            break;
        }
        timeval curr;
        gettimeofday(&curr, nullptr);
        CHECK_FAIL_RETURN_STATUS(curr.tv_sec < deadLine, StatusCode::K_RUNTIME_ERROR,
                                 FormatString("Process startup timed out. address: %s, port status: %s", addr, result));
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return Status::OK();
}

Status ExternalCluster::CheckProbeFile(const std::string &filepath, int timeoutSecs, pid_t pid)
{
    timeval now;
    gettimeofday(&now, NULL);
    time_t deadLine = now.tv_sec + timeoutSecs;
    while (true) {
        if (access(filepath.c_str(), F_OK) != -1) {
            break;
        }
        // process does not exist
        if (kill(pid, 0) != 0) {
            return Status(K_NOT_READY, "Subprocess not exist.");
        }
        // process is defunct
        int status;
        if (waitpid(pid, &status, WNOHANG) != 0) {
            return Status(K_NOT_READY, "Subprocess is abnormal.");
        }
        timeval curr;
        gettimeofday(&curr, NULL);
        CHECK_FAIL_RETURN_STATUS(curr.tv_sec < deadLine, StatusCode::K_RUNTIME_ERROR,
                                 FormatString("CheckHealthFile timed out, %s", filepath));
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return Status::OK();
}

Status ExternalCluster::WaitNodeReady(ClusterNodeType nodeType, uint32_t index, int timeoutSecs)
{
    switch (nodeType) {
        case ClusterNodeType::WORKER: {
            std::stringstream healthCheckPath;
            healthCheckPath << opts_.rootDir << "/worker" << index << "/health";
            return CheckProbeFile(healthCheckPath.str(), timeoutSecs, GetWorkerPid(index));
        }
        case ClusterNodeType::MASTER: {
            CHECK_FAIL_RETURN_STATUS(!opts_.isObjectCache, K_INVALID, "No master in object cache.");
            std::stringstream healthCheckPath;
            healthCheckPath << opts_.rootDir << "/master" << index << "/health";
            return CheckProbeFile(healthCheckPath.str(), timeoutSecs, GetMasterPid(index));
        }
        case ClusterNodeType::ETCD:
            return WaitEtcdReadyOrTimeout(timeoutSecs);
        default:
            RETURN_STATUS(StatusCode::K_INVALID, "Invalid nodeType:" + std::to_string(nodeType));
    }
}

bool ExternalCluster::IsWorkerLiveness(uint32_t index)
{
    std::string livenessCheckPath = FormatString("%s/worker%d/liveness", opts_.rootDir, index);
    std::ifstream file(livenessCheckPath);
    if (!file) {
        LOG(ERROR) << "Open file failed: " << StrErr(errno) << std::endl;
        return false;
    }
    std::stringstream buffer;
    buffer << file.rdbuf();
    std::string content = buffer.str();
    return content.find("liveness check success") != std::string::npos;
}

Status ExternalCluster::StartOBS()
{
    if (opts_.numOBS == 0) {
        LOG(INFO) << "The value of numOBS is 0. Therefore, the OBS is not started.";
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(opts_.numOBS <= opts_.OBSIpAddrs.size(), StatusCode::K_RUNTIME_ERROR,
                             "The number of OBSIpAddrs is less than numOBS.");
 
    for (size_t i = 0; i < opts_.numOBS; ++i) {
        RETURN_IF_NOT_OK(StartOBS(i));
    }
    return Status::OK();
}

Status ExternalCluster::ClearRootDir()
{
    return RemoveAll(opts_.rootDir);
}

Status ExternalCluster::ClearSocketDir()
{
    return RemoveAll(opts_.socketDir);
}

std::string ExternalCluster::GetSocketDir()
{
    return opts_.socketDir;
}

std::vector<std::string> ExternalCluster::GetGCSFragmentStoreDirs(uint32_t idx)
{
    std::string rootDir = opts_.rootDir + "/gcs" + std::to_string(idx);
    std::vector<std::string> dirs;
    dirs.push_back(rootDir + "/fragment_store_0");
    dirs.push_back(rootDir + "/fragment_store_1");
    return dirs;
}

std::string ExternalCluster::GetNASDir()
{
    return opts_.nasDir;
}

std::string SearchPath(const std::string &exec)
{
    const auto paths = Split(std::getenv("PATH"), ":");
    std::string fullPath;
    for (auto &path : paths) {
        fullPath = path + "/" + exec;
        if (access(fullPath.c_str(), X_OK) == 0) {
            return fullPath;
        }
    }
    return "";
}

Status ExternalCluster::StartEtcdNode(int index)
{
    std::string dataDir = opts_.rootDir + "/etcd_cluster/etcd" + std::to_string(index);
    if (!FileExist(dataDir)) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(CreateDir(dataDir, true), "Failed to create etcd data dir! " + dataDir);
    }
    std::string clientUrl = "http://" + opts_.etcdIpAddrs[index].first.ToString();
    if (FLAGS_enable_etcd_auth == true) {
        clientUrl = "https://" + opts_.etcdIpAddrs[index].first.ToString();
    }
    std::string serverUrl = "http://" + opts_.etcdIpAddrs[index].second.ToString();
    std::string clusterUrl;
    for (size_t i = 0; i < opts_.etcdIpAddrs.size(); ++i) {
        if (!clusterUrl.empty()) {
            clusterUrl += ",";
        }
        clusterUrl = clusterUrl + "etcd" + std::to_string(i) + "=http://" + opts_.etcdIpAddrs[i].second.ToString();
    }
    std::string etcdCmd = SearchPath("etcd") + " -name etcd" + std::to_string(index) + " --data-dir " + dataDir
                          + " --log-level error" + " --listen-client-urls " + clientUrl + " --advertise-client-urls "
                          + clientUrl + " --listen-peer-urls " + serverUrl + " --initial-advertise-peer-urls "
                          + serverUrl + " --initial-cluster " + clusterUrl + " --initial-cluster-token etcd-cluster";
    if (FLAGS_enable_etcd_auth == true) {
        etcdCmd += " --cert-file ./certs/etcd-server.crt  --key-file ./certs/etcd-server.key";
        etcdCmd += " --client-cert-auth --trusted-ca-file ./certs/ca.crt";
    }
    LOG(INFO) << "Launch etcd [" << index << "] command: " << etcdCmd;
    auto etcdProcess = std::make_unique<EtcdProcess>(etcdCmd, opts_.etcdIpAddrs[index]);
    RETURN_IF_NOT_OK(etcdProcess->Start());
    etcdProcesses_.emplace_back(std::move(etcdProcess));
    return Status::OK();
}

Status ExternalCluster::StartMaster(int index)
{
    CHECK_FAIL_RETURN_STATUS(!opts_.isObjectCache, K_INVALID, "No master in object cache.");
    std::string masterCmd = MASTER_BIN_PATH;
    std::string rootDir = opts_.rootDir + "/master" + std::to_string(index);
    std::string healthFile = rootDir + "/health";
    (void)DeleteFile(healthFile);
    masterCmd += " -master_address=" + opts_.masterIpAddrs[index].ToString() + " -log_dir=" + rootDir + "/log"
                 + " -rocksdb_store_dir=" + rootDir + "/rocksdb" + " -unix_domain_socket_dir=" + opts_.socketDir + " "
                 + " -v=" + std::to_string(opts_.vLogLevel) + " -health_check_path=" + healthFile
                 + +" -l2_cache_delete_thread_num=4 " + opts_.masterGflagParams
                 + " -rpc_thread_num=" + std::to_string(opts_.numRpcThreads);
    if (GetEtcdNum() > 0) {
        std::string etcdUrl;
        for (size_t i = 0; i < GetEtcdNum(); ++i) {
            std::pair<HostPort, HostPort> addrs;
            GetEtcdAddrs(i, addrs);
            if (!etcdUrl.empty()) {
                etcdUrl += ",";
            }
            etcdUrl += addrs.first.ToString();
        }
        masterCmd += " -backend_store=etcd -etcd_address=" + etcdUrl + " -cluster_name=" + opts_.etcdPrefix;
    }
    LOG(INFO) << "Launch master [" << index << "] command: " << masterCmd;
    auto masterProcess = std::make_unique<MasterProcess>(masterCmd, opts_.masterIpAddrs[index]);
    RETURN_IF_NOT_OK(masterProcess->Start());
    masterProcesses_.emplace_back(std::move(masterProcess));
    return Status::OK();
}

std::string ExternalCluster::AddGFlagForWorkerStart(std::string &flag)
{
    std::string oldFlag = opts_.workerGflagParams;
    opts_.workerGflagParams += flag;
    return oldFlag;
}

void ExternalCluster::SetGFlagForWorkerStart(std::string &flag)
{
    opts_.workerGflagParams = flag;
}

Status ExternalCluster::StartForkWorkerProcess()
{
    if (pipe(pipeFd_) == -1) {
        int err = errno;
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Unable to create pipe: " + StrErr(err));
    }

    workerForkPid_ = fork();
    if (workerForkPid_ == -1) {
        int err = errno;
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Unable to start control process: " + StrErr(err));
    } else if (workerForkPid_ == 0) {
        prctl(PR_SET_PDEATHSIG, SIGKILL);
        close(pipeFd_[1]);
        Status rc = Status::OK();
        while (read(pipeFd_[0], &message_, sizeof(message_)) > 0) {
            if (message_ == -1) {
                LOG_IF_ERROR(ShutdownWorkers(), "Shutdown workers failed");
                break;
            }
            rc = StartWorker(message_, HostPort());
            if (rc.IsError()) {
                break;
            }
        }
        close(pipeFd_[0]);
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rc, "Failed to start worker by fork process");
    } else {
        close(pipeFd_[0]);
    }
    return Status::OK();
}

Status ExternalCluster::StartWorkerByForkProcess(int index)
{
    message_ = index;
    ssize_t n = write(pipeFd_[1], &message_, sizeof(message_));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(n > 0, K_RUNTIME_ERROR, "Send msg to pipe failed");

    auto workerProcess = std::make_unique<WorkerProcess>("", opts_.workerConfigs[index]);
    RETURN_IF_NOT_OK(workerProcess->SetRunningMode());
    workerProcesses_[index].reset(workerProcess.release());

    return Status::OK();
}

Status ExternalCluster::FinishForkWorkerProcess()
{
    message_ = -1;
    ssize_t n = write(pipeFd_[1], &message_, sizeof(message_));
    if (n < 0) {
        LOG(ERROR) << "Failed to set finish to forkWorker";
    }
    close(pipeFd_[1]);
    return Status::OK();
}

Status ExternalCluster::StartWorker(int index, const HostPort &address, std::string gFlag)
{
    std::string cmd = WORKER_BIN_PATH;
    std::string rootDir = opts_.rootDir + "/worker" + std::to_string(index);
    std::string logDir = rootDir + "/log";
    std::string healthFile = rootDir + "/health";
    std::string spillDir;
    if (opts_.enableSpill) {
        spillDir = rootDir + "/spill";
    }
    (void)DeleteFile(healthFile);
    if (opts_.isStreamCacheCase) {
        opts_.numRpcThreads = 1;
        opts_.numOcThreadNum = 1;
        opts_.workerGflagParams = " -sc_regular_socket_num=" + std::to_string(opts_.numScRegularSocket)
                                  + " -sc_stream_socket_num=" + std::to_string(opts_.numScStreamSocket) + " "
                                  + opts_.workerGflagParams;
    } else {
        opts_.workerGflagParams = " -sc_regular_socket_num=0 -sc_stream_socket_num=0 " + opts_.workerGflagParams;
    }
    std::string injectActions = "test.start.notWait:call(0)"
                                + (opts_.injectActions.empty() ? "" : ";" + opts_.injectActions)
                                + (opts_.disableRocksDB ? ";master.disableRocksDb:1*call()" : "");
    cmd += " -worker_address=" + opts_.workerConfigs[index].ToString() + " -unix_domain_socket_dir=" + opts_.socketDir
           + " -log_dir=" + logDir + " -v=" + std::to_string(opts_.vLogLevel) + " -health_check_path=" + healthFile
           + " -l2_cache_delete_thread_num=4" + " -inject_actions=" + injectActions + " " + opts_.workerGflagParams
           + " " + gFlag + " -rpc_thread_num=" + std::to_string(opts_.numRpcThreads) + " -spill_directory=" + spillDir
           + " -enable_distributed_master=" + opts_.enableDistributedMaster
           + " -add_node_wait_time_s=" + std::to_string(opts_.addNodeTime) + " -iam_kit=" + opts_.iamKit
           + " -yuanrong_iam_url=" + opts_.yuanrong_iam_url
           + " -rocksdb_background_threads=" + std::to_string(opts_.numRocksDBThreads) + " -zmq_server_io_context="
           + std::to_string(opts_.numZmqServerCtx) + " -oc_thread_num=" + std::to_string(opts_.numOcThreadNum)
           + " -spill_thread_num=" + std::to_string(opts_.numSpillThreadNum) + " -check_async_queue_empty_time_s=1";
    cmd += " -system_access_key=" + opts_.systemAccessKey + " -system_secret_key=" + opts_.systemSecretKey
           + " -tenant_access_key=" + opts_.tenantAccessKey + " -tenant_secret_key=" + opts_.tenantSecretKey;
    cmd += " -oc_worker_worker_direct_port="
           + ((static_cast<size_t>(index) < opts_.workerOcDirectPorts.size())
                  ? std::to_string(opts_.workerOcDirectPorts[index])
                  : "0");

    auto &specifyParams = opts_.workerSpecifyGflagParams;
    if (specifyParams.find(index) != specifyParams.end()) {
        cmd += " " + specifyParams[index];
    }
    if (opts_.isObjectCache) {
        cmd += " -rocksdb_store_dir=" + rootDir + "/rocksdb ";
    }
    if (opts_.enableLivenessProbe) {
        cmd += " -liveness_check_path=" + rootDir + "/liveness ";
    }

    if (opts_.numEtcd > 0) {
        std::string etcdUrl;
        for (const auto &etcdIpAddr : opts_.etcdIpAddrs) {
            if (!etcdUrl.empty()) {
                etcdUrl += ",";
            }
            etcdUrl += etcdIpAddr.first.ToString();
        }
        cmd += " -etcd_address=" + etcdUrl;
    }

    cmd += GetProperMasterAddrParam(index, address);
    if (opts_.numOBS > 0) {
        cmd += FormatString(
            " -l2_cache_type=obs "
            "-obs_endpoint=%s -obs_access_key=3rtJpvkP4zowTDsx6XiE "
            "-obs_secret_key=SJx5Zecs7SL7I6Au9XpylG9LwPF29kMwIxisI5Xs"
            " -obs_bucket=test ",
            opts_.OBSIpAddrs[0].ToString());
    }
    LOG(INFO) << "Launch worker [" << index << "] command: " << cmd;
    auto workerProcess = std::make_unique<WorkerProcess>(cmd, opts_.workerConfigs[index]);
    std::string workerName = "worker_" + std::to_string(index);
    workerProcess->SetEnv({ std::make_pair("POD_NAME", workerName) });
    RETURN_IF_NOT_OK(workerProcess->Start());
    workerProcesses_[index].reset(workerProcess.release());
    if (opts_.waitWorkerReady) {
        RETURN_IF_NOT_OK(WaitNodeReady(WORKER, index, WAIT_TIMEOUT_SECS));
    }
    LOG(INFO) << "Launch worker [" << index << "] success";
    return Status::OK();
}

Status ExternalCluster::RestartWorkerAndWaitReadyOneByOne(std::initializer_list<uint32_t> indexes, int signal,
                                                          int maxWaitTimeSec)
{
    for (auto i : indexes) {
        if (signal == SIGTERM) {
            RETURN_IF_NOT_OK(ShutdownNode(WORKER, i));
        } else {
            RETURN_IF_NOT_OK(KillWorker(i));
        }
        RETURN_IF_NOT_OK(StartNode(WORKER, i, " -client_reconnect_wait_s=1"));
        RETURN_IF_NOT_OK(WaitNodeReady(WORKER, i, maxWaitTimeSec));
    }
    return Status::OK();
}

std::string ExternalCluster::GetProperMasterAddrParam(int index, const HostPort &defaultMasterAddr)
{
    std::string prefix = " -master_address=";
    if (GetEtcdNum() <= 0) {
        return prefix + defaultMasterAddr.ToString();
    }
    if (GetMasterNum() == 0) {
        if (!opts_.crossAZMap.empty()) {
            auto masterIndex = opts_.crossAZMap[index];
            return prefix + opts_.workerConfigs[masterIndex].ToString();
        }
        return prefix + defaultMasterAddr.ToString();
    }
    return "";
}

Status ExternalCluster::StartOBS(int index)
{
    std::string rootDir = opts_.rootDir + "/OBS/";
    if (!FileExist(rootDir)) {
        const int permission = 0700;
        RETURN_IF_NOT_OK(CreateDir(rootDir, true, permission));
    }
    char cwd[PATH_MAX + 1];
    if (getcwd(cwd, sizeof(cwd)) == NULL) {
        RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, "getcwd failed");
    }
    std::string mockOBS(cwd);
    mockOBS += "/../../../tests/st/cluster/mock_obs_service.py";
    if (!FileExist(mockOBS)) {
        RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, "cannot find the mock OBS service at " + mockOBS);
    }
    std::string cmd = FormatString("python3 %s %s %d %s", mockOBS, opts_.OBSIpAddrs[index].Host(),
                                   opts_.OBSIpAddrs[index].Port(), rootDir);
    LOG(INFO) << "Launch mock OBS [" << index << "] command: " << cmd;
    auto csProcess = std::make_unique<OBSProcess>(cmd, opts_.OBSIpAddrs[index]);
    RETURN_IF_NOT_OK(csProcess->Start());
    OBSProcesses_.emplace_back(std::move(csProcess));
    return Status::OK();
}

Status ExternalCluster::StartWorkerAndWaitReady(std::initializer_list<uint32_t> indexes, int maxWaitTimeSec)
{
    for (auto i : indexes) {
        RETURN_IF_NOT_OK(StartWorker(i, HostPort()));
    }
    for (auto i : indexes) {
        RETURN_IF_NOT_OK(WaitNodeReady(WORKER, i, maxWaitTimeSec));
    }
    return Status::OK();
}

Status ExternalCluster::StartWorkerAndWaitReady(const std::initializer_list<uint32_t> indexes,
                                                const std::string &workerFlags, int maxWaitTimeSec)
{
    for (auto i : indexes) {
        RETURN_IF_NOT_OK(StartWorker(i, HostPort(), workerFlags));
    }
    for (auto i : indexes) {
        RETURN_IF_NOT_OK(WaitNodeReady(WORKER, i, maxWaitTimeSec));
    }
    return Status::OK();
}

Status ExternalCluster::StartWorkerAndWaitReady(const std::initializer_list<uint32_t> indexes,
                                                const std::unordered_map<uint32_t, std::string> &workerFlags,
                                                int maxWaitTimeSec, const std::string &defaultWorkerFlags)
{
    for (auto i : indexes) {
        std::string flags = " " + defaultWorkerFlags;
        auto iter = workerFlags.find(i);
        if (iter != workerFlags.end()) {
            flags = " " + iter->second;
        }
        RETURN_IF_NOT_OK(StartWorker(i, HostPort(), flags));
    }
    for (auto i : indexes) {
        RETURN_IF_NOT_OK(WaitNodeReady(WORKER, i, maxWaitTimeSec));
    }
    return Status::OK();
}

ExternalClusterOptions::ExternalClusterOptions()
    : BaseClusterOptions(),
      rootDir(std::string(LLT_BIN_PATH) + "/ds"),
      maxLogSizeMb(DEFAULT_maxLogSizeMb),
      totalLogSizeMb(DEFAULT_totalLogSizeMb),
      logRollingSecs(DEFAULT_logRollingSecs),
      vLogLevel(1),
      logCompress(true),
      logAsync(true),
      isObjectCache(true),
      masterIdx(0),
      addNodeTime(0),
      waitWorkerReady(true),
      enableLivenessProbe(false),
      skipWorkerPreShutdown(true),
      isStreamCacheCase(false),
      disableRocksDB(true)
{
    if (isObjectCache) {
        numMasters = 0;
    }
}
}  // namespace st
}  // namespace datasystem
