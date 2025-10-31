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
#ifndef DATASYSTEM_TEST_ST_CLUSTER_EXTERNAL_CLUSTER_H
#define DATASYSTEM_TEST_ST_CLUSTER_EXTERNAL_CLUSTER_H

#include <memory>
#include <mutex>
#include <vector>
#include <string>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/rpc/rpc_channel.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/protos/generic_service.stub.rpc.pb.h"

#include "cluster/base_cluster.h"
#include "cluster/subprocess.h"

DS_DECLARE_bool(enable_curve_zmq);

namespace datasystem {
namespace st {
struct ExternalClusterOptions : public BaseClusterOptions {
public:
    ExternalClusterOptions();

    // Root directory of log files, which is used to store subdirectories of DsMaster and DsGCS files.
    // Default: ./
    std::string rootDir;

    // The socket dir
    std::string socketDir;

    // Maximum size of single log file, unit is MB.
    // Default: 100
    uint32_t maxLogSizeMb;

    // Maximum size of all log files, unit is MB.
    // Default: 1000
    uint32_t totalLogSizeMb;

    // Log rolling interval, unit is second.
    // Default: 5
    uint32_t logRollingSecs;

    // Default: 0
    uint32_t vLogLevel;

    // Log compress.
    // Default: false
    bool logCompress;

    // Async log flush.
    // Default: true
    bool logAsync;

    // on for object cache, off for file cache
    bool isObjectCache;

    // Store the map of worker and master, key is worker index and value is master index.
    // When start workers with crossAZMap, master_address will set by the map.
    std::map<uint32_t, uint32_t> crossAZMap;

    // Parameters for starting the worker
    // For example, "-shared_memory_size_mb=1024"
    std::string workerGflagParams;

    // Parameter for specific worker.
    // For example, "{1, "-v=3"}" means only worker 1 set speific param -v=3, other workers don't set the flags.
    std::map<uint32_t, std::string> workerSpecifyGflagParams;

    // Parameters for starting the master
    std::string masterGflagParams;

    // Parameters for starting the gcs
    std::string gcsGflagParams;

    // Path for writing to NAS.  Special keyword of "none" means read-only NAS
    std::string nasDir;

    std::string etcdPrefix;

    std::string enableDistributedMaster = "true";  // enable distributed master or not.

    bool enableSpill = false;

    // Used in object cache, where master is part of worker process. We use this index to tell which worker will work
    // as the master. workerConfigs[masterIdx] is the address of the master. masterIdx is 0 by default.
    int32_t masterIdx;

    int addNodeTime;

    bool waitWorkerReady;

    bool enableLivenessProbe;
    // used by ak/sk authorize
    std::string systemAccessKey = "QTWAOYTTINDUT2QVKYUC";
    std::string systemSecretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    std::string tenantAccessKey;
    std::string tenantSecretKey;
    std::string injectActions;

    // Skip the shutdown process to accelerate worker exit.
    bool skipWorkerPreShutdown;

    // Disable rocksdb, default true
    bool disableRocksDB;

private:
    static const uint32_t DEFAULT_maxLogSizeMb = 100;
    static const uint32_t DEFAULT_totalLogSizeMb = 1000;
    static const uint32_t DEFAULT_logRollingSecs = 5;
};

class MasterProcess : public ServerProcess {
public:
    MasterProcess(const std::string &cmd, const HostPort &addr) : ServerProcess(cmd, addr)
    {
    }

    ~MasterProcess() override = default;
};

class GCSProcess : public ServerProcess {
public:
    GCSProcess(const std::string &cmd, const HostPort &addr, const std::string &nodeId)
        : ServerProcess(cmd, addr), nodeId_(nodeId)
    {
    }

    ~GCSProcess() override = default;

    std::string GetNodeId() const
    {
        return nodeId_;
    }

private:
    std::string nodeId_;
};

class WorkerProcess : public ServerProcess {
public:
    WorkerProcess(const std::string &cmd, const HostPort &addr) : ServerProcess(cmd, addr)
    {
    }

    ~WorkerProcess() override = default;
};

class EtcdProcess : public Subprocess {
public:
    EtcdProcess(const std::string &cmd, const std::pair<HostPort, HostPort> &addrs) : Subprocess(cmd), addrs_(addrs)
    {
    }

    ~EtcdProcess() override = default;

    Status Start() override
    {
        if (firstStart_) {
            cmd_ += " --initial-cluster-state new";
        } else {
            cmd_ += " --initial-cluster-state existing";
        }
        RETURN_IF_NOT_OK(Subprocess::Start());
        firstStart_ = false;
        return Status::OK();
    }

    const std::pair<HostPort, HostPort> &GetAddrs() const
    {
        return addrs_;
    }

protected:
    std::pair<HostPort, HostPort> addrs_;
    bool firstStart_ = true;
};

class OBSProcess : public Subprocess {
public:
    OBSProcess(const std::string &cmd, const HostPort &addr) : Subprocess(cmd), addr_(addr)
    {
    }
 
    ~OBSProcess() override = default;
    const HostPort &GetAddr() const
    {
        return addr_;
    }

protected:
    HostPort addr_;
};

class ExternalCluster : public BaseCluster {
public:
    explicit ExternalCluster(const ExternalClusterOptions &opt);

    ~ExternalCluster() override = default;

    Status ShutdownNodes(ClusterNodeType nodeType) override;

    Status ShutdownNode(ClusterNodeType nodeType, uint32_t idx) override;

    Status ShutdownWorkers();

    /**
     * @brief Quickly Shutdown worker, skip checkAsyncTasks and voluntaryScaleDown.
     * @param[in] idx index of node.
     * @return Status::OK() if success.
     */
    Status QuicklyShutdownWorker(uint32_t idx) override;

    Status KillWorker(uint32_t idx);

    Status ShutdownMasters();

    Status ShutdownEtcds();

    Status ShutdownOBSs();

    Status StartNode(ClusterNodeType nodeType, uint32_t idx, const std::string &params) override;

    Status GetProcess(ClusterNodeType nodeType, uint32_t idx, Subprocess *&process);

    Status GetProcess(ClusterNodeType nodeType, uint32_t idx, ServerProcess *&process) override;

    Status SetInjectAction(ClusterNodeType nodeType, uint32_t idx, const std::string &name,
                           const std::string &action) override;

    Status ClearInjectAction(ClusterNodeType nodeType, uint32_t idx, const std::string &name) override;

    Status GetInjectActionExecuteCount(ClusterNodeType nodeType, uint32_t idx, const std::string &name,
                                       uint64_t &executCount);

    bool CheckWorkerProcess(uint32_t idx) override;

    size_t GetEtcdNum() const override;

    size_t GetMasterNum() const override;

    size_t GetGCSNum() const override;

    size_t GetWorkerNum() const override;

    Status GetEtcdAddrs(uint32_t idx, std::pair<HostPort, HostPort> &addrs) const override;

    std::string GetEtcdAddrs() const override;

    Status GetMasterAddr(uint32_t idx, HostPort &addr) const override;

    Status GetGCSAddr(uint32_t idx, HostPort &addr) const override;

    Status GetWorkerAddr(uint32_t idx, HostPort &addr) const override;

    Status GetOBSAddr(uint32_t idx, HostPort &addr) const override;

    Status GetMetaServerAddr(HostPort &addr) const override;

    pid_t GetEtcdPid(uint32_t idx) const override;

    pid_t GetMasterPid(uint32_t idx) const override;

    pid_t GetGCSPid(uint32_t idx) const override;

    pid_t GetWorkerPid(uint32_t idx) const override;

    Status StartEtcdCluster() override;

    Status StartMasters() override;

    Status StartWorkers() override;

    Status StartOBS() override;

    Status WaitUntilClusterReadyOrTimeout(int timeoutSecs) override;

    Status KillLeaderEtcd() override;

    Status ClearRootDir() override;

    Status ClearSocketDir() override;

    std::string GetSocketDir() override;

    std::vector<std::string> GetGCSFragmentStoreDirs(uint32_t idx) override;

    std::string GetNASDir() override;

    Status WaitNodeReady(ClusterNodeType nodeType, uint32_t index, int timeoutSecs) override;

    bool IsWorkerLiveness(uint32_t index) override;

    Status AddNode(const HostPort &masterAddress, const std::string workerAddress, int workerDirectPort) override;

    Status WaitForExpectedResult(std::function<Status()> function, int timeoutSecs,
                                 StatusCode expectedStatusCode) override;

    std::string GetRootDir() const override
    {
        return opts_.rootDir;
    }

    // address is master address for file cache, and meta server address for centralized master in object cache
    Status StartWorker(int index, const HostPort &address, std::string gFlag = "");

    ExternalClusterOptions GetExternalOptions() const
    {
        return opts_;
    }

    std::string AddGFlagForWorkerStart(std::string &flag);

    void SetGFlagForWorkerStart(std::string &flag);

    // Start a control process that is just to start the worker process for the purpose of circumventing the
    // constraints.
    Status StartForkWorkerProcess();

    // Start a worker by fork worker process.
    Status StartWorkerByForkProcess(int index);

    // Send finish to control process to stop while.
    Status FinishForkWorkerProcess();

    // Restart worker and wait ready one by one.
    Status RestartWorkerAndWaitReadyOneByOne(std::initializer_list<uint32_t> indexes, int signal = SIGTERM,
                                             int maxWaitTimeSec = 40);

    // Start worker and wait ready.
    Status StartWorkerAndWaitReady(std::initializer_list<uint32_t> indexes, int maxWaitTimeSec = 20);

    // Start worker with custom configuration and wait ready.
    Status StartWorkerAndWaitReady(const std::initializer_list<uint32_t> indexes, const std::string &workerFlags,
                                   int maxWaitTimeSec = 20);

    // Start worker with custom configuration and wait ready.
    Status StartWorkerAndWaitReady(const std::initializer_list<uint32_t> indexes,
                                   const std::unordered_map<uint32_t, std::string> &workerFlags,
                                   int maxWaitTimeSec = 20, const std::string &defaultWorkerFlags = "");

protected:
    Status StartEtcdNode(int index);

    Status StartMaster(int index);

    Status StartOBS(int index);

    Status WaitEtcdReadyOrTimeout(int timeoutSecs);

    std::string ConstructEtcdCheckHealthCmd() const;

    Status CheckProbeFile(const std::string &filepath, int timeoutSecs, pid_t pid);

    Status CheckIpPortListen(const std::string &addr, int timeoutSecs);

    // get proper master address param.
    std::string GetProperMasterAddrParam(int index, const HostPort &defaultMasterAddr);

    ExternalClusterOptions opts_;

    std::vector<std::unique_ptr<EtcdProcess>> etcdProcesses_;

    std::mutex mutex_master_;
    std::vector<std::unique_ptr<MasterProcess>> masterProcesses_;

    std::vector<std::pair<std::unique_ptr<GCSProcess>, bool>> gcsProcesses_;

    std::mutex mutex_worker_;
    std::vector<std::unique_ptr<WorkerProcess>> workerProcesses_;

    std::vector<std::unique_ptr<OBSProcess>> OBSProcesses_;

    RandomData randomData_;

    // Control process pid.
    pid_t workerForkPid_ = -1;

    // Send msg by pipe.
    int pipeFd_[2];

    // Msg between ds_llt and control process.
    int32_t message_;
};
}  // namespace st
}  // namespace datasystem

#endif  // DATASYSTEM_TEST_ST_CLUSTER_EXTERNAL_CLUSTER_H
