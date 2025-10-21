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
 * Description: base class of cluster, InternalCluster and ExternalCluster extends from this class.
 */
#ifndef DATASYSTEM_TEST_ST_CLUSTER_BASE_CLUSTER_H
#define DATASYSTEM_TEST_ST_CLUSTER_BASE_CLUSTER_H

#include <atomic>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <securec.h>

#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/log/log.h"
#include "subprocess.h"

#define ASSIGN_IF_NOT_OK(lastRc_, statement_)    \
    do {                                         \
        Status rc_ = (statement_);               \
        if (rc_.IsError()) {                     \
            LOG(ERROR) << rc_.ToString() << " "; \
            lastRc_ = rc_;                       \
        }                                        \
    } while (false)

namespace datasystem {
namespace st {
enum ClusterNodeType {
    ALL = 0,     // Operate all nodes.
    MASTER = 1,  // Operate all master nodes.
    WORKER = 3,  // Operate all worker nodes.
    ETCD = 4,    // Operate all etcd nodes.
    OBS = 5
};

constexpr int DEFAULT_SLEEP_FOR_TIME = 500;

struct BaseClusterOptions {
    BaseClusterOptions()
        : numMasters(DEFAULT_COMPONENT_NUM),
          numWorkers(DEFAULT_COMPONENT_NUM),
          numRpcThreads(DEFAULT_THREAD_NUM),
          numRocksDBThreads(DEFAULT_THREAD_NUM),
          numZmqServerCtx(DEFAULT_ZMQ_SERVER_IO_CTX_NUM),
          numOcThreadNum(DEFAULT_THREAD_NUM),
          numSpillThreadNum(DEFAULT_THREAD_NUM),
          numEtcd(0),
          numOBS(0)
    {
    }

    // Number of masters to be started. numMasters must be less than or equal to the number of masterIpAddrs. When
    // numMasters is less than the number of masterIpAddrs, the first numMasters IP addresses are used.
    // No master exists in object cache.
    uint32_t numMasters;

    // Number of workers to be started. numWorkers must be less than or equal to the number of workerConfigs. When
    // numMasters is less than the number of workerConfigs, the first numWorkers IP addresses are used.
    uint32_t numWorkers;

    // Number of threads used by DsWorker to process RPC requests.
    // Default: 4
    uint32_t numRpcThreads;

    // Number of background threads rocksdb can use for flushing and compacting.
    // Default: 4
    uint32_t numRocksDBThreads;

    // Optimize the performance of the customer.
    // Default: 1
    uint32_t numZmqServerCtx;

    // The number of worker service for object cache.
    // Default: 4
    uint32_t numOcThreadNum;

    // It represents the maximum parallelism of writing files.
    // Default: 4
    uint32_t numSpillThreadNum;

    // Master ip address.
    // Default: None. Must be specified by the user.
    std::vector<HostPort> masterIpAddrs;

    // Worker ip address.
    // Default: None. Must be specified by the user.
    std::vector<HostPort> workerConfigs;

    // Extra tcp/ip port for worker <-> worker direct connection.
    // One for WorkerWorkerOCService
    // Default: None. It is optional but its size must match workerConfigs
    std::vector<int> workerOcDirectPorts;

    uint32_t numEtcd;

    // One etcd node requires two ports.
    std::vector<std::pair<HostPort, HostPort>> etcdIpAddrs;

    uint32_t numOBS;
    // obs ip address
    std::vector<HostPort> OBSIpAddrs;

private:
    static const int DEFAULT_THREAD_NUM = 4;
    static const int DEFAULT_COMPONENT_NUM = 1;
    static const uint32_t DEFAULT_GCS_HEARTBEAT_MS = 50;
    static const int DEFAULT_ZMQ_SERVER_IO_CTX_NUM = 2;
};

class ServerProcess : public Subprocess {
public:
    ServerProcess(const std::string &cmd, const HostPort &addr)
        : Subprocess(cmd),
          addr_(addr),
          rpcSession_(std::make_unique<GenericService_Stub>(std::make_shared<RpcChannel>(addr, RpcCredential())))
    {
        const int timeoutMs = 500;
        opts_.SetTimeout(timeoutMs);
    }

    ~ServerProcess() override = default;

    Status Shutdown() override
    {
        Status status = GcovFlush();
        if (status.IsError()) {
            LOG(ERROR) << "GcovFlush failed:" << status.ToString();
        }
        return Subprocess::Shutdown();
    }

    const HostPort &GetAddr() const
    {
        return addr_;
    }

    Status GcovFlush() const
    {
        LOG(INFO) << FormatString("Process %s start to send flush coverage request.", addr_.ToString());
        GcovFlushReqPb req;
        GcovFlushRspPb rsp;
        RETURN_IF_NOT_OK(rpcSession_->GcovFlush(opts_, req, rsp));
        LOG(INFO) << FormatString("Process %s flush coverage request succeed.", addr_.ToString());
        return Status::OK();
    }

    Status SetInjectAction(const std::string &name, const std::string &action)
    {
        LOG(INFO) << FormatString("Set inject action of %s in %s.", name, addr_.ToString());
        if (action == "abort()") {
            SetInjectAbort();
        }
        datasystem::SetInjectActionReqPb req;
        datasystem::SetInjectActionRspPb rsp;
        req.set_name(name);
        req.set_action(action);
        RETURN_IF_NOT_OK(rpcSession_->SetInjectAction(opts_, req, rsp));

        return Status::OK();
    }

    Status ClearInjectAction(const std::string &name) const
    {
        LOG(INFO) << FormatString("Clear inject action of %s in %s.", name, addr_.ToString());
        datasystem::ClearInjectActionReqPb req;
        datasystem::ClearInjectActionRspPb rsp;
        req.set_name(name);
        RETURN_IF_NOT_OK(rpcSession_->ClearInjectAction(opts_, req, rsp));

        return Status::OK();
    }

    Status GetInjectActionExecuteCount(const std::string &name, uint64_t &executeCount) const
    {
        datasystem::GetInjectActionExecuteCountReqPb req;
        datasystem::GetInjectActionExecuteCountRspPb rsp;
        req.set_name(name);
        RETURN_IF_NOT_OK(rpcSession_->GetInjectActionExecuteCount(opts_, req, rsp));
        executeCount = rsp.execute_count();
        return Status::OK();
    }

    void SetRpcSession(const RpcCredential &cred)
    {
        rpcSession_ = std::make_unique<GenericService_Stub>(std::make_shared<RpcChannel>(addr_, cred));
    }

protected:
    HostPort addr_;
    std::unique_ptr<GenericService_Stub> rpcSession_;
    RpcOptions opts_;
};

class BaseCluster {
public:
    BaseCluster() = default;

    virtual ~BaseCluster() = default;

    /**
     * @brief Start cluster.
     * @param[in] timeoutSecs timeout seconds.
     * @return Status::OK() if success.
     */
    virtual Status Start(int timeoutSecs = WAIT_TIMEOUT_SECS)
    {
        DCHECK(!isRunning_);
        RETURN_IF_NOT_OK(StartEtcdCluster());
        RETURN_IF_NOT_OK(StartOBS());
        RETURN_IF_NOT_OK(StartWorkers());
        RETURN_IF_NOT_OK(WaitUntilClusterReadyOrTimeout(timeoutSecs));
        // Need to wait a bit before gcs send a heartbeat to register itself.
        std::this_thread::sleep_for(std::chrono::milliseconds(DEFAULT_SLEEP_FOR_TIME));
        return Status::OK();
    }

    /**
     * @brief Shutdown all cluster.
     * @return Status::OK() if success.
     */
    virtual Status Shutdown()
    {
        isRunning_ = false;
        Status status = Status::OK();
        ASSIGN_IF_NOT_OK(status, ShutdownNodes(ClusterNodeType::ALL));
        ASSIGN_IF_NOT_OK(status, ClearSocketDir());
        return status;
    }

    Status ExecuteCmd(const std::string &cmd, std::string &result, int *exitCode = nullptr);

    /**
     * @brief Shutdown all clusters, master cluster or gcs cluster.
     * @param[in] nodeType of the node to be shut down
     * @return Status::OK() if success.
     */
    virtual Status ShutdownNodes(ClusterNodeType nodeType) = 0;

    /**
     * @brief Shutdown worker, master or gcs.
     * @param[in] nodeType Type of the node to be shut down
     * @param[in] idx index of node.
     * @return Status::OK() if success.
     */
    virtual Status ShutdownNode(ClusterNodeType nodeType, uint32_t idx) = 0;

    /**
     * @brief Quickly Shutdown worker, skip checkAsyncTasks and voluntaryScaleDown.
     * @param[in] idx index of node.
     * @return Status::OK() if success.
     */
    virtual Status QuicklyShutdownWorker(uint32_t idx) = 0;

    /**
     * @brief Start worker, master or gcs.
     * @param[in] nodeType Type of the node to be start
     * @param[in] idx index of node
     * @param[in] params startup parameter
     * @return Status::OK() if success.
     */
    virtual Status StartNode(ClusterNodeType nodeType, uint32_t idx, const std::string &params) = 0;

    /**
     * @brief Start worker, master or gcs.
     * @param[in] nodeType Type of the node to be start
     * @param[in] idx index of node
     * @param[in] params startup parameter
     * @return Status::OK() if success.
     */
    virtual Status GetProcess(ClusterNodeType nodeType, uint32_t idx, ServerProcess *&process) = 0;

    /**
     * @brief Check worker alive.
     * @param[in] nodeType Type of the node to be start
     * @param[in] idx index of node
     * @return Status::OK() if success.
     */
    virtual bool CheckWorkerProcess(uint32_t idx) = 0;

    /**
     * @brief Set inject action for worker, master or gcs.
     * @param[in] nodeType Type of the node to be start
     * @param[in] idx index of node
     * @param[in] name Inject point name
     * @param[in] action Inject action name
     * @return Status::OK() if success.
     */
    virtual Status SetInjectAction(ClusterNodeType nodeType, uint32_t idx, const std::string &name,
                                   const std::string &action) = 0;

    /**
     * @brief Add one worker
     * @param masterAddress master address
     * @param workerAddress worker address
     * @param workerDirectPort worker <-> worker direct port. 0 == disabled.
     * @return
     */
    virtual Status AddNode(const HostPort &masterAddress, const std::string workerAddress, int workerDirectPort) = 0;

    /**
     * @brief Clear inject action for worker, master or gcs.
     * @param[in] nodeType Type of the node to be start
     * @param[in] idx index of node
     * @param[in] name Inject point name
     * @return Status::OK() if success.
     */
    virtual Status ClearInjectAction(ClusterNodeType nodeType, uint32_t idx, const std::string &name) = 0;

    /**
     * @brief Get inject action execute count for worker, master or gcs.
     * @param[in] nodeType Type of the node to be start
     * @param[in] idx index of node
     * @param[in] name Inject point name
     * @param[out] executeCout The inject action execute count
     * @return Status::OK() if success.
     */
    virtual Status GetInjectActionExecuteCount(ClusterNodeType nodeType, uint32_t idx, const std::string &name,
                                               uint64_t &executCount) = 0;

    /**
     * @brief Get the number of etcd.
     * @return the number of etcd.
     */
    virtual size_t GetEtcdNum() const = 0;

    /**
     * @brief Get the number of master.
     * @return the number of master.
     */
    virtual size_t GetMasterNum() const = 0;

    /**
     * @brief Get the number of gcs.
     * @return the number of gcs.
     */
    virtual size_t GetGCSNum() const = 0;

    /**
     * @brief Get the number of workers.
     * @return the number of workers.
     */
    virtual size_t GetWorkerNum() const = 0;

    /**
     * @brief Get IP address of obs.
     * @param[in] idx index of obs.
     * @param[out] addr return address.
     * @return Status::OK() if success.
     */
    virtual Status GetOBSAddr(uint32_t idx, HostPort &addr) const = 0;

    /**
     * @brief Get IP address of a etcd.
     * @param[in] idx index of etcd.
     * @param[out] addrs return address.
     * @return Status::OK() if success.
     */
    virtual Status GetEtcdAddrs(uint32_t idx, std::pair<HostPort, HostPort> &addrs) const = 0;

    /**
     * @brief Get IP address of a etcd.
     * @return The etcd cluster address.
     */
    virtual std::string GetEtcdAddrs() const = 0;

    /**
     * @brief Get IP address of a master.
     * @param[in] idx index of master.
     * @param[out] addr return address.
     * @return Status::OK() if success.
     */
    virtual Status GetMasterAddr(uint32_t idx, HostPort &addr) const = 0;

    /**
     * @brief Get IP address of gcs.
     * @param[in] idx index of gcs.
     * @param[out] addr return address.
     * @return Status::OK() if success.
     */
    virtual Status GetGCSAddr(uint32_t idx, HostPort &addr) const = 0;

    /**
     * @brief Get IP address of workers.
     * @param[in] idx index of workers.
     * @param[out] addr return address.
     * @return Status::OK() if success.
     */
    virtual Status GetWorkerAddr(uint32_t idx, HostPort &addr) const = 0;

    /**
     * @brief Get IP address of meta server. Used only in object cache. It is the first address in opts_.workerConfigs
     * @param[out] addr return address.
     * @return Status::OK() if success.
     */
    virtual Status GetMetaServerAddr(HostPort &addr) const = 0;

    /**
     * @brief Get process id of etcd.
     * @param[in] idx index of etcd.
     * @return return process id
     */
    virtual pid_t GetEtcdPid(uint32_t idx) const = 0;

    /**
     * @brief Get process id of master.
     * @param[in] idx index of master.
     * @return return process id
     */
    virtual pid_t GetMasterPid(uint32_t idx) const = 0;

    /**
     * @brief Get process id of gcs.
     * @param[in] idx index of gcs.
     * @return return process id
     */
    virtual pid_t GetGCSPid(uint32_t idx) const = 0;

    /**
     * @brief Get process id of worker.
     * @param[in] idx index of worker.
     * @return return process id
     */
    virtual pid_t GetWorkerPid(uint32_t idx) const = 0;

    /**
     * @brief Start etcd cluster, called in Start method.
     * @return Status::OK() if success.
     */
    virtual Status StartEtcdCluster() = 0;

    /**
     * @brief Start mock obs service, called in Start method.
     * @return Status::OK() if success.
     */
    virtual Status StartOBS() = 0;

    /**
     * @brief Start master nodes, called in Start method.
     * @return Status::OK() if success.
     */
    virtual Status StartMasters() = 0;

    /**
     * @brief Start worker nodes, called in Start method.
     * @return Status::OK() if success.
     */
    virtual Status StartWorkers() = 0;

    /**
     * @brief Wait util cluster is ready or timeout, called in Start method.
     * @param[in] timeoutSecs timeout seconds.
     * @return Status::OK() if  cluster is successfully started within the timeout period.
     */
    virtual Status WaitUntilClusterReadyOrTimeout(int timeoutSecs) = 0;

    virtual Status KillLeaderEtcd() = 0;

    /**
     * @brief Clear the root directory.
     * @return Status::OK() if success.
     */
    virtual Status ClearRootDir() = 0;

    /**
     * @brief Clear the domain socket directory.
     * @return Status::OK() if success.
     */
    virtual Status ClearSocketDir() = 0;

    /**
     * @brief Get domain socket directory.
     * @return Return directory of domain socket.
     */
    virtual std::string GetSocketDir() = 0;

    /**
     * @brief Get directory storing the fragment files
     * @return Return the directory storing the fragment files
     */
    virtual std::vector<std::string> GetGCSFragmentStoreDirs(uint32_t idx) = 0;

    /**
     * @brief Get the NAS directory
     * @return return the NAS directory
     */
    virtual std::string GetNASDir() = 0;

    /**
     * @brief Run until Node at index is ready
     * @return Status of call
     */
    virtual Status WaitNodeReady(ClusterNodeType nodeType, uint32_t index, int timeoutSecs = WAIT_TIMEOUT_SECS) = 0;

    virtual bool IsWorkerLiveness(uint32_t index) = 0;

    /**
     * @brief Run function until expect result
     * @param function The function to run
     * @param timeoutSecs The timeout seconds
     * @param expectedStatusCode The expected StatusCode of the function
     * @return Status of call
     */
    virtual Status WaitForExpectedResult(std::function<Status()> function, int timeoutSecs,
                                         StatusCode expectedStatusCode) = 0;

    virtual std::string GetRootDir() const = 0;

    virtual Status KillWorker(uint32_t idx) = 0;

protected:
    std::atomic<bool> isRunning_{ false };

    static const int WAIT_TIMEOUT_SECS = 150;
};
}  // namespace st
}  // namespace datasystem

#endif  // DATASYSTEM_TEST_ST_COMMON_BASE_CLUSTER_H
