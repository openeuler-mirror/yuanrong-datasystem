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
 * Description: The interface of worker server.
 */
#ifndef DATASYSTEM_WORKER_WORKER_OC_SERVER_H
#define DATASYSTEM_WORKER_WORKER_OC_SERVER_H

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/l2cache/persistence_api.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/common/util/wait_post.h"
#include "datasystem/master/master_service_impl.h"
#include "datasystem/master/object_cache/master_oc_service_impl.h"
#include "datasystem/master/replica_manager.h"
#include "datasystem/master/replication_service_impl.h"
#include "datasystem/master/stream_cache/master_sc_service_impl.h"
#include "datasystem/server/common_server.h"
#include "datasystem/worker/cluster_manager/etcd_cluster_manager.h"
#include "datasystem/worker/object_cache/master_worker_oc_service_impl.h"
#include "datasystem/worker/object_cache/worker_oc_service_impl.h"
#include "datasystem/worker/object_cache/worker_worker_oc_service_impl.h"
#include "datasystem/worker/object_cache/worker_worker_transport_service_impl.h"
#include "datasystem/worker/stream_cache/client_worker_sc_service_impl.h"
#include "datasystem/worker/stream_cache/master_worker_sc_service_impl.h"
#include "datasystem/worker/stream_cache/worker_worker_sc_service_impl.h"
#include "datasystem/worker/worker_liveness_check.h"
#include "datasystem/worker/worker_service_impl.h"
#include "datasystem/common/kvstore/metastore/metastore_server.h"
#ifdef WITH_TESTS
#include "datasystem/../../tests/st/st_oc_service_impl.h"
#endif
#ifdef ENABLE_PERF
#include "datasystem/worker/perf_service/perf_service_impl.h"
#endif

namespace datasystem {
namespace worker {
class WorkerOCServer : public CommonServer {
public:
    /**
     * @brief Create a new WorkerServer object.
     */
    WorkerOCServer(HostPort workerAddr, HostPort bindAddr, HostPort masterAddr)
        : CommonServer(std::move(workerAddr), std::move(bindAddr)), masterAddr_(std::move(masterAddr))
    {
    }

    ~WorkerOCServer() override;

    /**
     * @brief Initialize services such as register Rpc Service before Start WorkerServer.
     * @return Status of the call.
     */
    Status Init() override;

    /**
     * @brief Start WorkerServer.
     * @return Status of the call.
     */
    Status Start() override;

    /**
     * @brief Shutdown WorkerServer.
     * @return Status of the call.
     */
    Status Shutdown() override;

    /**
     * @brief Get the pointer information for shared memory communication
     * @param[int] lockId Get shm info by lock id.
     * @param[out] fd File descriptor of the allocated shared memory segments.
     * @param[out] mmapSize Total size of shared memory segments.
     * @param[out] offset Offset from the base of the shared memory mmap.
     * @param[out] id The id of this shmUnit.
     * @return Status of the call.
     */
    Status GetShmQueueUnit(uint32_t lockId, int &fd, uint64_t &mmapSize, ptrdiff_t &offset, ShmKey &id) override;

    /**
     * @brief After restart crashed server, we need to do some recovery job according to the message from the client.
     * @param[in] clientId The id of the client
     * @param[in] tenantId The tenantId
     * @param[in] reqToken Need construct obj uri.
     * @param[in] msg The message from the client
     * @return Status of the call
     */
    Status ProcessServerReboot(const ClientKey &clientId, const std::string &tenantId, const std::string &reqToken,
                               const google::protobuf::RepeatedPtrField<google::protobuf::Any> &msg) override;

    Status GetExclConnSockPath(std::string &sockPath) override;

    /**
     * @brief Register a client to client manager.
     * @param[in] clientId The clientId.
     * @param[in] shmEnabled Indicates whether the client allows shared memory.
     * @param[in] socketFd The unix domain socket Fd.
     * @param[in] tenantId The tenant id.
     * @param[in] enableCrossNode Client is enable cross node connection or not.
     * @param[in] podName Client pod name.
     * @param[in] supportMultiShmRefCount Indicates whether the client supports multiple shared memory references.
     * @param[out] lockId The lock id.
     * @return Status of the call.
     */
    Status AddClient(const ClientKey &clientId, bool shmEnabled, int32_t socketFd, const std::string &tenantId,
                     bool enableCrossNode, const std::string &podName, bool supportMultiShmRefCount,
                     uint32_t &lockId) override;

    /**
     * @brief Check unfinished asynchronous tasks and active scale-in process if exists.
     */
    Status PreShutDown();

    master::MasterOCServiceImpl *GetOcMetaSvc()
    {
        return objCacheMasterSvc_.get();
    }

    WorkerServiceImpl *GetWorkerService()
    {
        return workerSvc_.get();
    }

    object_cache::WorkerOCServiceImpl *GetWorkerOCService()
    {
        return objCacheClientWorkerSvc_.get();
    }

private:
    /**
     * @brief Init the access key and secret key for AK/SK authentication.
     * @return Status of the call.
     */
    Status InitAkSk();

    /**
     * @brief ReadinessProbe for worker service.
     * @return Status of the call.
     */
    Status ReadinessProbe();

    /**
     * @brief General method of cleaning the data of a client while the client disconnecting.
     * @param[in] clientId The client id of the corresponding client with the socket fd.
     */
    void AfterClientLostHandler(const ClientKey &clientId) override;

    /**
     * @brief Init object service for client request.
     * @return Status of the call.
     */
    Status InitWorkerOCService();

    /**
     * @brief Init object service for worker request.
     * @return Status of the call.
     */
    Status InitWorkerWorkerOCService();

    /**
     * @brief Init object service for worker worker fast transport.
     * @return Status of the call.
     */
    Status InitWorkerWorkerTransportService();

    /**
     * @brief Init object service for master request.
     * @return Status of the call.
     */
    Status InitMasterWorkerOCService();

    /**
     * @brief Init stream cache service for client request.
     * @return Status of the call.
     */
    Status InitClientWorkerSCService();

    /**
     * @brief Init stream cache service for worker request.
     * @return Status of the call.
     */
    Status InitWorkerWorkerSCService();

    /**
     * @brief Init stream cache service for master request.
     * @return Status of the call.
     */
    Status InitMasterWorkerSCService();

    /**
     * @brief Init common service for client request.
     * @return Status of the call.
     */
    Status InitWorkerService();

    /**
     * @brief Init common service for worker request.
     * @return Status of the call.
     */
    Status InitMasterService();

    /**
     * @brief Init object service for worker request.
     * @return Status of the call.
     */
    Status InitMasterOCService();

    /**
     * @brief Init stream cache service for worker request.
     * @return Status of the call.
     */
    Status InitMasterSCService();

    /**
     * @brief Init rocksdb replica service for worker request.
     * @return Status of the call.
     */
    Status InitReplicaService();

#ifdef WITH_TESTS
    /**
     * @brief Init service for requests from ut.
     * @return Status of the call.
     */
    Status InitUtOCService();
#endif

#ifdef ENABLE_PERF
    /**
     * @brief Init Perf Service.
     */
    Status CreateAndInitPerfService();
#endif

    /**
     * @brief Create all services above.
     */
    void CreateAllServices();

    /**
     * @brief Create all services related to master.
     */
    void CreateMasterServices();

    /**
     * @brief Create all services related to worker.
     */
    void CreateWorkerServices();

    /**
     * @brief Initialize all services above.
     * @return Status of the call.
     */
    Status InitializeAllServices(const ClusterInfo &clusterInfo);

    /**
     * @brief Initialize master services only.
     * @return Status of the call.
     */
    Status InitializeMasterServices(const ClusterInfo &clusterInfo);

    /**
     * @brief Initialize worker services only.
     * @return Status of the call.
     */
    Status InitializeWorkerServices();

    /**
     * @brief Hook up pointers between services that want to have local bypass optimzation for "same node" communication
     */
    void EnableLocalBypass();

    /**
     * @brief The Rule of checking async tasks.
     * @note This thread checks the async tasks status every seconds.
     *       If an async task is running, the thread set status to "prestop_status:wait".
     *       If checking no async tasks for five consecutive times, the thread set status to "prestop_status:ready".
     * @param[in] isAsyncTasksRunning Check whether an asynchronous task is running.
     * @param[in|out] checkNum Number of consecutive times for checking no async tasks.
     */
    void CheckRule(bool isAsyncTasksRunning, int &checkNum);

    /**
     * @brief Check whether there are unfinished asynchronous tasks.
     */
    void CheckAsyncTasks();

    /**
     * @brief Set checkAsyncTasksDone_ flag and notify waiting threads.
     * @param[in] value The value to set for checkAsyncTasksDone_.
     */
    void SetCheckAsyncTasksDone(bool value);

    /**
     * @brief Create all service.
     * @return Status of the call.
     */
    Status CreateAllService();

    /**
     * @brief Init liveness check instance.
     * @return Status of the call.
     */
    Status InitLivenessCheck();

    /**
     * @brief Stop liveness check.
     */
    void StopLivenessCheck();

    /**
     * @brief Registers callback functions for collecting resource information, including queues, thread pools, shared
     * memory, and spills.
     */
    void RegisteringAllResourceCollectionCallbackFunc();

    /**
     * @brief Registers callback functions for worker metrics.
     */
    void RegisteringWorkerCallbackFunc();

    /**
     * @brief Registers callback functions for master metrics.
     */
    void RegisteringMasterCallbackFunc();

    /**
     * @brief Registers callback functions for third component metrics.
     */
    void RegisteringThirdComponentCallbackFunc();

    /**
     * @brief Wait for service ready.
     * @return Status of the call.
     */
    Status WaitForServiceReady();

    /**
     * @brief Notify shutdown message to etcd.
     */
    void NotifyShutdownToEtcd();

    /**
     * @brief Init replic manager instance.
     * @return Status of this call.
     */
    Status InitReplicaManager();

    /**
     * @brief Check sc_encrypt_secret_key.
     * @return Status of this call.
     */
    Status CheckScEncryptSecretKey();

    /**
     * @brief Update cluster info in rocksdb.
     * @param[in] event The event watched from ETCD.
     */
    void UpdateClusterInfoInRocksDb(const mvccpb::Event &event);

    /**
     * @brief Construct cluster store.
     * @return Status of this call.
     */
    Status ConstructClusterStore();

    /**
     * @brief Construct cluster info.
     * @param[out] clusterInfo The necessary cluster information at startup.
     * @return Status of this call.
     */
    Status ConstructClusterInfo(ClusterInfo &clusterInfo);

    /**
     * @brief Construct cluster info during ETCD crash.
     * @param[out] clusterInfo The necessary cluster information at startup.
     * @return Status of this call.
     */
    Status ConstructClusterInfoDuringEtcdCrash(ClusterInfo &clusterInfo);

    /**
     * @brief Start metastore service on master worker.
     * @return Status of this call.
     */
    Status StartMetaStoreService();

    /**
     * @brief Stop metastore service on master worker.
     * @return Status of this call.
     */
    Status StopMetaStoreService();

    /**
     * @brief Reconcile cluster information with other nodes in the cluster.
     * @param[in] localHashRingPb The information required for reconciliation.
     * @param[in] api2Tag The way to contact other nodes in the cluster.
     * @return Status of this call.
     */
    Status ReconcileClusterInfo(
        const HashRingPb &localHashRingPb,
        const std::unordered_map<std::shared_ptr<object_cache::WorkerRemoteWorkerOCApi>, int64_t> &api2Tag);

    /**
     * @brief Load hashring from rocksdb.
     * @param[out] clusterInfo The necessary cluster information at startup.
     * @param[out] localHashRingPb The information required for reconciliation.
     * @return Status of this call.
     */
    Status LoadHashRingFromRocksDb(ClusterInfo &clusterInfo, HashRingPb &localHashRingPb);

    /**
     * @brief Load workers from rocksdb.
     * @param[out] clusterInfo The necessary cluster information at startup.
     * @param[out] activeNodesInLocalCluster The active nodes in local cluster.
     * @return Status of this call.
     */
    Status LoadWorkersFromRocksDb(ClusterInfo &clusterInfo, std::vector<std::string> &activeNodesInLocalCluster);

    /**
     * @brief Check if need scale in.
     * @return True if needed.
     */
    bool IsScaleIn();

    /**
     * @brief Is clients on this node exist.
     * @return true If exist.
     */
    bool IsClientsExist();

    /**
     * @brief Wait all clients on this node exited.
     */
    void WaitClientsExit();

    /**
     * @brief Is async tasks running.
     * @return true If async tasks running.
     * @return false .
     */
    bool IsAsyncTasksRunning();

    std::shared_ptr<PersistenceApi> persistenceApi_{ nullptr };
    std::unique_ptr<EtcdStore> etcdStore_;
    std::shared_ptr<AkSkManager> akSkManager_{ nullptr };
    HostPort masterAddr_;
    std::unique_ptr<datasystem::ReplicaManager> replicaManager_{ nullptr };
    std::unique_ptr<datasystem::master::ResourceManager> resourceManager_{ nullptr };
    std::shared_ptr<master::RpcSessionManager> rpcSessionManager_{ nullptr };
    std::unique_ptr<datasystem::EtcdClusterManager> etcdCM_{ nullptr };
    std::unique_ptr<WorkerServiceImpl> workerSvc_{ nullptr };  // Worker common service.
    WaitPost waitCond_;
    // Object cache rpc service for client request.
    std::shared_ptr<datasystem::object_cache::WorkerOCServiceImpl> objCacheClientWorkerSvc_{ nullptr };
    std::future<Status> clientWorkerCommonSvcStatus_;
    // Object cache rpc service for worker request.
    std::shared_ptr<datasystem::object_cache::WorkerWorkerOCServiceImpl> objCacheWorkerWkSvc_{ nullptr };
    // Object cache rpc service for worker fast transport request.
    std::shared_ptr<datasystem::object_cache::WorkerWorkerTransportServiceImpl> objCacheWorkerTransSvc_{ nullptr };
    // Object cache rpc service for master request.
    std::shared_ptr<datasystem::object_cache::MasterWorkerOCServiceImpl> objCacheWorkerMsSvc_{ nullptr };
    // Stream cache rpc service for client request.
    std::shared_ptr<stream_cache::ClientWorkerSCServiceImpl> streamCacheClientWorkerSvc_{ nullptr };
    // Stream cache rpc service for master request.
    std::shared_ptr<stream_cache::MasterWorkerSCServiceImpl> streamCacheMasterWorkerSvc_{ nullptr };
    // Stream cache rpc service for worker request.
    std::shared_ptr<stream_cache::WorkerWorkerSCServiceImpl> streamCacheWorkerWorkerSvc_{ nullptr };

    // Master services exist in the worker for Object cache compile mode.
    std::shared_ptr<datasystem::master::MasterServiceImpl> commonSvc_{ nullptr };
    std::unique_ptr<datasystem::master::MasterOCServiceImpl> objCacheMasterSvc_{ nullptr };
    std::unique_ptr<datasystem::master::MasterSCServiceImpl> streamCacheMasterSvc_{ nullptr };
    std::unique_ptr<datasystem::ReplicationServiceImpl> replicaSvc_{ nullptr };
    std::future<Status> objCacheMasterSvcStatus_;
    std::future<Status> objCacheMasterAdSvcStatus_;
    std::future<Status> streamCacheMasterSvcStatus_;

    // Check whether all asynchronous tasks are completed before the worker ends.
    std::unique_ptr<Thread> checkAsyncTasksThread_{ nullptr };
    std::atomic<bool> checkThreadRunning_{ true };
    std::atomic<bool> checkAsyncTasksDone_{ false };
    std::unique_ptr<Thread> clientsExitChecker_{ nullptr };
    std::atomic<bool> allClientsExited_{ false };
    int64_t lastRequestArrivalTime_{ 0 };
    std::string checkFilePath_;
    std::condition_variable checkAsyncTasksDoneCv_;
    std::mutex checkAsyncTasksDoneMutex_;

    std::unique_ptr<WorkerLivenessCheck> livenessCheck_;
    std::shared_ptr<RocksStore> clusterStore_;
    std::unique_ptr<MetaStoreServer> metaStoreServer_;
#ifdef WITH_TESTS
    std::unique_ptr<st::StOCServiceImpl> utSvc_{ nullptr };
#endif
#ifdef ENABLE_PERF
    std::unique_ptr<PerfServiceImpl> perfService_{ nullptr };
#endif
};
}  // namespace worker
}  // namespace datasystem
#endif  // DATASYSTEM_WORKER_WORKER_OC_SERVER_H
