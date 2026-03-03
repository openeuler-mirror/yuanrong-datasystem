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
 * Description: Interface to etcd.
 */
#ifndef DATASYSTEM_COMMON_KVSTORE_ETCD_ETCD_STORE_H
#define DATASYSTEM_COMMON_KVSTORE_ETCD_ETCD_STORE_H

#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "datasystem/utils/sensitive_value.h"
#include "etcd/api/etcdserverpb/rpc.grpc.pb.h"
#include "datasystem/common/kvstore/etcd/etcd_keep_alive.h"
#include "datasystem/common/kvstore/etcd/etcd_watch.h"
#include "datasystem/common/kvstore/etcd/grpc_session.h"
#include "datasystem/common/kvstore/etcd/etcd_keep_alive.h"
#include "datasystem/common/kvstore/kv_store.h"
#include "datasystem/common/util/locks.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/status_helper.h"
#include "etcd/api/etcdserverpb/rpc.grpc.pb.h"

namespace etcdserverpb {
class KV;
};

namespace datasystem {
static constexpr char ETCD_ADDR_PATTREN[] = ",";
const int64_t MIN_RPC_TIMEOUT_MS = 5000;  // 5s

enum class PrefixType { OTHER, RING, CLUSTER };
struct WatchElement {
    std::string tableName;
    std::string key;
    bool ifWatchOtherAz;
    int64_t startRevision;
};

class EtcdStore : public KvStore {
public:
    struct BatchInfoPutToEtcd {
        std::string meta;
        std::uint32_t hashVal = 0;
        std::string tableName;
        std::string etcdKey;
    };

    /**
     * @brief construct EtcdStore.
     * @param[in] address Etcd IP address.
     */
    explicit EtcdStore(const std::string &address);

    /**
     * @brief construct EtcdStore. If the etcd ca, cert, and key paths are configured, the content authentication is
     *        directly transferred. Otherwise, the gflags parameter is used for authentication.
     * @param[in] address Etcd IP address.
     * @param[in] etcdCa Root etcd certificate, optional parameters.
     * @param[in] etcdCert Etcd certificate chain, optional parameters.
     * @param[in] etcdKey Etcd private key, optional parameters.
     * @param[in] targetNameOverride etcd DNS name, optional parameters.
     */
    EtcdStore(const std::string &address, const std::string &etcdCa, const SensitiveValue &etcdCert,
              const SensitiveValue &etcdKey, std::string targetNameOverride);

    ~EtcdStore() override;

    Status Init();

    /**
     * @brief Authenticate to etcd using username/password
     * @param[in] username etcd username
     * @param[in] password etcd password
     * @param[in] tokenRefreshInterval token refresh interval in seconds
     * @return Status of the call
     */
    Status Authenticate(std::string username, const SensitiveValue &password, uint32_t tokenRefreshInterval);

    /**
     * @brief Close the database.
     */
    void Close() override;

    /**
     * @brief Shuts down the etcd store. Essentially the same as Close, except this one can return a status to
     * give feedback on the progress of the close and is not defined in the parent interface.
     * @return Status of the call
     */
    Status Shutdown();

    /**
     * @brief Create a new table (aka column family in RocksDB's term).
     * @param[in] tableName The table name to create.
     * @param[in] tablePrefix The table prefix.
     * @return Status of the call.
     */
    Status CreateTable(const std::string &tableName, const std::string &tablePrefix);

    /**
     * @brief Drop a table (aka column family in RocksDB's term).
     * @param[in] tableName The table name to create.
     * @return Status of the call.
     */
    Status DropTable(const std::string &tableName) override;

    /**
     * @brief Creates a new Lease and returns it's ID.
     * @param[in] ttlInSec The time to live for this leaseID in seconds
     * @param[out] LeaseId created in etcd
     * @return Status of the call
     */
    Status GetLeaseID(const int64_t ttlInSec, std::atomic<int64_t> &leaseId);

    /**
     * @brief Notify local worker timeout when keep alive failed.
     * @return Status of the call
     */
    Status HandleKeepAliveFailed();

    /**
     * @brief Creates a new Lease and returns it's ID, reconnect if failed.
     * @param[in] ttlInSec The time to live for this leaseID in seconds
     * @param[out] LeaseId created in etcd
     * @return Status of the call
     */
    Status GetLeaseIDWithReconnectIfError(const int64_t ttlInSec, std::atomic<int64_t> &leaseId);

    /**
     * @brief Renew an existing lease repeatedly. This version of the init instructs the keep alive logic to provide
     * built-in threading and functionality to register the kv pair and associate it with a lease. Call this overload
     * if you want to use auto-generated timestamp with tag as the value. You cannot provide value.
     * @param[in] tableName table name for the <key,value> to insert.
     * @param[in] key key to add to the table. Value is auto-generated timestamp with tag "start"/"restart".
     * @param[in] isRestart whether this node restarted.
     * @return Status of the call.
     */
    Status InitKeepAlive(const std::string &tableName, const std::string &key, bool isRestart = false,
                         bool isEtcdAvailableWhenStart = true);

    /**
     * @brief Renew an existing lease repeatedly upto ttl seconds.
     * @param[in] tableName The table on which watch will be set.
     * @param[in] key The starting key of the key range on which watch will observe events.
     * @param[in] ifWatchOtherAz Whether to watch other AZs' event.
     * @param[in] startRevision startRevision is an optional revision to watch from (inclusive).
     * @return Status of the call.
     */
    Status WatchEvents(const std::string &tableName, const std::string &key, bool ifWatchOtherAz,
                       int64_t startRevision);

    /**
     * @brief Renew an existing lease repeatedly upto ttl seconds.
     * @param[in] watchKeys The keys to be watched.
     * @return Status of the call.
     */
    Status WatchEvents(const std::vector<WatchElement> &watchKeys);
    /**
     * @brief Put a new key-value into a table.
     * @param[in] tableName The table name for the <key,value> to insert.
     * @param[in] key The key to add to the table.
     * @param[in] value The value to add to the table.
     * @return Status of the call.
     */
    Status Put(const std::string &tableName, const std::string &key, const std::string &value) override;

    /**
     * @brief Put a batch of objects into a table.
     * @param[in] metaInfos The metaInfos of a batch of objects to insert.
     * @return Status of the call.
     */
    Status BatchPut(const std::unordered_map<std::string, BatchInfoPutToEtcd> &metaInfos);

    /**
     * @brief Put a new key-value into a table.
     * @param[in] tableName The table name for the <key,value> to insert.
     * @param[in] key The key to add to the table.
     * @param[in] value The value to add to the table.
     * @param[in] leaseId The leaseId of the lease grant request.
     * @return Status of the call.
     */
    Status PutWithLeaseId(const std::string &tableName, const std::string &key, const std::string &value,
                          const int64_t leaseId);

    /**
     * @brief Put a new key-value into a table.
     * @param[in] tableName The table name for the <key,value> to insert.
     * @param[in] key The key to add to the table.
     * @param[in] value The value to add to the table.
     * @param[out] version The output version of the key.
     * @param[in] asyncElapse The time this object being in the async queue.
     * @return Status of the call.
     */
    Status Put(const std::string &tableName, const std::string &key, const std::string &value, int32_t timeoutMs,
               uint64_t asyncElapse = 0);

    /**
     * @brief Put a new key-value into a table.
     * @param[in] tableName The table name for the <key,value> to insert.
     * @param[in] key The key to add to the table.
     * @param[in] value The value to add to the table.
     * @param[out] version The output version of the key.
     * @param[in] asyncElapse The time this object being in the async queue.
     * @return Status of the call.
     */
    Status Put(const std::string &tableName, const std::string &key, const std::string &value, int64_t *version,
               int32_t timeoutMs = SEND_RPC_TIMEOUT_MS_DEFAULT, uint64_t asyncElapse = 0);

    /**
     * @brief List all the tables.
     * @param[out] tables A vector of all the tables in this database.
     * @return Status of the call.
     */
    Status ListTables(std::vector<std::string> &tables) override;

    /**
     * @brief Get the value of the given key.
     * @param[in] tableName The table to search for the key.
     * @param[in] key The key to search.
     * @param[out] value The value of the given key, if not found, the content is unchanged.
     * @return Status of the call, Status::KVStoreError() if the given key does not exist.
     */
    Status Get(const std::string &tableName, const std::string &key, std::string &value) override;

    /**
     * @brief Get the value of the given key.
     * @param[in] tableName The table to search for the key.
     * @param[in] key The key to search.
     * @param[out] value The value of the given key, if not found, the content is unchanged.
     * @param[out] version The version of the key.
     * @param[in] timeoutMs timeoutMs.
     * @return Status of the call, Status::KVStoreError() if the given key does not exist.
     */
    Status Get(const std::string &tableName, const std::string &key, RangeSearchResult &res,
               int32_t timeoutMs = SEND_RPC_TIMEOUT_MS_DEFAULT);

    /**
     * @brief Get the value of the given key.
     * @param[in] tableName The table to search for the key.
     * @param[in] key The real key stored in etcd to search.
     * @param[out] value The value of the given key, if not found, the content is unchanged.
     * @param[out] rspVersion The version of the key.
     * @param[in] reqRevision Get the key of the specified version. If revision is less or equal to zero, the range is
     * over the newest key-value store.
     * @param[in] timeoutMs timeoutMs.
     * @return Status of the call, Status::KVStoreError() if the given key does not exist.
     */
    Status RawGet(const std::string &etcdKey, RangeSearchResult &res, int64_t reqRevision = 0,
                  int32_t timeoutMs = SEND_RPC_TIMEOUT_MS_DEFAULT);

    /**
     * @brief Get all key-values from specific table.
     * @param[in] tableName The table to get key-values.
     * @param[out] outKeyValues The output key-values in table.
     * @return Status of the call.
     */
    Status GetAll(const std::string &tableName,
                  std::vector<std::pair<std::string, std::string>> &outKeyValues) override;

    /**
     * @brief Get all key-values from specific table.
     * @param[in] tableName The table to get key-values.
     * @param[in] reqRevision Get the key of the specified version. If revision is less or equal to zero, the range is
     * over the newest key-value store.
     * @param[out] outKeyValues The output key-values in table.
     * @return Status of the call.
     */
    Status GetAll(const std::string &tableName, int64_t reqRevision,
                  std::vector<std::pair<std::string, std::string>> &outKeyValues);

    /**
     * @brief Get all key-values from specific table.
     * @param[in] tableName The table to get key-values.
     * @param[out] outKeyValues The output key-values in table.
     * @param[out] rspRevision rspRevision is the key-value store revision when the request was applied.
     * @return Status of the call.
     */
    Status GetAll(const std::string &tableName, std::vector<std::pair<std::string, std::string>> &outKeyValues,
                  int64_t &rspRevision);

    /**
     * @brief Get all key-values from specific table.
     * @param[in] tableName The table to get key-values.
     * @param[in] reqRevision Get the key of the specified version. If revision is less or equal to zero, the range is
     * over the newest key-value store.
     * @param[out] outKeyValues The output key-values in table.
     * @param[out] rspRevision rspRevision is the key-value store revision when the request was applied.
     * @return Status of the call.
     */
    Status GetAll(const std::string &tableName, int64_t reqRevision,
                  std::vector<std::pair<std::string, std::string>> &outKeyValues, int64_t &rspRevision);

    /**
     * @brief Get all other AZs' key-values from specific table.
     * @param[in] tableName The table to get key-values.
     * @param[in] revision Get the key of the specified version. If revision is less or equal to zero, the range is
     * over the newest key-value store.
     * @param[out] outKeyValues The output key-values in table.
     * @return Status of the call.
     */
    Status GetOtherAzAllValue(const std::string &tableName, int64_t revision,
                              std::vector<std::pair<std::string, std::string>> &outKeyValues);

    /**
     * @brief Prefix search in specific table.
     * @param[in] tableName The table where the key to be search.
     * @param[in] prefixKey The prefix key to search.
     * @param[out] outKeyValues Array of the output key-value pairs.
     * @return Status of the call, Status::KVStoreError() if the given prefixKey does not exist.
     */
    Status PrefixSearch(const std::string &tableName, const std::string &prefixKey,
                        std::vector<std::pair<std::string, std::string>> &outKeyValues) override;

    /**
     * @brief Range search in specific table.
     * @param[in] tableName The table where the key to be search.
     * @param[in] begin Range begin.
     * @param[in] end Range end.
     * @param[out] outKeyValues Array of the output key-value pairs.
     */
    Status RangeSearch(const std::string &tableName, const std::string &begin, const std::string &end,
                       std::vector<std::pair<std::string, std::string>> &outKeyValues);

    /**
     * @brief Delete the given key (inline, high performance version).
     * @param[in] tableName The table where the key to be deleted.
     * @param[in] key The key to delete.
     * @return Status of the call, Status::KVStoreError() if the operation fails.
     */
    Status Delete(const std::string &tableName, const std::string &key) override;

    /**
     * @brief Delete the given key (inline, high performance version).
     * @param[in] tableName The table where the key to be deleted.
     * @param[in] key The key to delete.
     * @param[in] asyncElapse The time this object being in the async queue.
     * @return Status of the call, Status::KVStoreError() if the operation fails.
     */
    Status Delete(const std::string &tableName, const std::string &key, uint64_t asyncElapse,
                  int timeoutMs = SEND_RPC_TIMEOUT_MS_DEFAULT);

    using EtcdProcessFunction = std::function<Status(const std::string &, std::unique_ptr<std::string> &, bool &)>;

    /**
     * @brief Compare-And-Swap for the specific key. Before the processFunc function is executed, the CAS function
     * get the current value of the key from ETCD, then call 'processFunc' function do some process and 'processFunc'
     * will returns a new value. Finally, the CAS function will executes the transaction and retry when the key-value
     * was changed, it will ensure that the new value is successfully written to etcd.
     * @param[in] tableName The table where the key to be CAS.
     * @param[in] key The etcd key that will exec Compare And Swap.
     * @param[in] processFunc The function that used to generate a new key-value to be written to etcd, the meanings of
     * the parameters are as follows:
     * the first param is the input old value get from etcd,
     * the second param is the new output value that should be write to etcd,
     * the third param is the flags that used to indicate if or not to retry,
     * @param[out] version The key version after cas modification.
     * the return Status indicate the exec result.
     * @return Status of the call.
     */
    Status CAS(const std::string &tableName, const std::string &key, const EtcdProcessFunction &processFunc,
               RangeSearchResult &res);

    Status CAS(const std::string &tableName, const std::string &key, const EtcdProcessFunction &processFunc);

    /**
     * @brief Compare-And-Swap for the specific key. Before the processFunc function is executed, the CAS function
     * get the current value of the key from ETCD, then call 'processFunc' function do some process and 'processFunc'
     * will returns a new value. Finally, the CAS function will executes the transaction and retry when the key-value
     * was changed, it will ensure that the new value is successfully written to etcd.
     * @param[in] tableName The table where the key to be CAS.
     * @param[in] key The etcd key that will exec Compare And Swap.
     * @param[in] oldValue The old value of the key.
     * @param[in] newValue The new value of the key.
     * @return Status of the call.
     */
    Status CAS(const std::string &tableName, const std::string &key, const std::string &oldValue,
               const std::string &newValue);

    /**
     * @brief Update node state in etcd.
     * @param[in] state worker state.
     * @return status of the call.
     */
    Status UpdateNodeState(const std::string &state);

    /**
     * @brief Get real etcd key prefix by tableName.
     * @param[in] tableName The etcd table name
     * @param[out] prefix The real etcd key prefix stored in etcd.
     * @return Status of the call.
     */
    Status GetEtcdPrefix(const std::string &tableName, std::string &prefix);

    /**
     * @brief When all reconciliations are done, replace "restart" with "start" in ETCD.
     * @param[in] workerAddr The hostport of the local node whose reconciliations are done.
     * @return Return status.
     */
    Status InformEtcdReconciliationDone(const HostPort &workerAddr);

    /**
     * @brief Return the lease ID used for keep alive.
     * @return lease ID.
     */
    int64_t CheckLeaseId()
    {
        return leaseId_;
    }

    /**
     * @brief Check if etcd is writable.
     * @return Status of the call.
     */
    Status Writable()
    {
        static const std::string healthCheckVal = "RW";

        // The timeout interval is the same as that of the GetLeaseID() interface.
        // In scenarios with high network latency, the lease fails to be obtained but data can be written.
        // As a result, the heartbeat between the current node and etcd fails,
        // but the current node mistakenly considers that other etcd nodes are normal.
        return Put(ETCD_HEALTH_CHECK_TABLE, "", healthCheckVal, MIN_RPC_TIMEOUT_MS, MIN_RPC_TIMEOUT_MS);
    }

    /**
     * @brief Check if keepalive timeout.
     * @return keepAliveTimeout_.
     */
    bool IsKeepAliveTimeout()
    {
        return keepAliveTimeout_;
    }

    bool IsCreateFirstLease()
    {
        return keepAliveValue_.find("recover") != std::string::npos;
    }

    /**
     * @brief Obtains the success rate of all etcd requests.
     * @return The success rate string.
     */
    std::string GetEtcdRequestSuccessRate()
    {
        if (rpcSession_ == nullptr) {
            return "";
        }
        return rpcSession_->BlockingGetSuccessRateAndClean();
    }

    void SetCheckEtcdStateWhenNetworkFailedHandler(std::function<bool()> CheckEtcdStateWhenNetworkFailedHandler)
    {
        checkEtcdStateWhenNetworkFailedHandler_ = std::move(CheckEtcdStateWhenNetworkFailedHandler);
    }

    /**
     * @brief Set event handler.
     * @param[in] eventHandler Functions that handle events that are watched or obtained through other means.
     */
    void SetEventHandler(std::function<void(mvccpb::Event &&event)> &&eventHandler)
    {
        eventHandler_ = std::move(eventHandler);
    }

    /**
     * @brief Set update cluster info in rocksdb handler.
     * @param[in] updateClusterInfoInRocksDbHandler Functions that handle events that are watched or obtained through
     * other means.
     */
    void SetUpdateClusterInfoInRocksDbHandler(
        std::function<void(const mvccpb::Event &event)> &&updateClusterInfoInRocksDbHandler)
    {
        updateClusterInfoInRocksDbHandler_ = std::move(updateClusterInfoInRocksDbHandler);
    }

    // No copy allowed.
    EtcdStore(const EtcdStore &) = delete;
    EtcdStore &operator=(const EtcdStore &) = delete;
    // No move allowed.
    EtcdStore(const EtcdStore &&) = delete;
    EtcdStore &operator=(const EtcdStore &&) = delete;

    /**
     * @brief Get other az all hashring.
     * @param[in] revision Get the key of the specified version. If revision is less or equal to zero, the range is
     * over the newest key-value store.
     * @param[out] outKeyValues The output key-values in table.
     */
    Status GetOtherAzAllHashRing(int64_t revision, std::vector<std::pair<std::string, std::string>> &outKeyValues);

private:
    using TableMap = std::unordered_map<std::string, std::string>;
    using TableStrToVecMap = std::unordered_map<std::string, std::vector<std::string>>;
    const uint32_t CAS_MAX_SLEEP_TIME_US = 200000;
    const uint32_t CAS_ERROR_MAX_RETRY_NUM = 10;
    // Do not set it too long, otherwise in the scenario of etcd failure, the worker may have to wait for a long time
    // when exiting.
    const uint32_t CAS_GET_TIMEOUT_MS = 5000;
    const uint32_t NUM_KEEPALIVE_THREADS = 2;
    const uint32_t NUM_WATCH_THREADS = 2;
    const uint32_t NUM_HEALTH_CHECK_THREADS = 1;

    /**
     * @brief Launches the internal thread that loops the actual keep-alive logic
     * timestamp and tag, and each time reconnecting the etcd use the same value.
     * @return Status of the call.
     */
    Status RunKeepAliveTask(Timer &keepAliveTimeoutTimer, Timer &deathTimer);

    /**
     * @brief Helper function to kick off the threading and loops for the keep alive
     * @return Status of the call
     */
    Status LaunchKeepAliveThreads();

    /**
     * @brief Create new keep-alive kv pair.
     * @return Status of the call
     */
    Status AutoCreate();

    /**
     * @brief Creates and initializes the watch
     * @param[in] prefixMap Prefix to watch for
     * @param[in] writable Check if etcd is writable
     * @return Status of the call
     */
    Status InitWatch(std::unique_ptr<std::unordered_map<std::string, int64_t>> &&prefixMap,
                     const std::function<Status()> &writable);

    /**
     * @brief Runs the background threads for watch
     * @return Status of the call
     */
    Status WatchRun();

    /**
     * @brief Ends the watch and all threads
     * @return Status of the call
     */
    Status WatchShutdown();

    /**
     * @brief Get the key in etcd
     * @param[in] tableName The table name of etcd on worker
     * @param[in] key The key under table
     * @param[out] reakKey The key in etcd
     * @return Status of the call
     */
    Status GetRealKey(const std::string &tableName, const std::string &key, std::string &realKey);

    /**
     * @brief When watch fails, reinitialize it.
     * @return Status of the call
     */
    Status ReInitWatch();

    /**
     * @brief Prefix search.
     * @param[in] prefixKey The prefix key to search.
     * @param[out] outKeyValues Array of the output key-value pairs.
     * @param[out] revision The revision for this RangeGet.
     * @return Status of the call.
     */
    Status PrefixSearch(const std::string &prefixKey, EtcdRangeGetVector &outKeyValues, int64_t &revision);
    // Retrieves the token in a thread-safe manner
    std::string GetAuthToken();
    // Executes the actual RPC call to fetch a new token
    Status PerformAuthRequest();
    // Background loop that refreshes the token
    void TokenRefreshLoop();
    std::string address_;             // etcd address
    std::string keepAliveTableName_;  // The table on etcd for the leased kv's
    std::string keepAliveKey_;        // The key that is associated with the lease
    std::string keepAliveValue_;      // The value that is associated with the lease
    std::atomic<int64_t> leaseId_;    // The lease id for the keep alive.
    std::unique_ptr<GrpcSession<etcdserverpb::KV>> rpcSession_;
    mutable std::shared_timed_mutex mutex_;
    TableMap tableMap_;
    mutable std::shared_timed_mutex otherAzTblMutex_;
    TableStrToVecMap otherAzTableMap_;
    std::unique_ptr<ThreadPool> keepAlivePool_{ nullptr };
    std::unique_ptr<ThreadPool> watchRunPool_{ nullptr };
    std::unique_ptr<GrpcSession<etcdserverpb::Lease>> leaseSession_;
    std::unique_ptr<EtcdKeepAlive> leaseKeepAlive_;
    std::shared_ptr<EtcdWatch> watchEvents_;
    std::atomic<bool> keepAliveExit_{ false };
    std::atomic<bool> watchExit_{ false };
    std::unique_ptr<GrpcSession<etcdserverpb::Auth>> authSession_;
    std::string authToken_;
    RandomData randomData_{ RandomData::GetRandomSeed() };
    WriterPrefRWLock keepAliveLock_;  // protects the leaseKeepAlive_ ptr
    WriterPrefRWLock watchLock_;      // protects the watchEvents_ ptr

    std::string username_;
    SensitiveValue password_;  // Ensure it supports safe copy or assignment for background use

    // Thread safety and control mechanisms
    std::shared_mutex tokenMutex_;                 // Protects concurrent read/write to authToken_
    std::thread tokenRefreshThread_;               // Background thread for token refresh
    std::atomic<bool> stopTokenRefresh_{ false };  // Flag to terminate the refresh thread
    uint32_t tokenRefreshInterval_{ 30 };          // default to refresh token every 30 seconds

    // Router client connect paras
    RouterClientCurveKit clientCurveKit_;
    std::atomic<bool> isRouterClientCurveConnect_{ false };
    Timer keepAliveTimeoutTimer_;

    std::function<bool()> checkEtcdStateWhenNetworkFailedHandler_;

    std::atomic<bool> keepAliveTimeout_{ true };

    std::function<void(mvccpb::Event &&event)> eventHandler_;
    std::function<void(const mvccpb::Event &event)> updateClusterInfoInRocksDbHandler_ =
        [](const mvccpb::Event &event) { (void)event; };
};

class Transaction {
public:
    Transaction(std::string authToken = "");

    ~Transaction();

    /**
     * @brief Start ETCD transaction.
     */
    void StartTransaction();

    /**
     * @brief Commit ETCD transaction ops to ETCD cluster.
     * @return Status of the call.
     */
    Status Commit();

    /**
     * @brief Clean the transaction op.
     */
    void Clean();

    /**
     * @brief Check if transaction is ready.
     * @return True if txn is ready.
     */
    bool Ready()
    {
        return txnReq_ != nullptr;
    }

    /**
     * @brief Used to specific the old value of the key, it can be used to implement CAS semantics.
     * @param[in] key The key to be compare.
     * @param[in] oldValue The old value of the key.
     * @return Status of the call.
     */
    Status CompareKeyValue(const std::string &key, const std::string &oldValue);

    /**
     * @brief Used to specific the version of the key, it can be used to implement CAS semantics.
     * @param[in] key The key to be compare.
     * @param[in] version The version when modify the key.
     * @return Status of the call.
     */
    Status CompareKeyVersion(const std::string &key, int64_t version);

    /**
     * @brief Txn put, compare to EtcdStore::Put, it just add a record to txn list rather than send rpc request.
     * @param[in] key The key to add to the table.
     * @param[in] value The value to add to the table.
     * @return Status of the call.
     */
    Status Put(const std::string &key, const std::string &value);

    /**
     * @brief Txn delete, compare to EtcdStore::Delete, it just add a record to txn list rather than send rpc request.
     * @param[in] key The key to delete.
     * @return  Status of the call.
     */
    Status Delete(const std::string &key);

private:
    class AutoDereference {
    public:
        explicit AutoDereference(Transaction *txn) : txn_(txn)
        {
        }

        ~AutoDereference()
        {
            if (txn_ != nullptr) {
                txn_->txnReq_.reset();
            }
        }

    private:
        Transaction *txn_;
    };

    friend class EtcdStore;

    // Txn request to record the ops and need to send to ETCD cluster.
    std::unique_ptr<etcdserverpb::TxnRequest> txnReq_;

    // The auth token
    std::string authToken_;

    // Initialize flag.
    static std::once_flag flag_;

    // RPC session connect to ETCD cluster, all txn share on rpc session.
    static std::unique_ptr<GrpcSession<etcdserverpb::KV>> rpcSession_;

    // Record how many txn construct.
    static std::atomic<int> num_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_KVSTORE_ETCD_ETCD_STORE_H
