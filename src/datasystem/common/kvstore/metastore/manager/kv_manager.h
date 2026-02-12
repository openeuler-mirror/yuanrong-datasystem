/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
 * Description: KV manager for metastore service.
 */
#ifndef DATASYSTEM_COMMON_KVSTORE_METASTORE_KV_MANAGER_H
#define DATASYSTEM_COMMON_KVSTORE_METASTORE_KV_MANAGER_H

#include <array>
#include <atomic>
#include <functional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include <climits>

#include "etcd/api/mvccpb/kv.pb.h"
#include "etcd/api/etcdserverpb/rpc.pb.h"
#include "datasystem/utils/status.h"
#include "datasystem/common/kvstore/metastore/manager/lease_manager.h"

namespace datasystem {

constexpr int64_t MAX_HISTORY_ENTRIES = 5;

struct HistoricalEntry {
    int64_t revision;
    mvccpb::Event::EventType type;
    mvccpb::KeyValue kv;
};

class HistoryRingBuffer {
public:
    HistoryRingBuffer() = default;
    ~HistoryRingBuffer() = default;
    void Add(const HistoricalEntry &entry);
    std::vector<HistoricalEntry> GetRange(int64_t startRevision, int64_t endRevision = INT64_MAX) const;

private:
    std::array<HistoricalEntry, MAX_HISTORY_ENTRIES> entries_;
    int64_t head_ = 0;   // Index of most recent entry
    int64_t count_ = 0;  // Number of valid entries
};

class KVManager {
public:
    KVManager() = default;
    ~KVManager() = default;

    /**
     * @brief Put a key-value pair
     * @param key The key
     * @param value The value
     * @param lease The lease ID (0 for no lease)
     * @param prevKv Output for previous KV if prev_kv is true
     * @return Status of the operation
     */
    Status Put(const std::string &key, const std::string &value, int64_t lease, mvccpb::KeyValue *prevKv = nullptr);

    /**
     * @brief Get a key-value pair
     * @param key The key
     * @param kv Output KeyValue
     * @return Status of the operation
     */
    Status Get(const std::string &key, mvccpb::KeyValue *kv);

    /**
     * @brief Range query
     * @param start Start key
     * @param end End key (empty for single key, "\0" for all keys >= start)
     * @param kvs Output key-value pairs
     * @param limit Maximum number of results (0 for no limit)
     * @param countOnly Return only count
     * @param keysOnly Return only keys
     * @return Status of the operation
     */
    Status Range(const std::string &start, const std::string &end, std::vector<mvccpb::KeyValue> *kvs,
                 int64_t limit = 0, bool countOnly = false, bool keysOnly = false);

    /**
     * @brief Delete a key or range of keys
     * @param key Start key
     * @param rangeEnd End key (empty for single key)
     * @param prevKvs Output for previous KVs if prev_kv is true
     * @param skipLeaseCleanup If true, skip lease detach cleanup (for lease expiration)
     * @return Status of the operation
     */
    Status Delete(const std::string &key, const std::string &rangeEnd, std::vector<mvccpb::KeyValue> *prevKvs = nullptr,
                  bool skipLeaseCleanup = false);

    /**
     * @brief Get current revision
     */
    int64_t CurrentRevision() const
    {
        return revision_.load();
    }

    /**
     * @brief Execute a transaction
     * @param compares Compare operations
     * @param success Success operations
     * @param failure Failure operations
     * @param response Output response
     * @return Status of the operation
     */
    Status Txn(const std::vector<etcdserverpb::Compare> &compares, const std::vector<etcdserverpb::RequestOp> &success,
               const std::vector<etcdserverpb::RequestOp> &failure, etcdserverpb::TxnResponse *response);

    /**
     * @brief Set watch callbacks
     */
    void SetWatchCallback(
        std::function<void(const std::string &, const mvccpb::KeyValue &, const mvccpb::KeyValue *)> putCb,
        std::function<void(const std::string &, const mvccpb::KeyValue &)> delCb);

    /**
     * @brief Check if a key exists
     */
    bool KeyExists(const std::string &key) const;

    /**
     * @brief Set lease manager for lease-key binding
     */
    void SetLeaseManager(LeaseManager *leaseManager);

    /**
     * @brief Get historical events for a key within revision range
     * @param key The key
     * @param startRevision Start revision (inclusive)
     * @param endRevision End revision (exclusive, INT64_MAX for all)
     * @param events Output historical events
     * @return Status of operation
     */
    Status GetHistory(const std::string &key, int64_t startRevision, int64_t endRevision,
                      std::vector<HistoricalEntry> *events);

    /**
     * @brief Get historical events for keys in range within revision range
     * @param start Start key
     * @param end End key (empty for single key, "\0" for all keys >= start)
     * @param startRevision Start revision (inclusive)
     * @param events Output historical events (sorted by key then revision)
     * @return Status of operation
     */
    Status RangeHistory(const std::string &start, const std::string &end, int64_t startRevision,
                        std::vector<HistoricalEntry> *events);

private:
    struct KeyInfo {
        mvccpb::KeyValue kv;
        int64_t createRevision;
        int64_t modRevision;
        int64_t version;
        HistoryRingBuffer history;
    };

    /**
     * @brief Get target value for comparison
     * @param target The comparison target (VERSION, CREATE_REVISION, MOD_REVISION, LEASE)
     * @param info The key information
     * @return The target value
     */
    int64_t GetTargetValue(etcdserverpb::Compare_CompareTarget target, const KeyInfo &info) const;

    /**
     * @brief Compare string values
     * @param kvValue The key value to compare
     * @param cmpValue The comparison value
     * @param result The comparison result (EQUAL, NOT_EQUAL, LESS, GREATER)
     * @return true if comparison matches the expected result
     */
    bool CompareStringValue(const std::string &kvValue, const std::string &cmpValue,
                            etcdserverpb::Compare::CompareResult result) const;

    /**
     * @brief Compare int64 values
     * @param targetValue The target value
     * @param cmpValue The comparison value
     * @param result The comparison result (EQUAL, NOT_EQUAL, LESS, GREATER)
     * @return true if comparison matches the expected result
     */
    bool CompareIntValue(int64_t targetValue, int64_t cmpValue, etcdserverpb::Compare::CompareResult result) const;

    /**
     * @brief Inherit history for a key (from existing key or deleted history)
     * @param key The key
     * @param info KeyInfo to populate with inherited history
     * @note Does NOT acquire mutex, must be called while holding the mutex
     */
    void InheritKeyHistory(const std::string &key, KeyInfo *info);

    /**
     * @brief Record PUT event in history
     * @param info KeyInfo with kv populated
     * @param revision The revision number
     * @note Does NOT acquire mutex, must be called while holding the mutex
     */
    void RecordPutHistory(KeyInfo *info, int64_t revision);

    /**
     * @brief Record DELETE event in history and save to deletedHistory_
     * @param key The key being deleted
     * @param kv The KeyValue being deleted
     * @param revision The revision number
     * @note Does NOT acquire mutex, must be called while holding the mutex
     */
    void RecordDeleteHistory(const std::string &key, const mvccpb::KeyValue &kv, int64_t revision);

    /**
     * @brief Evaluate a compare operation
     * @param cmp The compare operation to evaluate
     * @param info The key information (null if key doesn't exist)
     * @return true if comparison evaluates to true
     */
    bool EvaluateCompare(const etcdserverpb::Compare &cmp, const KeyInfo *info) const;

    /**
     * @brief Check if a key is in range
     * @param key The key to check
     * @param start The start key of the range
     * @param end The end key of the range (empty for single key, "\0" for all keys >= start)
     * @return true if key is in the range
     */
    bool KeyInRange(const std::string &key, const std::string &start, const std::string &end) const;

    /**
     * @brief Watch event for transaction callback
     */
    struct WatchEvent {
        enum class Type { PUT, DELETE };
        Type type;
        std::string key;
        mvccpb::KeyValue newKv;   // For PUT: new value
        mvccpb::KeyValue prevKv;  // For PUT: previous value if existed
        bool hasPrevKv;           // For PUT: whether prevKv is valid
    };

    /**
     * @brief Execute put operation in transaction
     * @param req Put request
     * @param newRevision New revision number
     * @param respOp Response operation
     * @param oldLease Output old lease ID (0 if key didn't exist or had no lease)
     * @param event Output watch event (null if no event collection needed)
     */
    void ExecutePutOp(const etcdserverpb::PutRequest &req, int64_t newRevision, etcdserverpb::ResponseOp *respOp,
                      int64_t *oldLease, std::vector<WatchEvent> *events);

    /**
     * @brief Execute delete range operation in transaction
     * @param req Delete request
     * @param newRevision New revision number
     * @param respOp Response operation
     * @param deletedLeases Output map of deleted keys to (lease IDs
     * @param events Output watch events (null if no event collection needed)
     */
    void ExecuteDeleteRangeOp(const etcdserverpb::DeleteRangeRequest &req, int64_t newRevision,
                              etcdserverpb::ResponseOp *respOp,
                              std::unordered_map<std::string, int64_t> *deletedLeases, std::vector<WatchEvent> *events);

    /**
     * @brief Execute range operation in transaction
     * @param req The range request
     * @param respOp The response operation to fill
     */
    void ExecuteRangeOp(const etcdserverpb::RangeRequest &req, etcdserverpb::ResponseOp *respOp);

    /**
     * @brief Evaluate all compare operations in transaction
     * @param compares The compare operations to evaluate
     * @return true if all compares evaluate to true
     */
    bool EvaluateCompares(const std::vector<etcdserverpb::Compare> &compares) const;

    /**
     * @brief Process lease changes after transaction execution
     * @param putLeases Put operation lease changes (key, oldLease, newLease)
     * @param deletedLeases Delete operation lease changes (key -> leaseId)
     */
    void ProcessTxnLeaseChanges(const std::vector<std::tuple<std::string, int64_t, int64_t>> &putLeases,
                                const std::unordered_map<std::string, int64_t> &deletedLeases) const;

    /**
     * @brief Trigger watch events after transaction execution
     * @param events All watch events to trigger
     */
    void TriggerWatchEvents(const std::vector<WatchEvent> &events) const;

    /**
     * @brief Validate that all leases in operations exist
     * @param ops The operations to validate
     * @return Status OK if all leases exist, error otherwise
     */
    Status ValidateTxnLeases(const std::vector<etcdserverpb::RequestOp> &ops) const;

    /**
     * @brief Prepare KeyInfo for put operation
     * @param key The key
     * @param value The value
     * @param lease The lease ID
     * @return Prepared KeyInfo
     */
    KeyInfo PreparePutInfo(const std::string &key, const std::string &value, int64_t lease) const;

    /**
     * @brief Update data store with new key-value under lock
     * @param key The key
     * @param info The KeyInfo to store
     * @param prevKv Output for previous KV
     * @param oldLease Output for old lease ID
     * @param newRevision Output for the new revision number
     * @return true if key existed before
     */
    bool UpdatePutData(const std::string &key, KeyInfo *info, mvccpb::KeyValue *prevKv, int64_t *oldLease,
                       int64_t *newRevision);

    /**
     * @brief Attach new lease with rollback on failure
     * @param key The key
     * @param lease The lease ID to attach
     * @param oldLease The old lease ID (for rollback)
     * @param existed Whether key existed before
     * @param prevKvCopy Previous KV (for rollback)
     * @return Status of the operation
     */
    Status AttachNewLeaseWithRollback(const std::string &key, int64_t lease, int64_t oldLease, bool existed,
                                      const mvccpb::KeyValue &prevKvCopy);

    /**
     * @brief Trigger put watch event
     * @param key The key
     * @param newKv The new key-value
     * @param prevKv Previous key-value (null for new key)
     */
    std::unordered_map<std::string, KeyInfo> data_;
    std::unordered_map<std::string, HistoryRingBuffer> deletedHistory_;  // History of deleted keys
    std::atomic<int64_t> revision_{ 1 };
    mutable std::shared_mutex mutex_;

    std::function<void(const std::string &, const mvccpb::KeyValue &, const mvccpb::KeyValue *)> putCallback_;
    std::function<void(const std::string &, const mvccpb::KeyValue &)> deleteCallback_;
    LeaseManager *leaseManager_ = nullptr;
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_KVSTORE_METASTORE_KV_KV_MANAGER_H
