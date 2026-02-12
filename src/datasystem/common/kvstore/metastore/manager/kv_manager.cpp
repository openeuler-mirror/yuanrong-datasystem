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
 * Description: KV manager implementation
 */
#include "datasystem/common/kvstore/metastore/manager/kv_manager.h"

#include <algorithm>
#include "datasystem/common/log/log.h"

namespace datasystem {

void HistoryRingBuffer::Add(const HistoricalEntry &entry)
{
    entries_[head_] = entry;
    head_ = (head_ + 1) % MAX_HISTORY_ENTRIES;
    if (count_ < MAX_HISTORY_ENTRIES) {
        count_++;
    }
}

std::vector<HistoricalEntry> HistoryRingBuffer::GetRange(int64_t startRevision, int64_t endRevision) const
{
    std::vector<HistoricalEntry> result;
    if (count_ == 0) {
        return result;
    }

    // Iterate from oldest to newest
    int64_t tail = (head_ - count_ + MAX_HISTORY_ENTRIES) % MAX_HISTORY_ENTRIES;

    for (int64_t i = 0; i < count_; ++i) {
        int64_t index = (tail + i) % MAX_HISTORY_ENTRIES;
        const HistoricalEntry &entry = entries_[index];

        if (entry.revision >= startRevision && entry.revision < endRevision) {
            result.push_back(entry);
        }
    }

    return result;
}

KVManager::KeyInfo KVManager::PreparePutInfo(const std::string &key, const std::string &value, int64_t lease) const
{
    KeyInfo info;
    info.kv.set_key(key);
    info.kv.set_value(value);
    info.kv.set_lease(lease);
    return info;
}

bool KVManager::UpdatePutData(const std::string &key, KeyInfo *info, mvccpb::KeyValue *prevKv, int64_t *oldLease,
                              int64_t *newRevision)
{
    bool existed = false;
    std::unique_lock<std::shared_mutex> lock(mutex_);

    // Generate revision inside lock for consistency with Delete
    *newRevision = ++revision_;

    existed = data_.find(key) != data_.end();
    if (existed) {
        if (prevKv) {
            *prevKv = data_[key].kv;
        }
        *oldLease = data_[key].kv.lease();
        info->kv.set_create_revision(data_[key].createRevision);
        info->kv.set_version(data_[key].version + 1);
    } else {
        info->kv.set_create_revision(*newRevision);
        info->kv.set_version(1);
    }

    // Inherit history and record PUT event
    InheritKeyHistory(key, info);
    RecordPutHistory(info, *newRevision);

    data_[key] = *info;
    return existed;
}

Status KVManager::AttachNewLeaseWithRollback(const std::string &key, int64_t lease, int64_t oldLease, bool existed,
                                             const mvccpb::KeyValue &prevKvCopy)
{
    Status attachStatus = leaseManager_->AttachKey(lease, key);
    if (attachStatus.IsError()) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        if (existed) {
            KeyInfo restoreInfo;
            restoreInfo.kv = prevKvCopy;
            restoreInfo.createRevision = prevKvCopy.create_revision();
            restoreInfo.modRevision = prevKvCopy.mod_revision();
            restoreInfo.version = prevKvCopy.version();
            data_[key] = restoreInfo;
        } else {
            data_.erase(key);
        }
        // Rollback: restore old lease association
        // Note: This is needed because Put already called DetachKey before calling this function
        // Only restore if oldLease still exists (it may have been revoked concurrently)
        if (oldLease != 0 && leaseManager_ && leaseManager_->LeaseExists(oldLease)) {
            leaseManager_->AttachKey(oldLease, key);
        }
    }
    return attachStatus;
}

Status KVManager::Put(const std::string &key, const std::string &value, int64_t lease, mvccpb::KeyValue *prevKv)
{
    if (lease != 0 && leaseManager_ && !leaseManager_->LeaseExists(lease)) {
        LOG(ERROR) << "Lease not found: " << lease;
        return Status(StatusCode::K_NOT_FOUND, "Lease not found: " + std::to_string(lease));
    }

    mvccpb::KeyValue prevKvCopy;
    int64_t oldLease = 0;
    int64_t newRevision = 0;

    KeyInfo info = PreparePutInfo(key, value, lease);
    // Generate revision inside UpdatePutData's lock for consistency with Delete
    bool existed = UpdatePutData(key, &info, &prevKvCopy, &oldLease, &newRevision);

    info.kv.set_mod_revision(newRevision);
    info.createRevision = info.kv.create_revision();
    info.modRevision = info.kv.mod_revision();
    info.version = info.kv.version();

    if (prevKv && existed) {
        *prevKv = prevKvCopy;
    }

    mvccpb::KeyValue newKv = info.kv;

    // Detach from old lease (if it still exists)
    if (oldLease != 0 && leaseManager_) {
        leaseManager_->DetachKey(oldLease, key);
    }

    if (lease != 0 && leaseManager_) {
        Status attachStatus = AttachNewLeaseWithRollback(key, lease, oldLease, existed, prevKvCopy);
        if (attachStatus.IsError()) {
            return attachStatus;
        }
    }

    // Trigger watch callback for all successful put operations
    if (putCallback_) {
        putCallback_(key, newKv, existed ? &prevKvCopy : nullptr);
    }
    return Status::OK();
}

Status KVManager::Get(const std::string &key, mvccpb::KeyValue *kv)
{
    LOG(INFO) << "Get key=" << key;
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = data_.find(key);
    if (it == data_.end()) {
        LOG(WARNING) << "Key not found: " << key;
        return Status(StatusCode::K_NOT_FOUND, "Key not found: " + key);
    }
    *kv = it->second.kv;
    return Status::OK();
}

bool KVManager::KeyInRange(const std::string &key, const std::string &start, const std::string &end) const
{
    // Lower bound check: all Range requests require key >= start
    if (key < start) {
        return false;
    }

    // Exact match when end is empty
    if (end.empty()) {
        return key == start;
    }

    // No upper bound: end is "\0" means match all keys >= start
    if (end.size() == 1 && end[0] == '\0') {
        return true;
    }

    // Standard range/prefix match: [start, end)
    return key < end;
}

void KVManager::InheritKeyHistory(const std::string &key, KeyInfo *info)
{
    // Check if key exists in data_ (already exists)
    auto it = data_.find(key);
    if (it != data_.end()) {
        // Copy existing history from current key
        info->history = it->second.history;
    } else {
        // Check if key exists in deletedHistory_ (key was deleted before)
        // Inherit the complete history from when the key existed
        auto delIt = deletedHistory_.find(key);
        if (delIt != deletedHistory_.end()) {
            info->history = delIt->second;
            deletedHistory_.erase(delIt);
        }
    }
}

void KVManager::RecordPutHistory(KeyInfo *info, int64_t revision)
{
    HistoricalEntry newEntry;
    newEntry.revision = revision;
    newEntry.type = mvccpb::Event::PUT;
    newEntry.kv = info->kv;
    info->history.Add(newEntry);
}

void KVManager::RecordDeleteHistory(const std::string &key, const mvccpb::KeyValue &kv, int64_t revision)
{
    // Copy existing history before deletion
    auto it = data_.find(key);
    if (it == data_.end()) {
        return;
    }

    HistoryRingBuffer history = it->second.history;

    // Add DELETE event to history
    HistoricalEntry delEntry;
    delEntry.revision = revision;
    delEntry.type = mvccpb::Event::DELETE;
    delEntry.kv = kv;
    history.Add(delEntry);

    // Save to deletedHistory_
    deletedHistory_[key] = std::move(history);
}

Status KVManager::Range(const std::string &start, const std::string &end, std::vector<mvccpb::KeyValue> *kvs,
                        int64_t limit, bool countOnly, bool keysOnly)
{
    std::shared_lock<std::shared_mutex> lock(mutex_);

    // Collect all matching keys and values
    std::vector<mvccpb::KeyValue> results;
    for (const auto &[key, info] : data_) {
        if (!KeyInRange(key, start, end)) {
            continue;
        }
        if (keysOnly) {
            mvccpb::KeyValue kv;
            kv.set_key(key);
            results.push_back(kv);
        } else {
            results.push_back(info.kv);
        }
    }

    // Sort by key in UTF-8 byte order
    std::sort(results.begin(), results.end(),
              [](const mvccpb::KeyValue &a, const mvccpb::KeyValue &b) { return a.key() < b.key(); });

    // Apply limit after sorting
    int64_t count = results.size();
    if (limit > 0 && count > limit) {
        results.resize(limit);
        count = limit;
    }

    // Fill response
    if (!countOnly) {
        *kvs = std::move(results);
    }

    return Status::OK();
}

Status KVManager::Delete(const std::string &key, const std::string &rangeEnd, std::vector<mvccpb::KeyValue> *prevKvs,
                         bool skipLeaseCleanup)
{
    std::vector<mvccpb::KeyValue> deletedKvs;

    {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        ++revision_;

        for (auto it = data_.begin(); it != data_.end();) {
            if (KeyInRange(it->first, key, rangeEnd)) {
                mvccpb::KeyValue kv = it->second.kv;
                // Update mod_revision to the delete operation's revision
                kv.set_mod_revision(revision_.load());

                // Record delete history
                RecordDeleteHistory(it->first, kv, revision_.load());

                // Collect deleted KV for callback
                deletedKvs.push_back(kv);

                // Erase from data_
                it = data_.erase(it);
            } else {
                ++it;
            }
        }
    }  // Lock released here

    // Return previous KVs
    if (prevKvs) {
        *prevKvs = deletedKvs;
    }

    // Detach keys from leases after releasing lock
    // Skip lease cleanup when lease is expiring (lease already removed from lease manager)
    // Or if lease no longer exists (already revoked)
    if (!skipLeaseCleanup && leaseManager_) {
        for (const auto &kv : deletedKvs) {
            if (kv.lease() != 0 && leaseManager_->LeaseExists(kv.lease())) {
                leaseManager_->DetachKey(kv.lease(), kv.key());
            }
        }
    }

    // Trigger watch events after releasing lock to avoid deadlock/blocking
    if (deleteCallback_) {
        for (const auto &kv : deletedKvs) {
            deleteCallback_(kv.key(), kv);
        }
    }

    return Status::OK();
}

bool KVManager::KeyExists(const std::string &key) const
{
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return data_.find(key) != data_.end();
}

int64_t KVManager::GetTargetValue(etcdserverpb::Compare_CompareTarget target, const KeyInfo &info) const
{
    switch (target) {
        case etcdserverpb::Compare::VERSION:
            return info.version;
        case etcdserverpb::Compare::CREATE:
            return info.createRevision;
        case etcdserverpb::Compare::MOD:
            return info.modRevision;
        case etcdserverpb::Compare::LEASE:
            return info.kv.lease();
        default:
            return 0;
    }
}

bool KVManager::CompareStringValue(const std::string &kvValue, const std::string &cmpValue,
                                   etcdserverpb::Compare::CompareResult result) const
{
    switch (result) {
        case etcdserverpb::Compare::EQUAL:
            return kvValue == cmpValue;
        case etcdserverpb::Compare::NOT_EQUAL:
            return kvValue != cmpValue;
        case etcdserverpb::Compare::GREATER:
            return kvValue > cmpValue;
        case etcdserverpb::Compare::LESS:
            return kvValue < cmpValue;
        default:
            return false;
    }
}

bool KVManager::CompareIntValue(int64_t targetValue, int64_t cmpValue,
                                etcdserverpb::Compare::CompareResult result) const
{
    switch (result) {
        case etcdserverpb::Compare::EQUAL:
            return targetValue == cmpValue;
        case etcdserverpb::Compare::NOT_EQUAL:
            return targetValue != cmpValue;
        case etcdserverpb::Compare::GREATER:
            return targetValue > cmpValue;
        case etcdserverpb::Compare::LESS:
            return targetValue < cmpValue;
        default:
            return false;
    }
}

bool KVManager::EvaluateCompare(const etcdserverpb::Compare &cmp, const KeyInfo *info) const
{
    // When key doesn't exist (info is null), treat it as:
    // - VERSION/CREATE/MOD/LEASE: 0
    // - VALUE: empty string
    if (!info) {
        if (cmp.target() == etcdserverpb::Compare::VALUE) {
            return CompareStringValue("", cmp.value(), cmp.result());
        }
        if (cmp.target() == etcdserverpb::Compare::VERSION || cmp.target() == etcdserverpb::Compare::CREATE
            || cmp.target() == etcdserverpb::Compare::MOD || cmp.target() == etcdserverpb::Compare::LEASE) {
            return CompareIntValue(0, cmp.version(), cmp.result());
        }
        return false;
    }

    if (cmp.target() == etcdserverpb::Compare::VALUE) {
        return CompareStringValue(info->kv.value(), cmp.value(), cmp.result());
    }

    if (cmp.target() == etcdserverpb::Compare::VERSION || cmp.target() == etcdserverpb::Compare::CREATE
        || cmp.target() == etcdserverpb::Compare::MOD || cmp.target() == etcdserverpb::Compare::LEASE) {
        int64_t targetValue = GetTargetValue(cmp.target(), *info);
        return CompareIntValue(targetValue, cmp.version(), cmp.result());
    }

    return false;
}

void KVManager::ExecutePutOp(const etcdserverpb::PutRequest &req, int64_t newRevision, etcdserverpb::ResponseOp *respOp,
                             int64_t *oldLease, std::vector<WatchEvent> *events)
{
    mvccpb::KeyValue prevKv;
    bool existed = data_.find(req.key()) != data_.end();
    if (existed) {
        prevKv = data_[req.key()].kv;
    }
    *oldLease = existed ? prevKv.lease() : 0;

    KeyInfo info;
    info.kv.set_key(req.key());
    info.kv.set_value(req.value());
    info.kv.set_lease(req.lease());
    info.kv.set_create_revision(existed ? data_[req.key()].createRevision : newRevision);
    info.kv.set_mod_revision(newRevision);
    info.kv.set_version(existed ? data_[req.key()].version + 1 : 1);
    info.createRevision = info.kv.create_revision();
    info.modRevision = info.kv.mod_revision();
    info.version = info.kv.version();

    // Inherit history and record PUT event
    InheritKeyHistory(req.key(), &info);
    RecordPutHistory(&info, newRevision);

    data_[req.key()] = info;

    auto *putResp = respOp->mutable_response_put();
    if (req.prev_kv()) {
        *putResp->mutable_prev_kv() = prevKv;
    }

    // Collect watch event
    if (events) {
        WatchEvent evt;
        evt.type = WatchEvent::Type::PUT;
        evt.key = req.key();
        evt.newKv = info.kv;
        evt.prevKv = prevKv;
        evt.hasPrevKv = existed;
        events->push_back(evt);
    }
}

void KVManager::ExecuteDeleteRangeOp(const etcdserverpb::DeleteRangeRequest &req, int64_t newRevision,
                                     etcdserverpb::ResponseOp *respOp,
                                     std::unordered_map<std::string, int64_t> *deletedLeases,
                                     std::vector<WatchEvent> *events)
{
    std::vector<std::string> keysToDelete;
    std::vector<mvccpb::KeyValue> deletedKvs;

    for (auto it = data_.begin(); it != data_.end(); ++it) {
        if (KeyInRange(it->first, req.key(), req.range_end())) {
            keysToDelete.push_back(it->first);
            if (req.prev_kv()) {
                deletedKvs.push_back(it->second.kv);
            }
            // Record lease ID for lease tracking
            if (deletedLeases && it->second.kv.lease() != 0) {
                (*deletedLeases)[it->first] = it->second.kv.lease();
            }
            // Collect watch event
            if (events) {
                WatchEvent evt;
                evt.type = WatchEvent::Type::DELETE;
                evt.key = it->first;
                evt.newKv = it->second.kv;
                // Update mod_revision to delete operation's revision
                evt.newKv.set_mod_revision(newRevision);
                events->push_back(evt);
            }
        }
    }

    for (const auto &k : keysToDelete) {
        auto it = data_.find(k);
        if (it != data_.end()) {
            // Update mod_revision to delete operation's revision
            mvccpb::KeyValue kv = it->second.kv;
            kv.set_mod_revision(newRevision);
            // Record delete history before erasing
            RecordDeleteHistory(k, kv, newRevision);
            data_.erase(k);
        }
    }

    auto *delResp = respOp->mutable_response_delete_range();
    delResp->set_deleted(keysToDelete.size());
    if (req.prev_kv()) {
        for (const auto &kv : deletedKvs) {
            *delResp->add_prev_kvs() = kv;
        }
    }
}

void KVManager::ExecuteRangeOp(const etcdserverpb::RangeRequest &req, etcdserverpb::ResponseOp *respOp)
{
    std::vector<mvccpb::KeyValue> kvs;
    Range(req.key(), req.range_end(), &kvs, req.limit(), req.count_only(), req.keys_only());

    auto *rangeResp = respOp->mutable_response_range();
    for (const auto &kv : kvs) {
        *rangeResp->add_kvs() = kv;
    }
    rangeResp->set_count(kvs.size());
    rangeResp->set_more(false);
}

bool KVManager::EvaluateCompares(const std::vector<etcdserverpb::Compare> &compares) const
{
    for (const auto &cmp : compares) {
        auto it = data_.find(cmp.key());
        if (!EvaluateCompare(cmp, it != data_.end() ? &it->second : nullptr)) {
            return false;
        }
    }
    return true;
}

Status KVManager::ValidateTxnLeases(const std::vector<etcdserverpb::RequestOp> &ops) const
{
    for (const auto &op : ops) {
        if (op.has_request_put() && op.request_put().lease() != 0) {
            if (leaseManager_ && !leaseManager_->LeaseExists(op.request_put().lease())) {
                return Status(StatusCode::K_NOT_FOUND, "Lease not found: " + std::to_string(op.request_put().lease()));
            }
        }
    }
    return Status::OK();
}

void KVManager::ProcessTxnLeaseChanges(const std::vector<std::tuple<std::string, int64_t, int64_t>> &putLeases,
                                       const std::unordered_map<std::string, int64_t> &deletedLeases) const
{
    if (!leaseManager_) {
        return;
    }

    // Handle detach from old leases for Put operations
    for (const auto &[key, oldLease, newLease] : putLeases) {
        if (oldLease != 0) {
            leaseManager_->DetachKey(oldLease, key);
        }
        if (newLease != 0) {
            leaseManager_->AttachKey(newLease, key);
        }
    }

    // Handle detach for Delete operations
    for (const auto &[key, leaseId] : deletedLeases) {
        leaseManager_->DetachKey(leaseId, key);
    }
}

void KVManager::TriggerWatchEvents(const std::vector<WatchEvent> &events) const
{
    for (const auto &evt : events) {
        if (evt.type == WatchEvent::Type::PUT && putCallback_) {
            putCallback_(evt.key, evt.newKv, evt.hasPrevKv ? &evt.prevKv : nullptr);
        } else if (evt.type == WatchEvent::Type::DELETE && deleteCallback_) {
            deleteCallback_(evt.key, evt.newKv);
        }
    }
}

Status KVManager::Txn(const std::vector<etcdserverpb::Compare> &compares,
                      const std::vector<etcdserverpb::RequestOp> &success,
                      const std::vector<etcdserverpb::RequestOp> &failure, etcdserverpb::TxnResponse *response)
{
    // Collect lease changes and watch events for post-processing
    std::vector<std::tuple<std::string, int64_t, int64_t>> putLeases;  // (key, oldLease, newLease)
    std::unordered_map<std::string, int64_t> deletedLeases;            // key -> leaseId
    std::vector<WatchEvent> watchEvents;                               // All watch events

    // Validate leases for all Put operations BEFORE acquiring the lock
    // This ensures early failure and consistency with standalone Put() method
    Status status = ValidateTxnLeases(success);
    if (status.IsError()) {
        return status;
    }
    status = ValidateTxnLeases(failure);
    if (status.IsError()) {
        return status;
    }

    std::unique_lock<std::shared_mutex> lock(mutex_);
    int64_t newRevision = revision_;  // Use current revision, will increment on first write

    // Evaluate all Compare operations
    bool compareResult = EvaluateCompares(compares);

    // Execute success or failure operations
    const auto &ops = compareResult ? success : failure;
    response->set_succeeded(compareResult);

    bool hasWriteOp = false;
    for (const auto &op : ops) {
        auto *respOp = response->add_responses();
        if (op.has_request_put()) {
            if (!hasWriteOp) {
                newRevision = ++revision_;
                hasWriteOp = true;
            }
            int64_t oldLease = 0;
            ExecutePutOp(op.request_put(), newRevision, respOp, &oldLease, &watchEvents);
            putLeases.emplace_back(op.request_put().key(), oldLease, op.request_put().lease());
        } else if (op.has_request_delete_range()) {
            if (!hasWriteOp) {
                newRevision = ++revision_;
                hasWriteOp = true;
            }
            ExecuteDeleteRangeOp(op.request_delete_range(), newRevision, respOp, &deletedLeases, &watchEvents);
        } else if (op.has_request_range()) {
            ExecuteRangeOp(op.request_range(), respOp);
        }
    }

    // Fill response header
    auto *header = response->mutable_header();
    header->set_revision(newRevision);

    // Lock released here - process lease bindings and watch events outside lock
    ProcessTxnLeaseChanges(putLeases, deletedLeases);
    TriggerWatchEvents(watchEvents);

    return Status::OK();
}

Status KVManager::GetHistory(const std::string &key, int64_t startRevision, int64_t endRevision,
                             std::vector<HistoricalEntry> *events)
{
    std::shared_lock<std::shared_mutex> lock(mutex_);

    // Check if key exists in data_
    auto it = data_.find(key);
    if (it != data_.end()) {
        std::vector<HistoricalEntry> historical = it->second.history.GetRange(startRevision, endRevision);
        events->insert(events->end(), historical.begin(), historical.end());
        return Status::OK();
    }

    // Check deleted history
    auto delIt = deletedHistory_.find(key);
    if (delIt != deletedHistory_.end()) {
        std::vector<HistoricalEntry> historical = delIt->second.GetRange(startRevision, endRevision);
        events->insert(events->end(), historical.begin(), historical.end());
        return Status::OK();
    }

    // Key not found
    return Status::OK();
}

Status KVManager::RangeHistory(const std::string &start, const std::string &end, int64_t startRevision,
                               std::vector<HistoricalEntry> *events)
{
    std::shared_lock<std::shared_mutex> lock(mutex_);

    std::vector<std::tuple<std::string, HistoricalEntry>> allEvents;

    // Collect events from existing keys
    for (const auto &[key, info] : data_) {
        if (KeyInRange(key, start, end)) {
            std::vector<HistoricalEntry> historical = info.history.GetRange(startRevision);
            for (const auto &entry : historical) {
                allEvents.emplace_back(key, entry);
            }
        }
    }

    // Collect events from deleted keys
    for (const auto &[key, history] : deletedHistory_) {
        if (KeyInRange(key, start, end)) {
            std::vector<HistoricalEntry> historical = history.GetRange(startRevision);
            for (const auto &entry : historical) {
                allEvents.emplace_back(key, entry);
            }
        }
    }

    // Sort by key first, then by revision
    std::sort(allEvents.begin(), allEvents.end(), [](const auto &a, const auto &b) {
        if (std::get<0>(a) != std::get<0>(b)) {
            return std::get<0>(a) < std::get<0>(b);
        }
        return std::get<1>(a).revision < std::get<1>(b).revision;
    });

    // Extract events
    for (auto it = allEvents.begin(); it != allEvents.end(); ++it) {
        events->push_back(std::get<1>(*it));
    }

    return Status::OK();
}

void KVManager::SetWatchCallback(
    std::function<void(const std::string &, const mvccpb::KeyValue &, const mvccpb::KeyValue *)> putCb,
    std::function<void(const std::string &, const mvccpb::KeyValue &)> delCb)
{
    putCallback_ = putCb;
    deleteCallback_ = delCb;
}

void KVManager::SetLeaseManager(LeaseManager *leaseManager)
{
    leaseManager_ = leaseManager;
}

}  // namespace datasystem
