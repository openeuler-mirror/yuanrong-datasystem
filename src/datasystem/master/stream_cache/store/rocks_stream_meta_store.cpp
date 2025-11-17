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
 * Description: Define interface to store stream meta in RocksDB.
 */
#include "datasystem/master/stream_cache/store/rocks_stream_meta_store.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/constants.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/kvstore/rocksdb/replica.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/uri.h"
#include "datasystem/master/stream_cache/store/stream_transform.h"

namespace datasystem {
namespace master {
namespace stream_cache {
const std::string RocksStreamMetaStore::streamTableName_ = STREAM_TABLE_NAME;
const std::string RocksStreamMetaStore::pubTableName_ = PUB_TABLE_NAME;
const std::string RocksStreamMetaStore::subTableName_ = SUB_TABLE_NAME;
const std::string RocksStreamMetaStore::notifyPubTableName_ = NOTIFY_PUB_TABLE_NAME;
const std::string RocksStreamMetaStore::notifySubTableName_ = NOTIFY_SUB_TABLE_NAME;
const std::string RocksStreamMetaStore::streamConCntTableName_ = STREAM_CON_CNT_TABLE_NAME;
const std::string RocksStreamMetaStore::streamProCntTableName_ = STREAM_PRODUCER_COUNT;
RocksStreamMetaStore::RocksStreamMetaStore(RocksStore *rocksStore) : rocksStore_(std::move(rocksStore))
{
}

Status RocksStreamMetaStore::Init()
{
    RETURN_RUNTIME_ERROR_IF_NULL(rocksStore_);
    return Replica::CreateScTable(rocksStore_);
}

Status RocksStreamMetaStore::AddPubNode(const ProducerMetaPb &producerMeta)
{
    INJECT_POINT("master.RocksStreamMetaStore.DoNotAddPubSubMetadata");
    // Construct key-value pair in rocksdb
    HostPort workerAddr(producerMeta.worker_address().host(), producerMeta.worker_address().port());
    std::string key(producerMeta.stream_name() + PREFIX_SPLITTER + workerAddr.ToString());

    std::string serializedStr;
    CHECK_FAIL_RETURN_STATUS(producerMeta.SerializeToString(&serializedStr), StatusCode::K_UNKNOWN_ERROR,
                             "Failed to Serialize Pub meta");

    std::lock_guard<std::shared_timed_mutex> lock(mutex_);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rocksStore_->Put(pubTableName_, key, serializedStr),
                                     "Failed to add pub meta: " + key);
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s] Success to add pub meta: %s", LogPrefix(), key);
    return Status::OK();
}

Status RocksStreamMetaStore::DelPubNode(const ProducerMetaPb &producerMeta)
{
    // Construct key-value pair in rocksdb
    HostPort workerAddr(producerMeta.worker_address().host(), producerMeta.worker_address().port());
    std::string key(producerMeta.stream_name() + PREFIX_SPLITTER + workerAddr.ToString());

    std::lock_guard<std::shared_timed_mutex> lock(mutex_);
    return rocksStore_->Delete(pubTableName_, key);
}

Status RocksStreamMetaStore::AddSubNode(const ConsumerMetaPb &consumerMeta)
{
    INJECT_POINT("master.RocksStreamMetaStore.DoNotAddPubSubMetadata");
    std::string key(consumerMeta.stream_name() + PREFIX_SPLITTER + consumerMeta.consumer_id());
    std::string serializedStr;
    CHECK_FAIL_RETURN_STATUS(consumerMeta.SerializeToString(&serializedStr), StatusCode::K_UNKNOWN_ERROR,
                             "Failed to Serialize Sub meta");
    std::lock_guard<std::shared_timed_mutex> lock(mutex_);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rocksStore_->Put(subTableName_, key, serializedStr),
                                     "Failed to add sub meta: " + key);
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s] Success to add sub meta: %s", LogPrefix(), key);
    return Status::OK();
}

Status RocksStreamMetaStore::GetLifeTimeConsumerCount(const std::string &streamName, uint32_t &consumerCount)
{
    std::string outValue;
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s] Get consumer life counts", LogPrefix(), streamName);

    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rocksStore_->Get(streamConCntTableName_, streamName, outValue),
                                     "Failed to get all pairs from table: " + streamConCntTableName_);
    lock.unlock();
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("Succeed to get count from table: %s, streamName:<%s>: count:<%s>",
                                              streamConCntTableName_, streamName, outValue);
    consumerCount = static_cast<uint32_t>(std::stoul(outValue));
    return Status::OK();
}

Status RocksStreamMetaStore::UpdateLifeTimeConsumerCount(const std::string &streamName, const uint32_t consumerCount)
{
    std::lock_guard<std::shared_timed_mutex> lock(mutex_);
    const std::string &key(streamName);
    std::string value = std::to_string(consumerCount);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rocksStore_->Put(streamConCntTableName_, key, value),
                                     "Failed to put stream consumer count: " + key);
    return Status::OK();
}

Status RocksStreamMetaStore::DelSubNode(const std::string &streamName, const std::string &consumerId)
{
    std::string key(streamName + PREFIX_SPLITTER + consumerId);
    std::lock_guard<std::shared_timed_mutex> lock(mutex_);
    return rocksStore_->Delete(subTableName_, key);
}

Status RocksStreamMetaStore::AddOrUpdateStream(const std::string &streamName, const StreamFields &streamFields)
{
    const std::string &key(streamName);
    StreamMetaPb streamMetaPb;
    streamMetaPb.set_stream_name(streamName);
    streamMetaPb.set_max_stream_size(streamFields.maxStreamSize_);
    streamMetaPb.set_page_size(streamFields.pageSize_);
    streamMetaPb.set_auto_cleanup(streamFields.autoCleanup_);
    streamMetaPb.set_retain_num_consumer(streamFields.retainForNumConsumers_);
    streamMetaPb.set_encrypt_stream(streamFields.encryptStream_);
    streamMetaPb.set_reserve_size(streamFields.reserveSize_);
    streamMetaPb.set_stream_mode(streamFields.streamMode_);
    std::string serializedStr;
    CHECK_FAIL_RETURN_STATUS(streamMetaPb.SerializeToString(&serializedStr), StatusCode::K_UNKNOWN_ERROR,
                             "Failed to Serialize Stream meta");
    std::lock_guard<std::shared_timed_mutex> lock(mutex_);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rocksStore_->Put(streamTableName_, key, serializedStr),
                                     "Failed to put stream meta: " + key);
    return Status::OK();
}

Status RocksStreamMetaStore::AddStream(const std::string &streamName, const StreamFields &streamFields)
{
    RETURN_IF_NOT_OK(AddOrUpdateStream(streamName, streamFields));
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s] Success to add stream meta: %s", LogPrefix(), streamName);
    return Status::OK();
}

Status RocksStreamMetaStore::UpdateStream(const std::string &streamName, const StreamFields &streamFields)
{
    RETURN_IF_NOT_OK(AddOrUpdateStream(streamName, streamFields));
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s] Success to update stream meta: %s", LogPrefix(), streamName);
    return Status::OK();
}

Status RocksStreamMetaStore::DelStream(const std::string &streamName)
{
    std::lock_guard<std::shared_timed_mutex> lock(mutex_);
    return rocksStore_->Delete(streamTableName_, streamName);
}

Status RocksStreamMetaStore::GetOneStreamProducers(const std::string &streamName,
                                                   std::vector<ProducerMetaPb> &pubWorkerMetas)
{
    std::vector<std::pair<std::string, std::string>> outKeyValues;
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s] Get pub nodes", LogPrefix(), streamName);

    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        rocksStore_->PrefixSearch(pubTableName_, streamName + PREFIX_SPLITTER, outKeyValues),
        "Failed to get all pairs from table: " + pubTableName_);
    lock.unlock();

    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("Succeed to get all pairs from table: %s, Pub outKeyValues.size() = %zu",
                                              pubTableName_, outKeyValues.size());
    ProducerMetaPb producerMetaPb;
    for (const auto &outKeyValue : outKeyValues) {
        CHECK_FAIL_RETURN_STATUS(producerMetaPb.ParseFromString(outKeyValue.second), StatusCode::K_UNKNOWN_ERROR,
                                 "Parse string to producerMetaPb failed.");
        if (streamName != producerMetaPb.stream_name()) {
            continue;
        }
        pubWorkerMetas.emplace_back(producerMetaPb);
        VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("HostPort:<%s:%d>, streamName:<%s>",
                                                  producerMetaPb.worker_address().host(),
                                                  producerMetaPb.worker_address().port(), producerMetaPb.stream_name());
    }
    return Status::OK();
}

Status RocksStreamMetaStore::GetOneStreamConsumers(const std::string &streamName,
                                                   std::vector<ConsumerMetaPb> &consumerMetas)
{
    std::vector<std::pair<std::string, std::string>> outKeyValues;
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s] Get consumers", LogPrefix(), streamName);

    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        rocksStore_->PrefixSearch(subTableName_, streamName + PREFIX_SPLITTER, outKeyValues),
        "Failed to get all pairs from table: " + subTableName_);
    lock.unlock();

    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString(
        "Succeed to get all pairs from table: %s, Consumer outKeyValues.size() = %zu", subTableName_,
        outKeyValues.size());
    ConsumerMetaPb consumerMetaPb;
    for (const auto &outKeyValue : outKeyValues) {
        CHECK_FAIL_RETURN_STATUS(consumerMetaPb.ParseFromString(outKeyValue.second), StatusCode::K_UNKNOWN_ERROR,
                                 "Parse string to ConsumerMetaPb failed.");
        if (streamName != consumerMetaPb.stream_name()) {
            continue;
        }
        consumerMetas.emplace_back(std::move(consumerMetaPb));
    }
    return Status::OK();
}

Status RocksStreamMetaStore::GetAllStream(std::vector<StreamMetaPb> &streamMetas)
{
    std::vector<std::pair<std::string, std::string>> outKeyValues;

    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rocksStore_->GetAll(streamTableName_, outKeyValues),
                                     FormatString("Fail to get all pairs from table:<%s>", streamTableName_));
    lock.unlock();

    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString(
        "Succeed to get all pairs from table: %s, stream outKeyValues.size() = %zu", streamTableName_,
        outKeyValues.size());
    StreamMetaPb streamMetaPb;
    for (const auto &outKeyValue : outKeyValues) {
        CHECK_FAIL_RETURN_STATUS(streamMetaPb.ParseFromString(outKeyValue.second), StatusCode::K_UNKNOWN_ERROR,
                                 "Parse string to streamMetaPb failed.");
        CHECK_FAIL_RETURN_STATUS(
            streamMetaPb.stream_name() == outKeyValue.first, StatusCode::K_RUNTIME_ERROR,
            FormatString("Key:<%s>, value:<%s> are not equal", outKeyValue.first, streamMetaPb.stream_name()));
        streamMetas.emplace_back(streamMetaPb);
    }
    return Status::OK();
}

Status RocksStreamMetaStore::AddNotifyPub(const std::string &workerAddr, const NotifyPubPb &pub)
{
    std::string key = workerAddr + "_" + pub.stream_name() + "_" + pub.worker_addr();
    std::string value;
    if (!pub.SerializeToString(&value)) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Failed to Serialize NotifyPubPb");
    }
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rocksStore_->Put(notifyPubTableName_, key, value),
                                     FormatString("Failed to put: %s", key));
    return Status::OK();
}

Status RocksStreamMetaStore::RemoveNotifyPub(const std::string &workerAddr, const NotifyPubPb &pub)
{
    std::string key = workerAddr + "_" + pub.stream_name() + "_" + pub.worker_addr();
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rocksStore_->Delete(notifyPubTableName_, key),
                                     FormatString("Failed to delete: %s", key));
    return Status::OK();
}

Status RocksStreamMetaStore::AddNotifySub(const std::string &workerAddr, const NotifyConsumerPb &sub)
{
    const auto &meta = sub.consumer();
    std::string key = workerAddr + "_" + meta.stream_name() + "_" + meta.consumer_id();
    std::string value;
    if (!sub.SerializeToString(&value)) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Failed to Serialize NotifyConsumerPb");
    }
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rocksStore_->Put(notifySubTableName_, key, value),
                                     FormatString("Failed to put: %s", key));
    return Status::OK();
}

Status RocksStreamMetaStore::RemoveNotifySub(const std::string &workerAddr, const NotifyConsumerPb &sub)
{
    const auto &meta = sub.consumer();
    std::string key = workerAddr + "_" + meta.stream_name() + "_" + meta.consumer_id();
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rocksStore_->Delete(notifySubTableName_, key),
                                     FormatString("Failed to delete: %s", key));
    return Status::OK();
}

Status RocksStreamMetaStore::RemoveNotificationByWorker(const std::string &workerAddr)
{
    std::string prefixKey = workerAddr + "_";
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rocksStore_->PrefixDelete(notifyPubTableName_, prefixKey),
                                     FormatString("Failed to delete prefix key: %s", prefixKey));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rocksStore_->PrefixDelete(notifySubTableName_, prefixKey),
                                     FormatString("Failed to delete prefix key: %s", prefixKey));
    return Status::OK();
}

Status RocksStreamMetaStore::GetAllNotifyPub(std::vector<std::pair<std::string, NotifyPubPb>> &pubs)
{
    std::vector<std::pair<std::string, std::string>> outKeyValues;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rocksStore_->GetAll(notifyPubTableName_, outKeyValues),
                                     FormatString("Failed to get all pairs from table: %s", notifyPubTableName_));

    NotifyPubPb pub;
    pubs.clear();
    pubs.reserve(outKeyValues.size());
    for (const auto &kv : outKeyValues) {
        CHECK_FAIL_RETURN_STATUS(pub.ParseFromString(kv.second), StatusCode::K_UNKNOWN_ERROR,
                                 "Parse string to NotifyPubPb failed.");
        pubs.emplace_back(kv.first, std::move(pub));
    }
    return Status::OK();
}

Status RocksStreamMetaStore::GetAllNotifySub(std::vector<std::pair<std::string, NotifyConsumerPb>> &subs)
{
    std::vector<std::pair<std::string, std::string>> outKeyValues;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rocksStore_->GetAll(notifySubTableName_, outKeyValues),
                                     FormatString("Failed to get all pairs from table: %s", notifySubTableName_));

    NotifyConsumerPb sub;
    subs.clear();
    subs.reserve(outKeyValues.size());
    for (const auto &kv : outKeyValues) {
        CHECK_FAIL_RETURN_STATUS(sub.ParseFromString(kv.second), StatusCode::K_UNKNOWN_ERROR,
                                 "Parse string to NotifyConsumerPb failed.");
        subs.emplace_back(kv.first, std::move(sub));
    }
    return Status::OK();
}

std::string RocksStreamMetaStore::LogPrefix()
{
    return "SC Rocksdb on master";
}
}  // namespace stream_cache
}  // namespace master
}  // namespace datasystem
