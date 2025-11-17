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
 * Description: Declare interface to store stream meta in RocksDB.
 */
#ifndef DATASYSTEM_MASTER_STREAM_CACHE_STORE_ROCKS_STREAM_META_STORE_H
#define DATASYSTEM_MASTER_STREAM_CACHE_STORE_ROCKS_STREAM_META_STORE_H

#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>

#include "datasystem/common/constants.h"
#include "datasystem/common/kvstore/rocksdb/rocks_store.h"
#include "datasystem/common/stream_cache/stream_fields.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/protos/master_stream.pb.h"
#include "datasystem/protos/worker_stream.pb.h"
#include "datasystem/stream/stream_config.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace master {
namespace stream_cache {

// Definition:
// PubNode: Worker node which contains at least one producer.
// SubNode: Consumer node which includes all information about one consumer.
class RocksStreamMetaStore {
public:
    /**
     * @brief Construct RocksStreamMetaStore.
     */
    explicit RocksStreamMetaStore(RocksStore *rocksStore);

    /**
     * @brief Used path backStorePath to start rocksdb in rocksStore.
     * @return Status of the call.
     */
    Status Init();

    /**
     * @brief Add Pub node meta in Rocksdb.
     * @param[in] producerMeta The producer metadata.
     * @return Status of the call.
     */
    Status AddPubNode(const ProducerMetaPb &producerMeta);

    /**
     * @brief Remove pub meta from Rocksdb.
     * @param[in] producerMeta The producer metadata.
     * @return Status of the call.
     */
    Status DelPubNode(const ProducerMetaPb &producerMeta);

    /**
     * @brief Add Sub consumer meta in Rocksdb.
     * @param[in] consumerMeta The consumer metadata.
     * @return Status of the call.
     */
    Status AddSubNode(const ConsumerMetaPb &consumerMeta);

    /**
     * @brief Remove consumer meta from Rocksdb.
     * @param[in] streamName The stream name.
     * @param[in] consumerId The consumer id.
     * @return Status of the call.
     */
    Status DelSubNode(const std::string &streamName, const std::string &consumerId);

    /**
     * @brief Add stream meta in Rocksdb, we save the key as streamName.
     * @param[in] streamName The target stream.
     * @param[in] streamFields The stream fields
     * @return Status of the call.
     */
    Status AddStream(const std::string &streamName, const StreamFields &streamFields);

    /**
     * @brief Update stream meta in Rocksdb, we save the key as streamName.
     * @param[in] streamName The target stream.
     * @param[in] streamFields The stream fields
     * @return Status of the call.
     */
    Status UpdateStream(const std::string &streamName, const StreamFields &streamFields);

    /**
     * @brief Remove stream meta from Rocksdb.
     * @param[in] streamName The stream meta key to be removed.
     * @return Status of the call.
     */
    Status DelStream(const std::string &streamName);

    /**
     * @brief Updates consumer lifetime count in Rocksdb.
     * @param[in] streamName The stream name.
     * @param[in] consumerCount The consumer count to be updated
     * @return Status of the call.
     */
    Status UpdateLifeTimeConsumerCount(const std::string &streamName, const uint32_t consumerCount);

    /**
     * @brief Gets consumer lifetime count from Rocksdb.
     * @param[in] streamName The stream meta key to be removed.
     * @param[out] consumerCount The consumer count
     * @return Status of the call.
     */
    Status GetLifeTimeConsumerCount(const std::string &streamName, uint32_t &consumerCount);

    /**
     * @brief Get all pub node k-v pairs from rocksdb pubTable for a stream.
     * @param[in] streamName The stream name.
     * @param[out] producerMetas The producer metadata.
     * @return Status of the call.
     */
    Status GetOneStreamProducers(const std::string &streamName, std::vector<ProducerMetaPb> &producerMetas);

    /**
     * @brief Get all sub node k-v pairs from rocksdb subTable for a stream.
     * @param[in] streamName The stream name.
     * @param[out] consumerMetas The consumer metadata.
     * @return Status of the call.
     */
    Status GetOneStreamConsumers(const std::string &streamName, std::vector<ConsumerMetaPb> &consumerMetas);

    /**
     * @brief Get all stream k-v pairs from rocksdb streamTable.
     * @param[out] streamMetas The output metas in table.
     * @return Status of the call.
     */
    Status GetAllStream(std::vector<StreamMetaPb> &streamMetas);

    /**
     * @brief Add pub node change notification to Rocksdb.
     * @param[in] workerAddr The target worker address.
     * @param[in] pub The pub metadata.
     * @return Status of the call.
     */
    Status AddNotifyPub(const std::string &workerAddr, const NotifyPubPb &pub);

    /**
     * @brief Remote pub node change notification in Rocksdb.
     * @param[in] workerAddr The target worker address.
     * @param[in] pub The pub metadata.
     * @return Status of the call.
     */
    Status RemoveNotifyPub(const std::string &workerAddr, const NotifyPubPb &pub);

    /**
     * @brief Add sub node change notification to Rocksdb.
     * @param[in] workerAddr The target worker address.
     * @param[in] sub The sub metadata.
     * @return Status of the call.
     */
    Status AddNotifySub(const std::string &workerAddr, const NotifyConsumerPb &sub);

    /**
     * @brief Remove sub node change notification in Rocksdb.
     * @param[in] workerAddr The target worker address.
     * @param[in] sub The sub metadata.
     * @return Status of the call.
     */
    Status RemoveNotifySub(const std::string &workerAddr, const NotifyConsumerPb &sub);

    /**
     * @brief Remove all notification send to the specific worker.
     * @param[in] workerAddr
     * @return Status of the call.
     */
    Status RemoveNotificationByWorker(const std::string &workerAddr);

    /**
     * @brief Get the all pub change notification from Rocksdb.
     * @param[out] pubs The list of pub change notification.
     * @return Status of the call.
     */
    Status GetAllNotifyPub(std::vector<std::pair<std::string, NotifyPubPb>> &pubs);

    /**
     * @brief Get the all sub change notification from Rocksdb.
     * @param[out] pubs The list of sub change notification.
     * @return Status of the call.
     */
    Status GetAllNotifySub(std::vector<std::pair<std::string, NotifyConsumerPb>> &subs);

    /**
     * @brief Get log prefix
     * @return The log prefix
     */
    static std::string LogPrefix();

private:

    /**
     * @brief Add or update stream meta in Rocksdb, we save the key as streamName.
     * @param[in] streamName The target stream.
     * @param[in] streamFields The stream fields
     * @return Status of the call.
     */
    Status AddOrUpdateStream(const std::string &streamName, const StreamFields &streamFields);

    const static std::string streamTableName_;        // the stream meta table name
    const static std::string pubTableName_;           // the global pub table name
    const static std::string subTableName_;           // the global sub table name
    const static std::string notifyPubTableName_;     // the notify pub table name
    const static std::string notifySubTableName_;     // the notify sub table name
    const static std::string streamConCntTableName_;  // the table to store lifetime consumer count
    const static std::string streamProCntTableName_;  // the table to store lifetime producer count

    std::shared_timed_mutex mutex_;  // Concurrent control for this class

    // The backend rocksdb storage.
    RocksStore *rocksStore_;
};
}  // namespace stream_cache
}  // namespace master
}  // namespace datasystem
#endif
