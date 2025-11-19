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
 * Description: Test ObjectMeta Storage basic functions.
 */

#include <list>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "common.h"
#include "datasystem/common/stream_cache/stream_fields.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/master/stream_cache/store/rocks_stream_meta_store.h"
#include "datasystem/protos/master_stream.pb.h"
#include "datasystem/stream/stream_config.h"

using namespace datasystem::master::stream_cache;
DS_DECLARE_string(rocksdb_write_mode);
namespace datasystem {
namespace ut {
class RocksStreamMetaStoreTest : public CommonTest {
public:
    void SetUp()
    {
        FLAGS_rocksdb_write_mode = "sync";
        backStorePath_ = "rocks_streammeta_store_" + random_.GetRandomString(8);
        rocksStore_ = RocksStore::GetInstance(backStorePath_);
        rocksStreamMetaStore_ = std::make_unique<RocksStreamMetaStore>(rocksStore_.get());
        CHECK_EQ(rocksStreamMetaStore_->Init(), Status::OK());
    }

    void TearDown()
    {
        rocksStreamMetaStore_.reset();
        DS_ASSERT_OK(RemoveAll(backStorePath_));
    }

    void MakePubWorkerMetas(size_t createNum, std::unordered_map<std::string, ProducerMetaPb> &produerMetaPbs)
    {
        for (size_t i = 0; i < createNum; i++) {
            ProducerMetaPb pubMetaPb;
            std::string streamName = "streamName";
            pubMetaPb.set_stream_name(streamName + std::to_string(i));
            pubMetaPb.mutable_worker_address()->set_host("127.0.0.1");
            pubMetaPb.mutable_worker_address()->set_port(1000);
            produerMetaPbs.emplace(pubMetaPb.stream_name(), std::move(pubMetaPb));
        }
    }

    void MakeConsumerMetas(size_t createNum, std::unordered_map<std::string, ConsumerMetaPb> &consumerMetas)
    {
        for (size_t i = 0; i < createNum; i++) {
            ConsumerMetaPb subMeta;
            subMeta.set_stream_name("streamName" + std::to_string(i));
            subMeta.mutable_worker_address()->set_host("127.0.0.1");
            subMeta.mutable_worker_address()->set_port(1234);
            subMeta.set_consumer_id(GetStringUuid());
            std::string subName("sub_" + std::to_string(i));
            subMeta.mutable_sub_config()->set_subscription_name(subName);
            subMeta.mutable_sub_config()->set_subscription_type(SubscriptionTypePb::STREAM_PB);
            subMeta.set_last_ack_cursor(0);
            consumerMetas.emplace(subMeta.stream_name(), subMeta);
        }
    }

    inline std::string HostPb2Str(const HostPortPb &hostPb) noexcept
    {
        HostPort addr(hostPb.host(), hostPb.port());
        return addr.ToString();
    }

    void MakeStreamMetas(size_t createNum, std::vector<std::string> &streamMetas)
    {
        for (size_t i = 0; i < createNum; i++) {
            std::string streamName = "streamName" + std::to_string(i);
            streamMetas.emplace_back(streamName);
        }
    }

    void MakeProducerExistIds(const std::unordered_map<std::string, ProducerMetaPb> &producerMetas,
                              std::unordered_map<std::string, ProducerMetaPb> &queryIds)
    {
        size_t index = 0;
        for (const auto &meta : producerMetas) {
            if (index % 2 == 0) {
                LOG(INFO) << "Producer streamName:" << meta.first;
                queryIds.emplace(meta.first, meta.second);
            }
            index++;
        }
    }

    void MakeConsumerExistIds(const std::unordered_map<std::string, ConsumerMetaPb> &consumerMetas,
                              std::unordered_map<std::string, std::string> &queryIds)
    {
        size_t index = 0;
        for (const auto &meta : consumerMetas) {
            LOG(INFO) << "consumer streamName:" << meta.second.stream_name();
            queryIds.emplace(meta.second.stream_name(), meta.second.consumer_id());
            index++;
        }
    }

    void MakeStreamExistIds(const std::vector<std::string> &streamMetas, std::list<std::string> &queryIds)
    {
        size_t index = 0;
        for (const auto &meta : streamMetas) {
            queryIds.emplace_back(meta);
            LOG(INFO) << "meta.second.streamName:" << meta;
            index++;
        }
    }

    Status StorePubWorker(const std::unordered_map<std::string, ProducerMetaPb> &producerMetas)
    {
        for (const auto &kv : producerMetas) {
            RETURN_IF_NOT_OK(rocksStreamMetaStore_->AddPubNode(kv.second));
        }
        return Status::OK();
    }

    Status StoreConsumer(const std::unordered_map<std::string, ConsumerMetaPb> &consumerMetas)
    {
        LOG(INFO) << "inMetas:" << consumerMetas.size();
        for (const auto &kv : consumerMetas) {
            RETURN_IF_NOT_OK(rocksStreamMetaStore_->AddSubNode(kv.second));
        }
        return Status::OK();
    }

    Status StoreStream(const std::vector<std::string> &inMetas, const StreamFields &streamFields)
    {
        for (const auto &meta : inMetas) {
            RETURN_IF_NOT_OK(rocksStreamMetaStore_->AddStream(meta, streamFields));
        }
        return Status::OK();
    }

    Status GetAllPubWorkers(const std::unordered_map<std::string, ProducerMetaPb> &removeKeys,
                            std::vector<ProducerMetaPb> &pubWorkerMetas)
    {
        for (const auto &removeKey : removeKeys) {
            std::string streamName = removeKey.first;
            LOG(INFO) << " PubWorker streamName:" << streamName;
            RETURN_IF_NOT_OK(rocksStreamMetaStore_->GetOneStreamProducers(streamName, pubWorkerMetas));
        }
        RETURN_IF_NOT_OK(rocksStreamMetaStore_->GetOneStreamProducers("streamName4", pubWorkerMetas));
        return Status::OK();
    }

    Status GetAllConsumer(const std::unordered_map<std::string, std::string> &removeKeys,
                          std::vector<ConsumerMetaPb> &consumerMetas)
    {
        for (const auto &removeKey : removeKeys) {
            std::string streamName = removeKey.first;
            LOG(INFO) << " Consumer streamName:" << streamName;
            RETURN_IF_NOT_OK(rocksStreamMetaStore_->GetOneStreamConsumers(streamName, consumerMetas));
        }
        return Status::OK();
    }

    Status StoreGetStream(std::vector<master::StreamMetaPb> &streamMetas)
    {
        RETURN_IF_NOT_OK(rocksStreamMetaStore_->GetAllStream(streamMetas));
        return Status::OK();
    }

    Status AddRemovePubWorker(const std::unordered_map<std::string, ProducerMetaPb> &removeKeys)
    {
        for (const auto &removeKey : removeKeys) {
            RETURN_IF_NOT_OK(this->rocksStreamMetaStore_->DelPubNode(removeKey.second));
        }
        return Status::OK();
    }

    Status AddRemoveConsumer(const std::unordered_map<std::string, std::string> &removeKeys)
    {
        for (const auto &removeKey : removeKeys) {
            const std::string &streamName = removeKey.first;
            const std::string &consumerId = removeKey.second;
            RETURN_IF_NOT_OK(this->rocksStreamMetaStore_->DelSubNode(streamName, consumerId));
        }
        return Status::OK();
    }

    Status AddRemoveStream(std::list<std::string> &removeKeys)
    {
        for (const auto &removeKey : removeKeys) {
            const std::string &streamName = removeKey;
            RETURN_IF_NOT_OK(this->rocksStreamMetaStore_->DelStream(streamName));
        }
        return Status::OK();
    }

    std::string backStorePath_;
    static RandomData random_;
    std::shared_ptr<RocksStore> rocksStore_;
    std::unique_ptr<RocksStreamMetaStore> rocksStreamMetaStore_;
};

RandomData RocksStreamMetaStoreTest::random_;

TEST_F(RocksStreamMetaStoreTest, TestCreateQueryRemovePubWorkerMeta)
{
    // Create
    size_t createNum = 10;
    std::unordered_map<std::string, ProducerMetaPb> producerMetas;
    this->MakePubWorkerMetas(createNum, producerMetas);
    EXPECT_EQ(this->StorePubWorker(producerMetas), Status::OK());
    // Create same
    EXPECT_EQ(this->StorePubWorker(producerMetas), Status::OK());
    std::unordered_map<std::string, ProducerMetaPb> removeKeys;
    this->MakeProducerExistIds(producerMetas, removeKeys);

    // get data from rocksdb
    std::vector<ProducerMetaPb> producerOutMetas;
    EXPECT_EQ(this->GetAllPubWorkers(removeKeys, producerOutMetas), Status::OK());

    // Remove exist
    EXPECT_EQ(this->AddRemovePubWorker(removeKeys), Status::OK());
    // Remove not exist
    EXPECT_EQ(this->AddRemovePubWorker(removeKeys), Status::OK());
}

TEST_F(RocksStreamMetaStoreTest, TestCreateQueryRemoveSubWorkerMeta)
{
    // Create
    size_t createNum = 2;
    std::unordered_map<std::string, ConsumerMetaPb> consumerMetas;
    this->MakeConsumerMetas(createNum, consumerMetas);
    EXPECT_EQ(this->StoreConsumer(consumerMetas), Status::OK());
    // Create same
    EXPECT_EQ(this->StoreConsumer(consumerMetas), Status::OK());
    std::unordered_map<std::string, std::string> removeKeys;
    this->MakeConsumerExistIds(consumerMetas, removeKeys);

    // get data from rocksdb
    std::vector<ConsumerMetaPb> consumerOutMetas;
    EXPECT_EQ(this->GetAllConsumer(removeKeys, consumerOutMetas), Status::OK());

    // Remove exist
    EXPECT_EQ(this->AddRemoveConsumer(removeKeys), Status::OK());
    // Remove not exist
    EXPECT_EQ(this->AddRemoveConsumer(removeKeys), Status::OK());
}

TEST_F(RocksStreamMetaStoreTest, TestCreateQueryRemoveStreamWorkerMeta)
{
    // Create
    size_t createNum = 10;
    const uint64_t maxStreamSize = 1024 * 1024 * 1024;  // 1G max stream size
    const int64_t pageSize = 1024 * 1024;
    StreamFields streamFields(maxStreamSize, pageSize, false, 0, false, 0, StreamMode::MPMC);
    std::vector<std::string> streamMetas;
    this->MakeStreamMetas(createNum, streamMetas);
    EXPECT_EQ(this->StoreStream(streamMetas, streamFields), Status::OK());
    // Create same
    EXPECT_EQ(this->StoreStream(streamMetas, streamFields), Status::OK());
    std::list<std::string> removeKeys;
    this->MakeStreamExistIds(streamMetas, removeKeys);

    // get data from rocksdb
    std::vector<master::StreamMetaPb> streamOutMetas;
    EXPECT_EQ(this->StoreGetStream(streamOutMetas), Status::OK());

    for (const auto &meta : streamOutMetas) {
        EXPECT_EQ(meta.max_stream_size(), maxStreamSize);
        EXPECT_EQ(meta.page_size(), pageSize);
        EXPECT_EQ(meta.auto_cleanup(), false);
    }

    // Remove exist
    EXPECT_EQ(this->AddRemoveStream(removeKeys), Status::OK());
    // Remove not exist
    EXPECT_EQ(this->AddRemoveStream(removeKeys), Status::OK());
}

TEST_F(RocksStreamMetaStoreTest, TestAddRemoveGetNotification)
{
    std::string worekrAddr1 = "127.0.0.1:8001";
    std::string worekrAddr2 = "127.0.0.1:8002";
    std::string worekrAddr3 = "127.0.0.1:8003";

    const size_t createNum = 3;
    // test pub
    NotifyPubPb pubPbs[createNum];
    pubPbs[0].set_stream_name("stream1");
    pubPbs[0].set_worker_addr(worekrAddr2);
    pubPbs[1].set_stream_name("stream2");
    pubPbs[1].set_worker_addr(worekrAddr3);
    pubPbs[1].set_is_close(true);
    pubPbs[2].set_stream_name("stream3");
    pubPbs[2].set_worker_addr(worekrAddr1);

    DS_ASSERT_OK(rocksStreamMetaStore_->AddNotifyPub(worekrAddr1, pubPbs[0]));
    DS_ASSERT_OK(rocksStreamMetaStore_->AddNotifyPub(worekrAddr2, pubPbs[1]));
    DS_ASSERT_OK(rocksStreamMetaStore_->AddNotifyPub(worekrAddr3, pubPbs[0]));
    DS_ASSERT_OK(rocksStreamMetaStore_->AddNotifyPub(worekrAddr3, pubPbs[1]));
    DS_ASSERT_OK(rocksStreamMetaStore_->AddNotifyPub(worekrAddr3, pubPbs[2]));
    std::vector<std::pair<std::string, NotifyPubPb>> pubs;
    DS_ASSERT_OK(rocksStreamMetaStore_->GetAllNotifyPub(pubs));
    ASSERT_EQ(pubs.size(), 5ul);
    DS_ASSERT_OK(rocksStreamMetaStore_->RemoveNotifyPub(worekrAddr1, pubPbs[0]));
    DS_ASSERT_OK(rocksStreamMetaStore_->GetAllNotifyPub(pubs));
    ASSERT_EQ(pubs.size(), 4ul);

    // test sub
    NotifyConsumerPb subPbs[createNum];
    std::unordered_map<std::string, ConsumerMetaPb> consumerMetas;
    MakeConsumerMetas(createNum, consumerMetas);
    int index = 0;
    for (auto &kv : consumerMetas) {
        *subPbs[index].mutable_consumer() = std::move(kv.second);
        index++;
    }

    DS_ASSERT_OK(rocksStreamMetaStore_->AddNotifySub(worekrAddr1, subPbs[0]));
    DS_ASSERT_OK(rocksStreamMetaStore_->AddNotifySub(worekrAddr2, subPbs[1]));
    DS_ASSERT_OK(rocksStreamMetaStore_->AddNotifySub(worekrAddr3, subPbs[0]));
    DS_ASSERT_OK(rocksStreamMetaStore_->AddNotifySub(worekrAddr3, subPbs[1]));
    DS_ASSERT_OK(rocksStreamMetaStore_->AddNotifySub(worekrAddr3, subPbs[2]));
    std::vector<std::pair<std::string, NotifyConsumerPb>> subs;
    DS_ASSERT_OK(rocksStreamMetaStore_->GetAllNotifySub(subs));
    ASSERT_EQ(subs.size(), 5ul);
    DS_ASSERT_OK(rocksStreamMetaStore_->RemoveNotifySub(worekrAddr1, subPbs[0]));
    DS_ASSERT_OK(rocksStreamMetaStore_->GetAllNotifySub(subs));
    ASSERT_EQ(subs.size(), 4ul);

    // test remove by worker
    DS_ASSERT_OK(rocksStreamMetaStore_->RemoveNotificationByWorker(worekrAddr3));
    DS_ASSERT_OK(rocksStreamMetaStore_->GetAllNotifyPub(pubs));
    DS_ASSERT_OK(rocksStreamMetaStore_->GetAllNotifySub(subs));
    ASSERT_EQ(pubs.size(), 1ul);
    ASSERT_EQ(subs.size(), 1ul);
}
}  // namespace ut
}  // namespace datasystem
