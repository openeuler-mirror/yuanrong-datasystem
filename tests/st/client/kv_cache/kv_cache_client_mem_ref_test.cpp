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
 * Description: shared memory ref testcases.
 */
#include <gtest/gtest.h>
#include <tbb/concurrent_hash_map.h>
#include <unistd.h>
#include <memory>
#include <string>
#include <vector>

#include "client/object_cache/oc_client_common.h"
#include "common.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/rpc/mem_view.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/kv/read_only_buffer.h"
#include "datasystem/kv_client.h"
#include "datasystem/utils/optional.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace st {
class KVCacheClientMemRefTest : public OCClientCommon {
public:
    std::vector<std::string> workerAddress_;

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 1;
        opts.enableDistributedMaster = "true";
        opts.numEtcd = 1;
        std::string hostIp = "127.0.0.1";
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        for (const auto &addr : opts.workerConfigs) {
            workerAddress_.emplace_back(addr.ToString());
        }
        opts.workerGflagParams =
            "-shared_memory_size_mb=128 -v=1 -log_monitor=true -max_client_num=2000 -arena_per_tenant=1";
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "ShmUnit.FreeMemory", "call()"));
        DS_ASSERT_OK(inject::Set("client.DecreaseReferenceCnt", "call(0)"));
        InitClients();
    }

    void TearDown() override
    {
        client_.reset();
        ExternalClusterTest::TearDown();
    }

    void InitClients()
    {
        InitTestKVClient(0, client_);
    }

    static Status BufferToString(ReadOnlyBuffer &buffer, std::string &value)
    {
        RETURN_IF_NOT_OK(buffer.RLatch());
        value = std::string(reinterpret_cast<const char *>(buffer.ImmutableData()), buffer.GetSize());
        return buffer.UnRLatch();
    }

    static MemView BufferToMemView(ReadOnlyBuffer &buffer)
    {
        return MemView(buffer.ImmutableData(), buffer.GetSize());
    }

    static std::string MemViewToString(const MemView &view)
    {
        return std::string(reinterpret_cast<const char *>(view.Data()), view.Size());
    }

    static bool IsZeroInit(const MemView &view)
    {
        for (size_t i = 0; i < view.Size(); ++i) {
            if (reinterpret_cast<const uint8_t *>(view.Data())[i] != 0) {
                return false;
            }
        }
        return true;
    }

    std::shared_ptr<KVClient> client_;
};

TEST_F(KVCacheClientMemRefTest, TestMultiGet)
{
    auto func = [this](const std::string &key) {
        std::shared_ptr<KVClient> client;
        InitTestKVClient(0, client);
        std::string value = "value";
        DS_ASSERT_OK(client->Set(key, value));

        Optional<ReadOnlyBuffer> buffer1;
        Optional<ReadOnlyBuffer> buffer2;
        DS_ASSERT_OK(client->Get(key, buffer1));
        DS_ASSERT_OK(client->Get(key, buffer2));

        ASSERT_TRUE(buffer1);
        ASSERT_TRUE(buffer2);
        auto mv1 = BufferToMemView(*buffer1);
        auto mv2 = BufferToMemView(*buffer2);
        ASSERT_EQ(value, MemViewToString(mv1));
        ASSERT_EQ(value, MemViewToString(mv2));

        DS_ASSERT_OK(client->Set(key, "new"));

        ASSERT_EQ(value, MemViewToString(mv1));
        ASSERT_EQ(value, MemViewToString(mv2));
        buffer1 = Optional<ReadOnlyBuffer>();
        ASSERT_EQ(value, MemViewToString(mv1));
        ASSERT_EQ(value, MemViewToString(mv2));
        buffer2 = Optional<ReadOnlyBuffer>();
        ASSERT_TRUE(IsZeroInit(mv1)) << "unexpect value:" << MemViewToString(mv1);
        ASSERT_TRUE(IsZeroInit(mv2)) << "unexpect value:" << MemViewToString(mv2);
    };
    func("key1");
    DS_ASSERT_OK(inject::Set("client.RegisterClient.multi_shm_ref_count", "call(0)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.RegisterClient.multi_shm_ref_count", "call(1)"));
    func("key2");
    DS_ASSERT_OK(inject::Set("client.RegisterClient.multi_shm_ref_count", "call(1)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.RegisterClient.multi_shm_ref_count", "call(0)"));
    func("key3");
}

TEST_F(KVCacheClientMemRefTest, TestClientDisconnect)
{
    auto func = [this](const std::string &key) {
        std::shared_ptr<KVClient> client1;
        InitTestKVClient(0, client1);
        std::string value = "value";
        DS_ASSERT_OK(client1->Set(key, value));

        Optional<ReadOnlyBuffer> buffer1;
        Optional<ReadOnlyBuffer> buffer2;
        DS_ASSERT_OK(client1->Get(key, buffer1));
        DS_ASSERT_OK(client1->Get(key, buffer2));

        ASSERT_TRUE(buffer1);
        ASSERT_TRUE(buffer2);

        DS_ASSERT_OK(client1->Del(key));
        auto mv1 = BufferToMemView(*buffer1);
        auto mv2 = BufferToMemView(*buffer2);
        ASSERT_EQ(value, MemViewToString(mv1));
        ASSERT_EQ(value, MemViewToString(mv2));

        DS_ASSERT_OK(client1->ShutDown());

        ASSERT_TRUE(IsZeroInit(mv1)) << "unexpect value:" << MemViewToString(mv1);
        ASSERT_TRUE(IsZeroInit(mv2)) << "unexpect value:" << MemViewToString(mv2);
    };
    func("key1");
    DS_ASSERT_OK(inject::Set("client.RegisterClient.multi_shm_ref_count", "call(0)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.RegisterClient.multi_shm_ref_count", "call(1)"));
    func("key2");
    DS_ASSERT_OK(inject::Set("client.RegisterClient.multi_shm_ref_count", "call(1)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.RegisterClient.multi_shm_ref_count", "call(0)"));
    func("key3");
}

TEST_F(KVCacheClientMemRefTest, TestParallelGetAndSet)
{
    const int getThreadCount = 2;
    const int loopCount = 1000;
    std::vector<std::thread> threads;
    std::string key = "key1";
    std::string value = "value";
    DS_ASSERT_OK(client_->Set(key, value));
    threads.reserve(getThreadCount);
    for (int i = 0; i < getThreadCount; i++) {
        threads.emplace_back([this, &key, &value] {
            for (int n = 0; n < loopCount; n++) {
                Optional<ReadOnlyBuffer> buffer;
                DS_ASSERT_OK(client_->Get(key, buffer));
                std::string value1;
                ASSERT_TRUE(buffer);
                DS_ASSERT_OK(BufferToString(*buffer, value1));
                ASSERT_EQ(value, value1);
            }
        });
    }
    threads.emplace_back([this, &key, &value] {
        for (int n = 0; n < loopCount; n++) {
            DS_ASSERT_OK(client_->Set(key, value));
        }
    });

    for (auto &t : threads) {
        t.join();
    }
}

}  // namespace st
}  // namespace datasystem
