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
 * Description: Regression tests for KVClient Get-and-copy paths when the worker
 * runs with oc_metadata_header=false. With the metadata header disabled, the
 * shm buffer has no lock frame and Buffer::RLatch() returns K_NOT_SUPPORTED;
 * the SDK-internal CopyDataWithRLatch helper must still let user-facing
 * KVClient::Get() succeed and return the correct value.
 */

#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <vector>

#include "client/object_cache/oc_client_common.h"
#include "common.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/kv/read_only_buffer.h"
#include "datasystem/kv_client.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace st {

constexpr int64_t SHM_SIZE = 500 * 1024;  // match existing ST convention; well above payload-inline threshold

class KVCacheNoMetadataHeaderTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numOBS = 1;
        opts.numWorkers = 1;
        opts.enableDistributedMaster = "false";
        opts.numEtcd = 1;
        std::string hostIp = "127.0.0.1";
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        // Run the worker with oc_metadata_header disabled so every returned shm
        // Buffer carries a DisabledLock — the case the helper is meant to fix.
        opts.workerGflagParams = "-shared_memory_size_mb=25 -v=1 -oc_metadata_header=false";
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
    }

    void TearDown() override
    {
        client_.reset();
        ExternalClusterTest::TearDown();
    }

    std::shared_ptr<KVClient> client_;
};

// Single-key Get must succeed end-to-end on the shm return path even though
// Buffer::RLatch() would fail with K_NOT_SUPPORTED — the SDK helper must
// transparently fall back to a no-latch copy.
TEST_F(KVCacheNoMetadataHeaderTest, GetSingleKeyShmReturnsValue)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);

    const std::string key = "single-key";
    const std::string value(SHM_SIZE, 'a');
    DS_ASSERT_OK(client->Set(key, value));

    std::string got;
    DS_ASSERT_OK(client->Get(key, got));
    ASSERT_EQ(got.size(), value.size());
    ASSERT_EQ(got, value);
}

// Batch Get must also succeed and return every value — without the helper the
// first DisabledLock RLatch would abort the loop and surface K_NOT_SUPPORTED.
TEST_F(KVCacheNoMetadataHeaderTest, GetBatchShmReturnsAllValues)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);

    std::vector<std::string> keys = { "batch-a", "batch-b", "batch-c" };
    std::vector<std::string> values = {
        std::string(SHM_SIZE, 'a'),
        std::string(SHM_SIZE, 'b'),
        std::string(SHM_SIZE, 'c'),
    };
    for (size_t i = 0; i < keys.size(); ++i) {
        DS_ASSERT_OK(client->Set(keys[i], values[i]));
    }

    std::vector<std::string> got;
    DS_ASSERT_OK(client->Get(keys, got));
    ASSERT_EQ(got.size(), keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        ASSERT_EQ(got[i], values[i]) << "mismatch at index " << i;
    }
}

// Explicit Buffer::RLatch() must still report K_NOT_SUPPORTED — only the
// SDK-internal copy path is supposed to gracefully fall through. This protects
// the contract the public API exposes to users that opt in to shm latching.
TEST_F(KVCacheNoMetadataHeaderTest, ExplicitRLatchKeepsNotSupportedSemantics)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);

    const std::string key = "explicit-latch";
    const std::string value(SHM_SIZE, 'x');
    DS_ASSERT_OK(client->Set(key, value));

    Optional<ReadOnlyBuffer> buffer;
    DS_ASSERT_OK(client->Get(key, buffer));
    ASSERT_TRUE(static_cast<bool>(buffer));
    ASSERT_EQ(buffer->RLatch().GetCode(), StatusCode::K_NOT_SUPPORTED);
}

}  // namespace st
}  // namespace datasystem
