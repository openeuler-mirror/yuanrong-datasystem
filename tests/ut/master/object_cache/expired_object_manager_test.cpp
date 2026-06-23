/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: Test expired object manager class.
 */
#include "datasystem/master/object_cache/expired_object_manager.h"

#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "ut/common.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/common/inject/inject_point.h"

DS_DECLARE_string(rocksdb_store_dir);

using namespace datasystem::master;
namespace datasystem {
namespace ut {
class ExpiredObjectManagerTest : public CommonTest {};

TEST_F(ExpiredObjectManagerTest, TestParallelInsert)
{
    ExpiredObjectManager manager("127.0.0.1:10001", nullptr);
    const size_t objectCount = 10000;
    DS_ASSERT_OK(inject::Set("master.ExpiredObjectManager.Run", "call()"));
    manager.Init();
    std::vector<std::thread> threads;
    const int threadCount = 5;
    auto func = [&manager](int n) {
        for (size_t i = 0; i < objectCount; i++) {
            auto objectKey = "object-" + std::to_string(n) + "-" + std::to_string(i);
            (void)manager.InsertObject(objectKey, 0, 1);
            const size_t sleepPerCount = 100;
            if (i % sleepPerCount == 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                LOG(INFO) << "i:" << i;
            }
        }
    };
    for (int n = 0; n < threadCount; n++) {
        threads.emplace_back(func, n);
    }

    for (auto &t : threads) {
        t.join();
    }
}

TEST_F(ExpiredObjectManagerTest, TestGetExpiredObjectCrossShardFairness)
{
    ExpiredObjectManager manager("127.0.0.1:10001", nullptr);
    DS_ASSERT_OK(inject::Set("master.ExpiredObjectManager.Run", "call()"));
    manager.Init();

    // Generate keys that hash to specific shards. Shard 0 gets > MAX_DEL_BATCH_NUM (3000)
    // entries with later expiration; shard 63 gets a handful with earlier expiration.
    // The k-way merge must surface the earlier-expiring shard-63 entries even though
    // shard 0 has enough volume to fill the entire batch.
    static constexpr size_t kShardCount = 64;
    static constexpr size_t kTargetShardLow = 0;
    static constexpr size_t kTargetShardHigh = 63;
    static constexpr size_t kFillShardLow = 3100;
    static constexpr size_t kFillShardHigh = 20;

    std::vector<std::string> keysShardLow;
    std::vector<std::string> keysShardHigh;
    keysShardLow.reserve(kFillShardLow);
    keysShardHigh.reserve(kFillShardHigh);

    for (size_t i = 0; keysShardLow.size() < kFillShardLow || keysShardHigh.size() < kFillShardHigh; ++i) {
        std::string key = "fairness-" + std::to_string(i);
        size_t shardIdx = std::hash<std::string>{}(key) % kShardCount;
        if (shardIdx == kTargetShardLow && keysShardLow.size() < kFillShardLow) {
            keysShardLow.push_back(key);
        } else if (shardIdx == kTargetShardHigh && keysShardHigh.size() < kFillShardHigh) {
            keysShardHigh.push_back(key);
        }
    }

    const uint64_t laterExpireUs = 2000;
    const uint64_t earlierExpireUs = 100;
    for (const auto &key : keysShardLow) {
        (void)manager.InsertObject(key, laterExpireUs, 1);
    }
    for (const auto &key : keysShardHigh) {
        (void)manager.InsertObject(key, earlierExpireUs, 1);
    }

    auto expired = manager.GetExpiredObject();

    // Shard 63's earlier-expiring entries must be present — they expire first.
    for (const auto &key : keysShardHigh) {
        EXPECT_TRUE(expired.find(key) != expired.end())
            << "Key " << key << " from shard " << kTargetShardHigh << " should be in expired set";
    }
}
}  // namespace ut
}  // namespace datasystem
