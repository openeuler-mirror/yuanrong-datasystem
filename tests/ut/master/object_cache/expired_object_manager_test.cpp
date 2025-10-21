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

#include "common.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/common/inject/inject_point.h"

DS_DECLARE_string(backend_store_dir);

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
}  // namespace ut
}  // namespace datasystem
