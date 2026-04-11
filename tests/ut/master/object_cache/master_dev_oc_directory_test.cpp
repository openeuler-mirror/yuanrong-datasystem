/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: Test master device oc directory.
 */

#include <cstddef>
#include <string>
#include "ut/common.h"

#include "datasystem/common/util/random_data.h"
#include "datasystem/master/object_cache/device/master_dev_oc_directory.h"

using namespace datasystem::master;

namespace datasystem {
namespace ut {

class MasterDevOcDirectoryTest : public CommonTest {
protected:
    ObjectGetP2PMetaReqSubscriptionTable getP2PMetaTable_;
};

TEST_F(MasterDevOcDirectoryTest, ConcurrentAddAndGetAll)
{
    const int threadNum = 5;
    const int iterations = 1000;
    std::vector<std::string> keys;
    keys.reserve(iterations);
    for (int i = 0; i < iterations; i++) {
        keys.emplace_back("key" + std::to_string(i));
    }

    std::atomic<bool> keepRunning{true};
    auto addFunc = [&]() {
        while (keepRunning) {
            for (const auto &key : keys) {
                GetP2PMetaReqPb req;
                auto request = std::make_shared<GetP2PMetaRequest>(std::vector<std::string>{ key }, nullptr,
                                                                   ClientKey::Intern("client1"), 0, req);
                getP2PMetaTable_.AddGetP2PMetaRequest(key, request);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10)); // sleep 10 ms
        }
    };

    auto getAllFunc = [&]() {
        while (keepRunning) {
            for (const auto &key : keys) {
                (void)getP2PMetaTable_.GetAllP2PMetaRequest(key);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10)); // sleep 10 ms
        }
    };
    ThreadPool threadPools(threadNum << 1);
    std::vector<std::future<void>> futs;
    for (int i = 0; i < threadNum; i++) {
        futs.emplace_back(threadPools.Submit(addFunc));
        futs.emplace_back(threadPools.Submit(getAllFunc));
    }
    std::this_thread::sleep_for(std::chrono::seconds(3)); // run 3 seconds
    keepRunning = false;
    for (auto &fut : futs) {
        fut.get();
    }
}

}  // namespace ut
}  // namespace datasystem