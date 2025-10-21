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
 * Description: Test WorkerOcServiceImpl.
 */

#include "datasystem/worker/object_cache/worker_oc_service_impl.h"

#include "common.h"
#include "datasystem/common/object_cache/safe_table.h"
#include "datasystem/protos/worker_object.pb.h"

using namespace datasystem::object_cache;

namespace datasystem {
namespace ut {
class WorkerOcServiceImplTest : public CommonTest {
public:
    void SetUp() override
    {
        CommonTest::SetUp();
        Init();
    }

    void Init()
    {
        objectTable_ = std::make_shared<object_cache::ObjectTable>();
        HostPort hostPort("127.0.0.1:18481");
        auto evictionManager = std::make_shared<WorkerOcEvictionManager>(objectTable_, hostPort, hostPort, nullptr);
        impl_ = std::make_shared<WorkerOCServiceImpl>(hostPort, hostPort, objectTable_, nullptr, evictionManager,
                                                      nullptr, nullptr, nullptr);
        impl_->InitServiceImpl();
    };

protected:
    std::shared_ptr<ObjectTable> objectTable_;
    std::shared_ptr<WorkerOCServiceImpl> impl_;
};

TEST_F(WorkerOcServiceImplTest, TestParallelClearData)
{
    std::vector<std::thread> threads;
    int threadCount = 5;
    int batchCount = 100;

    std::vector<std::string> objKeys{ "key1", "key2" };
    for (int i = 0; i < threadCount; i++) {
        threads.emplace_back([this, &objKeys, batchCount] {
            ClearDataReqPb req;
            for (int n = 0; n < batchCount; n++) {
                impl_->ClearObject(objKeys, req);
            }
        });
    }
    for (auto &t : threads) {
        t.join();
    }
    for (const auto &id : objKeys) {
        std::shared_ptr<SafeObjType> entry;
        auto rc = objectTable_->Get(id, entry);
        ASSERT_EQ(rc.GetCode(), K_NOT_FOUND);
    }
}

}  // namespace ut
}  // namespace datasystem