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
 * Description: UT Test for oc_metadata_header feature
 */

#include <sstream>
#include <atomic>
#include <thread>
#include <vector>

#define private public
#include "datasystem/worker/object_cache/worker_oc_service_impl.h"
#undef private

#include "ut/common.h"
#include "datasystem/common/object_cache/lock.h"
#include "datasystem/common/object_cache/safe_table.h"
#include "datasystem/protos/worker_object.pb.h"

using namespace datasystem::object_cache;

DS_DECLARE_bool(oc_metadata_header);

namespace datasystem {
namespace ut {

TEST(DisabledLockTest, WLatchReturnsNotSupported)
{
    auto lock = std::make_shared<object_cache::DisabledLock>();
    ASSERT_EQ(lock->Init().GetCode(), StatusCode::K_OK);
    ASSERT_EQ(lock->WLatch(1).GetCode(), StatusCode::K_NOT_SUPPORTED);
    ASSERT_EQ(lock->RLatch(1).GetCode(), StatusCode::K_NOT_SUPPORTED);
    ASSERT_FALSE(lock->TryRLatch());
}

TEST(DisabledLockTest, IsSupportedFalse)
{
    // DisabledLock advertises IsSupported() == false so that SDK-internal
    // Get-and-copy helpers know to skip latch acquisition, while public
    // Buffer::RLatch() callers still receive K_NOT_SUPPORTED.
    object_cache::DisabledLock disabled;
    ASSERT_FALSE(disabled.IsSupported());

    // CommonLock (non-shm path) keeps the default true so the helper still
    // takes the latch on non-shm reads.
    object_cache::CommonLock common;
    ASSERT_TRUE(common.IsSupported());
}

class WorkerOcMetadataHeaderUTTest : public CommonTest {
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
        auto evictionManager = std::make_shared<WorkerOcEvictionManager>(
            objectTable_, hostPort, hostPort, nullptr);
        impl_ = std::make_shared<WorkerOCServiceImpl>(
            hostPort, hostPort, objectTable_, nullptr, evictionManager,
            nullptr, nullptr, nullptr);
        impl_->InitMetaSize();
        impl_->InitServiceImpl();
    }

protected:
    std::shared_ptr<ObjectTable> objectTable_;
    std::shared_ptr<WorkerOCServiceImpl> impl_;
};

TEST_F(WorkerOcMetadataHeaderUTTest, TestMetadataHeaderEnabled)
{
    FLAGS_oc_metadata_header = true;
    Init();
    ASSERT_NE(impl_, nullptr);
    ASSERT_GT(impl_->GetMetadataSize(), 0);
}

TEST_F(WorkerOcMetadataHeaderUTTest, TestMetadataHeaderDisabled)
{
    FLAGS_oc_metadata_header = false;
    Init();
    ASSERT_NE(impl_, nullptr);
    ASSERT_EQ(impl_->GetMetadataSize(), 0);
}

TEST_F(WorkerOcMetadataHeaderUTTest, TestMetadataHeaderSwitch)
{
    FLAGS_oc_metadata_header = false;
    Init();
    ASSERT_NE(impl_, nullptr);
    ASSERT_EQ(impl_->GetMetadataSize(), 0);

    FLAGS_oc_metadata_header = true;
    Init();
    ASSERT_NE(impl_, nullptr);
    ASSERT_GT(impl_->GetMetadataSize(), 0);
}

TEST_F(WorkerOcMetadataHeaderUTTest, TestRepeatedInitWithoutHeader)
{
    FLAGS_oc_metadata_header = false;

    for (int i = 0; i < 10; i++) {
        Init();
        ASSERT_NE(impl_, nullptr);
        ASSERT_NE(objectTable_, nullptr);
        ASSERT_EQ(impl_->GetMetadataSize(), 0);
    }
}

TEST_F(WorkerOcMetadataHeaderUTTest, TestRepeatedInitWithHeader)
{
    FLAGS_oc_metadata_header = true;

    for (int i = 0; i < 10; i++) {
        Init();
        ASSERT_NE(impl_, nullptr);
        ASSERT_NE(objectTable_, nullptr);
        ASSERT_GT(impl_->GetMetadataSize(), 0);
    }
}

TEST_F(WorkerOcMetadataHeaderUTTest, TestRefreshMetaWithoutHeader)
{
    FLAGS_oc_metadata_header = false;
    Init();

    ClientKey clientKey;
    auto status = impl_->RefreshMeta(clientKey);
    ASSERT_NE(status.GetCode(), StatusCode::K_RUNTIME_ERROR);
}

TEST_F(WorkerOcMetadataHeaderUTTest, TestConcurrentGetMetadataSize)
{
    FLAGS_oc_metadata_header = false;
    Init();

    std::vector<std::thread> threads;
    std::atomic<int> completedThreads{0};

    for (int i = 0; i < 20; i++) {
        threads.emplace_back([this, &completedThreads]() {
            for (int j = 0; j < 50; j++) {
                ASSERT_EQ(impl_->GetMetadataSize(), 0);
            }
            completedThreads++;
        });
    }

    for (auto &t : threads) {
        t.join();
    }

    ASSERT_EQ(completedThreads.load(), 20);
}

}  // namespace ut
}  // namespace datasystem