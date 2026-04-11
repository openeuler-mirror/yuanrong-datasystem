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
 * Description: SharedPageQueueGroup test
 */

#include <map>

#include "ut/common.h"
#include "datasystem/worker/stream_cache/client_worker_sc_service_impl.h"
#include "datasystem/worker/stream_cache/page_queue/shared_page_queue_group.h"
#include "datasystem/common/util/uuid_generator.h"

DS_DECLARE_uint32(sc_shared_page_group_count);

using namespace datasystem::worker::stream_cache;
namespace datasystem {
namespace ut {
class SharedPageQueueGroupTest : public CommonTest {
public:
    void SetUp() override
    {
        HostPort hostPort;
        DS_ASSERT_OK(hostPort.ParseString("127.0.0.1:9000"));
        const int pageGroupCount = 8;
        FLAGS_sc_shared_page_group_count = pageGroupCount;
        svc_ = std::make_shared<ClientWorkerSCServiceImpl>(hostPort, hostPort, nullptr, nullptr, nullptr);
        svc_->Init();
        pageQueueGroup_ = std::make_unique<SharedPageQueueGroup>(hostPort, nullptr, svc_.get());
        CommonTest::SetUp();
    }

    void TearDown() override
    {
        CommonTest::TearDown();
    }

protected:
    std::unique_ptr<SharedPageQueueGroup> pageQueueGroup_;
    std::shared_ptr<ClientWorkerSCServiceImpl> svc_;
};

TEST_F(SharedPageQueueGroupTest, TestPageQueueGroup)
{
    std::string tenantId1 = "tanant1";
    std::string streamName = "stream1";
    std::shared_ptr<SharedPageQueue> pageQueue1;
    pageQueueGroup_->GetOrCreateSharedPageQueue(tenantId1 + "$" + streamName, pageQueue1);
    std::shared_ptr<SharedPageQueue> pageQueue2;
    pageQueueGroup_->GetOrCreateSharedPageQueue(tenantId1 + "$" + streamName, pageQueue2);
    // using the same tenant and stream, will get the same page queue.
    ASSERT_EQ(pageQueue1, pageQueue2);
    std::shared_ptr<SharedPageQueue> pageQueue3;
    pageQueueGroup_->GetOrCreateSharedPageQueue(streamName, pageQueue2);
    // different tenant using different page queue.
    ASSERT_NE(pageQueue1, pageQueue3);

    // get page queue by tenant and stream.
    pageQueue2 = nullptr;
    DS_ASSERT_OK(pageQueueGroup_->GetSharedPageQueue(tenantId1 + "$" + streamName, pageQueue2));
    ASSERT_EQ(pageQueue1, pageQueue2);

    // using unknown tenant.
    pageQueue3 = nullptr;
    DS_ASSERT_NOT_OK(pageQueueGroup_->GetSharedPageQueue("unknown$" + streamName, pageQueue3));

    // remove page queue for tenant
    DS_ASSERT_OK(pageQueueGroup_->RemoveSharedPageQueueForTenant(tenantId1));
    // remove page queue for unknown tenant
    DS_ASSERT_NOT_OK(pageQueueGroup_->RemoveSharedPageQueueForTenant("unknown"));
    // get after delete.
    DS_ASSERT_NOT_OK(pageQueueGroup_->GetSharedPageQueue(tenantId1 + "$" + streamName, pageQueue2));
    DS_ASSERT_OK(pageQueueGroup_->GetSharedPageQueue(streamName, pageQueue2));

    DS_ASSERT_OK(pageQueue1->AfterAck());
    std::vector<DataElement> recvElements;
    DS_ASSERT_OK(pageQueue1->SendElements(nullptr, 0, 0, "127.0.0.1:9000", recvElements));
}

TEST_F(SharedPageQueueGroupTest, TestMaxPageQueue)
{
    std::string tenantId = "tanant1";
    std::set<SharedPageQueue *> pageQueueMap;
    int count = 200;
    for (int i = 0; i < count; i++) {
        std::shared_ptr<SharedPageQueue> pageQueue;
        std::string streamName = GetStringUuid();
        pageQueueGroup_->GetOrCreateSharedPageQueue(tenantId + "$" + streamName, pageQueue);
        pageQueueMap.insert(pageQueue.get());
    }

    ASSERT_LE(pageQueueMap.size(), FLAGS_sc_shared_page_group_count);
}
}  // namespace ut
}  // namespace datasystem
