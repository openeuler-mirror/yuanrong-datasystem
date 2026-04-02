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
 * Description: SharedPageQueue test
 */

#include <map>

#include "ut/common.h"
#include "../../../common/binmock/binmock.h"
#include "datasystem/common/constants.h"
#include "datasystem/stream/stream_config.h"
#include "datasystem/worker/stream_cache/client_worker_sc_service_impl.h"
#include "datasystem/common/stream_cache/stream_data_page.h"
#include "datasystem/common/stream_cache/stream_fields.h"
#include "datasystem/worker/stream_cache/page_queue/page_queue_handler.h"
#include "datasystem/worker/stream_cache/page_queue/shared_page_queue.h"
#include "datasystem/worker/stream_cache/remote_worker_manager.h"
#include "datasystem/common/util/uuid_generator.h"

DS_DECLARE_uint32(sc_shared_page_size_mb);

using namespace ::testing;
using namespace datasystem::worker::stream_cache;

namespace datasystem {
namespace ut {
constexpr int K_TWO = 2;
constexpr uint64_t SHM_CAP = 128L * 1024L * 1024L;
class SharedPageQueueTest : public CommonTest {
public:
    void SetUp() override
    {
        datasystem::memory::Allocator::Instance()->Init(SHM_CAP);
        allocate_ = std::make_shared<worker::stream_cache::WorkerSCAllocateMemory>(nullptr);
        HostPort hostPort;
        LOG_IF_ERROR(hostPort.ParseString("127.0.0.1:9000"), "ParseString failed");
        svc_ = std::make_shared<ClientWorkerSCServiceImpl>(hostPort, hostPort, nullptr, nullptr, allocate_);
        svc_->Init();
        CommonTest::SetUp();
    }
    void TearDown() override
    {
        CommonTest::TearDown();
    }

    std::shared_ptr<SharedPageQueue> CreateSharedPageQueue()
    {
        HostPort hostPort;
        LOG_IF_ERROR(hostPort.ParseString("127.0.0.1:9000"), "ParseString failed");
        FLAGS_sc_shared_page_size_mb = 1;
        return std::make_shared<SharedPageQueue>("tenant", std::move(hostPort), 0, allocate_, svc_.get());
    }

protected:
    std::shared_ptr<WorkerSCAllocateMemory> allocate_;
    std::shared_ptr<ClientWorkerSCServiceImpl> svc_;
};

TEST_F(SharedPageQueueTest, TestAllocateSharedPage)
{
    StreamFields streamFields;
    streamFields.streamMode_ = StreamMode::MPSC;
    PageQueueHandler handler(nullptr, Optional<StreamFields>(streamFields));
    auto pageQueue = CreateSharedPageQueue();
    handler.SetSharedPageQueue(pageQueue);
    // create cursor
    std::shared_ptr<Cursor> out;
    ShmView view;
    DS_ASSERT_OK(handler.AddCursor("id", true, out, view));
    // client will set esyCatcher to K_CURSOR_SIZE_V2
    out->SetClientVersion(Cursor::K_CURSOR_SIZE_V2);

    // allocate shared page.
    const int timeoutMs = 3000;
    ShmView preLastView;
    std::shared_ptr<StreamDataPage> lastPage;
    DS_ASSERT_OK(pageQueue->CreateOrGetLastDataPage(timeoutMs, preLastView, lastPage, false));
    ASSERT_TRUE(lastPage->IsSharedPage());
    preLastView = lastPage->GetShmView();
    std::shared_ptr<StreamDataPage> lastPage2;
    DS_ASSERT_OK(pageQueue->CreateOrGetLastDataPage(timeoutMs, preLastView, lastPage2, false));
    ASSERT_TRUE(lastPage2->IsSharedPage());
    ASSERT_EQ(lastPage, lastPage2);

    // create a client shm unit info.
    auto shmUnitInfo = lastPage->GetShmUnitInfo();
    shmUnitInfo->pointer = reinterpret_cast<uint8_t *>(shmUnitInfo->pointer) - shmUnitInfo->offset;
    int lockId = 12;
    auto clientPage = std::make_shared<StreamDataPage>(shmUnitInfo, lockId, true, false);
    DS_ASSERT_OK(clientPage->Init());
    ASSERT_TRUE(clientPage->IsSharedPage());

    int lockTimeout = 10000;  // 10s.
    // before lock, client will record page.
    out->SetLastLockedPage(clientPage->GetShmView(), lockTimeout);
    // lock page.
    StreamPageLock pageLock(clientPage);
    DS_ASSERT_OK(pageLock.Lock(lockTimeout));

    // unlock.
    pageQueue->TryUnlockByLockId(lockId);

    // lock again.
    StreamPageLock pageLock2(clientPage);
    DS_ASSERT_OK(pageLock2.Lock(lockTimeout));
}

TEST_F(SharedPageQueueTest, TestReadWrite)
{
    auto pageQueue = CreateSharedPageQueue();
    const int timeoutMs = 3000;
    const size_t eleSz1 = 102ul;
    const size_t eleSz2 = 1004ul;
    std::string data1(eleSz1, 'a');
    std::string data2(eleSz2, 'b');
    ShmView preLastView;
    std::shared_ptr<StreamDataPage> lastPage;
    DS_ASSERT_OK(pageQueue->CreateOrGetLastDataPage(timeoutMs, preLastView, lastPage, false));

    uint64_t streamNo1 = 111;
    uint64_t streamNo2 = 112;
    HeaderAndData element1((uint8_t *)data1.c_str(), data1.size(), streamNo1);
    HeaderAndData element2((uint8_t *)data2.c_str(), data2.size(), streamNo2);
    InsertFlags flags = InsertFlags::NONE;
    DS_ASSERT_OK(lastPage->Insert(element1, timeoutMs, flags));
    DS_ASSERT_OK(lastPage->Insert(element2, timeoutMs, flags));

    std::vector<DataElement> out;
    DS_ASSERT_OK(lastPage->Receive(0, timeoutMs, out));
    ASSERT_EQ(out.size(), 2ul);
    std::string r1((char *)out[0].ptr, out[0].size);
    std::string r2((char *)out[1].ptr, out[1].size);
    ASSERT_EQ(data1, r1);
    ASSERT_EQ(out[0].streamNo_, streamNo1);
    ASSERT_EQ(data2, r2);
    ASSERT_EQ(out[1].streamNo_, streamNo2);
}

TEST_F(SharedPageQueueTest, TestWriteMaxSize)
{
    auto pageQueue = CreateSharedPageQueue();
    const int timeoutMs = 3000;
    InsertFlags flags = InsertFlags::NONE;
    ShmView preLastView;
    std::shared_ptr<StreamDataPage> lastPage;
    DS_ASSERT_OK(pageQueue->CreateOrGetLastDataPage(timeoutMs, preLastView, lastPage, false));

    const size_t elementMaxSize = FLAGS_sc_shared_page_size_mb * MB_TO_BYTES - StreamDataPage::PageOverhead(true);
    std::string data1(elementMaxSize + 1, 'a');
    uint64_t streamNo1 = 111;
    HeaderAndData element1((uint8_t *)data1.c_str(), data1.size(), streamNo1);
    DS_ASSERT_NOT_OK(lastPage->Insert(element1, timeoutMs, flags));

    std::string data2(elementMaxSize, 'a');
    uint64_t streamNo2 = 112;
    HeaderAndData element2((uint8_t *)data2.c_str(), data2.size(), streamNo2);
    DS_ASSERT_OK(lastPage->Insert(element2, timeoutMs, flags));

    std::vector<DataElement> out;
    DS_ASSERT_OK(lastPage->Receive(0, timeoutMs, out));
    ASSERT_EQ(out.size(), 1ul);
    std::string r2((char *)out[0].ptr, out[0].size);
    ASSERT_EQ(data2, r2);
    ASSERT_EQ(out[0].streamNo_, streamNo2);
}

TEST_F(SharedPageQueueTest, TestWriteFull)
{
    auto pageQueue = CreateSharedPageQueue();
    const int timeoutMs = 3000;
    InsertFlags flags = InsertFlags::NONE;
    ShmView preLastView;
    std::shared_ptr<StreamDataPage> lastPage;
    DS_ASSERT_OK(pageQueue->CreateOrGetLastDataPage(timeoutMs, preLastView, lastPage, false));

    size_t remainingSize = lastPage->PagePayloadSize();
    size_t count = 30;
    size_t perSize = remainingSize % count;
    uint64_t streamNo = 111;
    size_t metaSize = StreamDataPage::GetMetaSize(true);
    while (remainingSize > StreamDataPage::GetMetaSize(true)) {
        size_t dataWithMetaSize = std::max(perSize, remainingSize);
        size_t dataSize = dataWithMetaSize - metaSize;
        remainingSize -= dataWithMetaSize;
        std::string data(dataSize, 'a');
        HeaderAndData element((uint8_t *)data.c_str(), data.size(), streamNo);
        DS_ASSERT_OK(lastPage->Insert(element, timeoutMs, flags));
    }
    ASSERT_EQ(lastPage->GetFreeSpaceSize(), remainingSize);

    std::string data(1, 'a');
    uint64_t streamNo1 = 111;
    HeaderAndData element((uint8_t *)data.c_str(), data.size(), streamNo1);
    DS_ASSERT_NOT_OK(lastPage->Insert(element, timeoutMs, flags));

    std::vector<DataElement> out;
    DS_ASSERT_OK(lastPage->Receive(0, timeoutMs, out));
    size_t recvSize = 0;
    for (const auto &e : out) {
        ASSERT_EQ(e.streamNo_, streamNo);
        recvSize += e.size + metaSize;
    }
    ASSERT_EQ(recvSize + remainingSize, lastPage->PagePayloadSize());
}

TEST_F(SharedPageQueueTest, TestAllocMemeoryAndRelease)
{
    BINEXPECT_CALL(&PageQueueHandler::CreateExclusivePageQueue, (_, _)).Times(1).WillOnce(Return(nullptr));
    PageQueueHandler handler(nullptr, Optional<StreamFields>());
    auto pageQueue = CreateSharedPageQueue();
    handler.SetSharedPageQueue(pageQueue);
    ASSERT_EQ(handler.GetPageSize(), FLAGS_sc_shared_page_size_mb * MB_TO_BYTES);
    size_t pageSize = 1024 * 1024 * 5;
    std::shared_ptr<ShmUnitInfo> pageUnitInfo;
    DS_ASSERT_OK(handler.AllocMemory(pageSize, true, pageUnitInfo, false));
    handler.DumpPoolPages(1);
    ShmView shmView{
        .fd = pageUnitInfo->fd, .mmapSz = pageUnitInfo->mmapSize, .off = pageUnitInfo->offset, .sz = pageUnitInfo->size
    };
    DS_ASSERT_OK(handler.ReleaseMemory(shmView));
    handler.DumpPoolPages(1);
}

TEST_F(SharedPageQueueTest, TestRemoteAck)
{
    auto pageQueue = CreateSharedPageQueue();
    const int timeoutMs = 3000;
    const size_t eleSz1 = 102ul;
    const size_t eleSz2 = 1004ul;
    std::string data1(eleSz1, 'a');
    std::string data2(eleSz2, 'b');
    ShmView preLastView;
    std::shared_ptr<StreamDataPage> lastPage;
    DS_ASSERT_OK(pageQueue->CreateOrGetLastDataPage(timeoutMs, preLastView, lastPage, false));

    uint64_t streamNo1 = 111;
    uint64_t streamNo2 = 112;
    HeaderAndData element1((uint8_t *)data1.c_str(), data1.size(), streamNo1);
    HeaderAndData element2((uint8_t *)data2.c_str(), data2.size(), streamNo2);
    InsertFlags flags = InsertFlags::NONE;
    DS_ASSERT_OK(lastPage->Insert(element1, timeoutMs, flags));
    DS_ASSERT_OK(lastPage->Insert(element2, timeoutMs, flags));

    std::vector<DataElement> out;
    DS_ASSERT_OK(lastPage->Receive(0, timeoutMs, out));
    ASSERT_EQ(out.size(), 2ul);
    std::string r1((char *)out[0].ptr, out[0].size);
    std::string r2((char *)out[1].ptr, out[1].size);
    ASSERT_EQ(data1, r1);
    ASSERT_EQ(out[0].streamNo_, streamNo1);
    ASSERT_EQ(data2, r2);
    ASSERT_EQ(out[1].streamNo_, streamNo2);

    BINEXPECT_CALL(&RemoteWorkerManager::GetLastAckCursor, (_)).WillRepeatedly(Return(K_TWO));
    DS_ASSERT_OK(pageQueue->RemoteAck());
}
}  // namespace ut
}  // namespace datasystem
