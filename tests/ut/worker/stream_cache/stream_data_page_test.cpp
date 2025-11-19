/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: Test StreamPage StreamPageOwner classes.
 */

#include "common.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/common/stream_cache/stream_data_page.h"
#include "datasystem/stream/stream_config.h"
#include "datasystem/worker/stream_cache/buffer_pool.h"

namespace datasystem {
namespace ut {
using namespace datasystem::worker::stream_cache;
constexpr static int FOUR_K = 4096;
constexpr uint64_t SHM_CAP = 128L * 1024L * 1024L;

class StreamDataPageTest : public CommonTest {
protected:
    void SetUp() override;
    void TearDown() override;

    RandomData randomData_;
    std::shared_ptr<ShmUnit> pageUnit_;
    std::shared_ptr<datasystem::StreamDataPage> page_;
    static bool VerifyElement(const std::string &expected, const Element &ele);
    std::string GetRandomString();
    Status InsertUntilPageFull(std::shared_ptr<ShmUnit> &pageUnit, uint32_t lockId, bool isClient,
                               std::vector<std::string> &out);
    Status ReceiveUntilTimeout(std::shared_ptr<ShmUnit> &pageUnit, uint64_t &lastRecvCursor, uint32_t lockId,
                               uint64_t timeoutMs, std::vector<Element> &out);
    static std::string ShmUnitInfoToStr(std::shared_ptr<ShmUnitInfo> &shm)
    {
        ShmView v = { .fd = shm->fd, .mmapSz = shm->mmapSize, .off = shm->offset, .sz = shm->size };
        return v.ToStr();
    }
};

void StreamDataPageTest::SetUp()
{
    FLAGS_v = datasystem::SC_INTERNAL_LOG_LEVEL;
    datasystem::memory::Allocator::Instance()->Init(SHM_CAP);
    pageUnit_ = std::make_shared<ShmUnit>();
    DS_ASSERT_OK(pageUnit_->AllocateMemory("", FOUR_K + StreamDataPage::PageOverhead(), false));
    page_ = std::make_shared<datasystem::StreamDataPage>(pageUnit_, 0, false);
    ASSERT_EQ(page_->Init(), Status::OK());
}

void StreamDataPageTest::TearDown()
{
}

bool StreamDataPageTest::VerifyElement(const std::string &expected, const Element &ele)
{
    std::string str(reinterpret_cast<const char *>(ele.ptr), ele.size);
    return expected == str;
}

std::string StreamDataPageTest::GetRandomString()
{
    const int maxLen = 20;
    auto str = randomData_.GetRandomString((randomData_.GetRandomUint8() % maxLen) + 1);
    return str;
}

Status StreamDataPageTest::InsertUntilPageFull(std::shared_ptr<ShmUnit> &pageUnit, uint32_t lockId, bool isClient,
                                               std::vector<std::string> &out)
{
    auto page = std::make_shared<datasystem::StreamDataPage>(pageUnit, lockId, isClient);
    RETURN_IF_NOT_OK(page->Init());
    Status rc;
    while (rc.IsOk()) {
        auto str = GetRandomString();
        HeaderAndData ele(reinterpret_cast<uint8_t *>(str.data()), str.length(), 0);
        auto flag = InsertFlags::NONE;
        rc = page->Insert(ele, 0, flag);
        if (rc.GetCode() == K_NO_SPACE) {
            rc = Status::OK();
            break;
        }
        if (rc.GetCode() == K_TRY_AGAIN) {
            rc = Status::OK();
            continue;
        }
        RETURN_IF_NOT_OK(rc);
        out.emplace_back(std::move(str));
    }
    return rc;
}

Status StreamDataPageTest::ReceiveUntilTimeout(std::shared_ptr<ShmUnit> &pageUnit, uint64_t &lastRecvCursor,
                                               uint32_t lockId, uint64_t timeoutMs, std::vector<Element> &out)
{
    auto page = std::make_shared<datasystem::StreamDataPage>(pageUnit, lockId, true);
    RETURN_IF_NOT_OK(page->Init());
    Status rc;
    while (rc.IsOk()) {
        std::vector<DataElement> elements;
        rc = page->Receive(lastRecvCursor, timeoutMs, elements);
        if (rc.GetCode() == K_TRY_AGAIN) {
            rc = Status::OK();
            break;
        }
        RETURN_IF_NOT_OK(rc);
        LOG(INFO) << "Receive " << elements.size() << " elements";
        lastRecvCursor += elements.size();
        out.insert(out.end(), elements.begin(), elements.end());
    }
    return rc;
}

TEST_F(StreamDataPageTest, TestCreateDataPageSuccess)
{
    ASSERT_EQ(page_->InitEmptyPage(), Status::OK());
}

TEST_F(StreamDataPageTest, TestCreateDataPageFail)
{
    const int NotBigEnough = 48;
    auto pageUnit = std::make_shared<ShmUnit>();
    DS_ASSERT_OK(pageUnit->AllocateMemory("", NotBigEnough, false));
    auto page = std::make_shared<datasystem::StreamDataPage>(pageUnit, 0, false);
    Status rc = page->Init();
    LOG(INFO) << rc.ToString();
    ASSERT_NE(rc, Status::OK());
}

TEST_F(StreamDataPageTest, TestMultiElementsRW)
{
    ASSERT_EQ(page_->InitEmptyPage(), Status::OK());
    std::vector<std::string> strs;
    Status rc = InsertUntilPageFull(pageUnit_, 0, false, strs);
    ASSERT_EQ(rc, Status::OK());
    ASSERT_GT(strs.size(), 0u);
    LOG(INFO) << FormatString("Number of elements inserted %zu", strs.size());
    // Receive all the elements on the page starting from cursor 1.
    std::vector<DataElement> v;
    rc = page_->Receive(0, 0, v);
    ASSERT_EQ(rc, Status::OK());
    ASSERT_EQ(strs.size(), v.size());
    // Compare what we insert
    for (size_t i = 0; i < strs.size(); ++i) {
        ASSERT_TRUE(VerifyElement(strs[i], v[i]));
    }
}

TEST_F(StreamDataPageTest, TestMultiElementsSPSC)
{
    // Two threads. One simulate producer and one simulate consumer.
    const int poolSz = 2;
    auto pool = std::make_unique<datasystem::ThreadPool>(poolSz);
    ASSERT_EQ(page_->InitEmptyPage(), Status::OK());
    std::vector<std::string> strs;
    // Producer simulation
    auto res1 = pool->Submit([this, &strs]() {
        // Pick lockID 1
        DS_ASSERT_OK(InsertUntilPageFull(pageUnit_, 1, true, strs));
        ASSERT_GT(strs.size(), 0u);
        LOG(INFO) << FormatString("Number of elements inserted %zu", strs.size());
    });
    // Consumer simulation
    std::vector<Element> v;
    uint64_t lastRecvCursor = 0;
    auto res2 = pool->Submit([this, &v, &lastRecvCursor]() {
        // Pick lockID 2
        // We will pick a long timeout value (5s), keep receiving until we time out
        const uint64_t timeoutMs = 5 * 1000;
        DS_ASSERT_OK(ReceiveUntilTimeout(pageUnit_, lastRecvCursor, 2, timeoutMs, v));
    });
    // Wait for both threads to come back.
    res1.get();
    res2.get();
    ASSERT_EQ(strs.size(), v.size());
    // Compare what we insert
    for (size_t i = 0; i < strs.size(); ++i) {
        ASSERT_TRUE(VerifyElement(strs[i], v[i]));
    }
}

TEST_F(StreamDataPageTest, TestMultiElementsMPSC)
{
    // A few number of producers and 1 consumer
    const int numProducers = 2;
    const int numConsumers = 1;
    auto pool = std::make_unique<datasystem::ThreadPool>(numProducers + numConsumers);
    ASSERT_EQ(page_->InitEmptyPage(), Status::OK());
    std::atomic<uint32_t> lockId(1);
    // Producer simulation
    std::vector<std::future<void>> producerRes;
    std::vector<std::vector<std::string>> strs(numProducers);
    for (auto i = 0; i < numProducers; ++i) {
        producerRes.emplace_back(pool->Submit([this, &lockId, &strs, i]() {
            DS_ASSERT_OK(InsertUntilPageFull(pageUnit_, lockId.fetch_add(1), true, strs[i]));
            ASSERT_GT(strs[i].size(), 0u);
            LOG(INFO) << FormatString("Number of elements inserted %zu", strs[i].size());
        }));
    }
    // Consumer simulation
    std::vector<Element> v;
    uint64_t lastRecvCursor = 0;
    auto res2 = pool->Submit([this, &v, &lastRecvCursor, &lockId]() {
        // We will pick a long timeout value (5s), keep receiving until we time out
        const uint64_t timeoutMs = 5 * 1000;
        DS_ASSERT_OK(ReceiveUntilTimeout(pageUnit_, lastRecvCursor, lockId.fetch_add(1), timeoutMs, v));
    });
    for (auto &res : producerRes) {
        res.get();
    }
    res2.get();
    LOG(INFO) << FormatString("Consumer receives %zu elements", v.size());
    size_t totalElements = 0;
    for (auto &str : strs) {
        totalElements += str.size();
    }
    ASSERT_EQ(totalElements, v.size());
}

TEST_F(StreamDataPageTest, LEVEL2_TestMillionInsert)
{
    PerfManager *perfManager = PerfManager::Instance();
    FLAGS_v = 0;
    const size_t pageSize = 1048576ul;
    const size_t eleSz = 1024ul;
    std::string a(eleSz, 'a');
    const size_t numElements = 1'280'000ul;
    auto pageUnit = std::make_shared<ShmUnit>();
    DS_ASSERT_OK(pageUnit->AllocateMemory("", pageSize + StreamDataPage::PageOverhead(), false));
    auto page = std::make_shared<datasystem::StreamDataPage>(pageUnit, 0, false);
    DS_ASSERT_OK(page->Init());
    DS_ASSERT_OK(page->InitEmptyPage());
    Status rc;
    HeaderAndData ele((uint8_t *)a.c_str(), a.size(), 0);
    Timer t;
    size_t numPagesNeeded = 1;
    auto flags = InsertFlags::NONE;
    for (size_t i = 0; i < numElements; ++i) {
        rc = page->Insert(ele, 0, flags);
        if (rc.GetCode() == K_NO_SPACE) {
            DS_ASSERT_OK(page->InitEmptyPage());
            ++numPagesNeeded;
            DS_ASSERT_OK(page->Insert(ele, 0, flags));
            continue;
        }
        DS_ASSERT_OK(rc);
    }
    LOG(INFO) << FormatString("Elapsed time [%.6lf]s. Total pages %zu", t.ElapsedSecond(), numPagesNeeded);
    if (perfManager != nullptr) {
        perfManager->Tick();
        perfManager->PrintPerfLog();
    }
}
}  // namespace ut
}  // namespace datasystem
