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
 * Description: Test StreamPage StreamPageOwner classes.
 */

#include "common.h"
#include "datasystem/common/constants.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/common/stream_cache/cursor.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/stream/stream_config.h"
#include "datasystem/worker/stream_cache/producer.h"

namespace datasystem {
namespace ut {
constexpr size_t NUM_THREADS = 4;
constexpr uint64_t SHM_CAP = 64L * 1024L * 1024L;

class StreamCursorTest : public CommonTest {
protected:
    void SetUp() override;
    void TearDown() override;
    StreamCursorTest() : pool_(NUM_THREADS)
    {
    }
    ~StreamCursorTest() override = default;
    std::shared_ptr<ShmUnit> shmUnit_;
    std::shared_ptr<datasystem::Cursor> cursor_;
    ThreadPool pool_;
};

void StreamCursorTest::SetUp()
{
    FLAGS_v = datasystem::SC_INTERNAL_LOG_LEVEL;
    datasystem::memory::Allocator::Instance()->Init(SHM_CAP);
    shmUnit_ = std::make_shared<ShmUnit>();
    DS_ASSERT_OK(shmUnit_->AllocateMemory("CursorTest", Cursor::K_CURSOR_SIZE_V2, false));
    cursor_ = std::make_shared<datasystem::Cursor>(shmUnit_->GetPointer(), Cursor::K_CURSOR_SIZE_V2, 0);
    ASSERT_EQ(cursor_->Init(), Status::OK());
}

void StreamCursorTest::TearDown()
{
}

TEST_F(StreamCursorTest, TestShmViewAndFutexArea)
{
    cursor_->InitFutexArea();
    const int32_t val = K_OUT_OF_MEMORY;
    auto fut = pool_.Submit([this]() {
        ShmView view;
        Timer t(DEFAULT_TIMEOUT_MS);
        while (t.GetRemainingTimeMs() > 0) {
            RETURN_IF_NOT_OK(cursor_->GetLastPageView(view, DEFAULT_TIMEOUT_MS));
            if (view != ShmView()) {
                break;
            }
        }
        LOG(INFO) << "LastPage view: " << view.ToStr();
        CHECK_FAIL_RETURN_STATUS(view == shmUnit_->GetShmView(), K_RUNTIME_ERROR, "GetLastPageView error");
        size_t numWaiter;
        RETURN_IF_NOT_OK(cursor_->Wake(val, numWaiter));
        LOG(INFO) << "Wake up " << numWaiter << " waiters";
        return Status::OK();
    });
    // Send a ShmView
    DS_ASSERT_OK(cursor_->SetLastPage(shmUnit_->GetShmView(), DEFAULT_TIMEOUT_MS));
    // Get a reply
    int32_t fetchVal;
    DS_ASSERT_OK(cursor_->Wait(DEFAULT_TIMEOUT_MS, fetchVal));
    ASSERT_EQ(val, fetchVal);
    DS_ASSERT_OK(fut.get());
}

TEST_F(StreamCursorTest, TestForceClose)
{
    auto fut = pool_.Submit([this]() {
        Timer t(DEFAULT_TIMEOUT_MS);
        while (t.GetRemainingTimeMs() > 0) {
            if (cursor_->ForceClose()) {
                return Status::OK();
            }
        }
        RETURN_STATUS(K_RUNTIME_ERROR, "Force close error");
    });
    cursor_->SetForceClose();
    DS_ASSERT_OK(fut.get());
}

TEST_F(StreamCursorTest, TestSetCusorToProducer)
{
    auto producer = std::make_shared<datasystem::worker::stream_cache::Producer>("producerId", "StreamName", nullptr);
    auto copyCursor = cursor_;
    producer->SetCursor(std::move(copyCursor));

    auto fut = pool_.Submit([this]() {
        Timer t(DEFAULT_TIMEOUT_MS);
        while (t.GetRemainingTimeMs() > 0) {
            if (cursor_->ForceClose()) {
                return Status::OK();
            }
        }
        RETURN_STATUS(K_RUNTIME_ERROR, "Force close error");
    });
    producer->SetForceClose();
    DS_ASSERT_OK(fut.get());
}
}  // namespace ut
}  // namespace datasystem
