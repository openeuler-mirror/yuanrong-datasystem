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

#include "datasystem/common/urma_mock/objects/mock_jfc.h"

#include <sys/eventfd.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>

#include "datasystem/common/urma_mock/inject/fault_inject.h"
#include "datasystem/common/urma_mock/urma_mock_backend.h"  // MockCr

namespace datasystem {
namespace urma_mock {
MockJfc::MockJfc(uint64_t id, const std::shared_ptr<MockContext> &ctx) : id_(id), ctx_(ctx)
{
}

MockJfc::~MockJfc()
{
    int fd = -1;
    {
        std::lock_guard<std::mutex> lk(mu_);
        fd = eventFd_;
        eventFd_ = -1;
    }
    if (fd >= 0) {
        ::close(fd);
    }
}

uint64_t MockJfc::GetId() const
{
    return id_;
}

std::shared_ptr<MockContext> MockJfc::LockContext() const
{
    return ctx_.lock();
}

void MockJfc::PushCr(const MockCr &cr)
{
    auto pushed = cr;
    auto injectedStatus = GetMockInjectCrStatus();
    if (injectedStatus != URMA_CR_SUCCESS) {
        pushed.status = static_cast<urma_status_t>(injectedStatus);
        pushed.byteCnt = 0;
    }
    {
        std::lock_guard<std::mutex> lk(mu_);
        crs_.push_back(pushed);
        if (eventFd_ >= 0) {
            uint64_t one = 1;
            ssize_t n = ::write(eventFd_, &one, sizeof(one));
            (void)n;
        }
    }
    cv_.notify_all();
}

int MockJfc::Poll(int maxCr, urma_cr_t *outRecords)
{
    if (maxCr <= 0 || outRecords == nullptr) {
        return 0;
    }
    std::lock_guard<std::mutex> lk(mu_);
    int n = std::min(static_cast<int>(crs_.size()), maxCr);
    for (int i = 0; i < n; ++i) {
        const auto &c = crs_.front();
        outRecords[i].status = static_cast<urma_cr_status_t>(c.status);
        outRecords[i].byte_cnt = c.byteCnt;
        outRecords[i].user_ctx = c.userCtx;
        outRecords[i].opcode = c.opcodeIsRead ? URMA_OPC_READ : URMA_OPC_WRITE;
        outRecords[i].immediate_data = static_cast<int>(c.immData);
        outRecords[i].local_id = c.localId;
        crs_.pop_front();
    }
    return n;
}

int MockJfc::Wait(int maxEvents, int timeoutMs, MockJfc **outJfc)
{
    std::unique_lock<std::mutex> lk(mu_);
    if (crs_.empty()) {
        auto pred = [this] { return !crs_.empty(); };
        if (timeoutMs < 0) {
            cv_.wait(lk, pred);
        } else {
            cv_.wait_for(lk, std::chrono::milliseconds(timeoutMs), pred);
        }
    }
    if (outJfc) {
        *outJfc = this;
    }
    if (crs_.empty()) {
        return 0;
    }
    (void)maxEvents;  // single-batch ack
    return 1;
}

void MockJfc::Ack(uint32_t ackCnt)
{
    (void)ackCnt;
}

void MockJfc::Rearm(bool enableEvents)
{
    std::lock_guard<std::mutex> lk(mu_);
    rearmed_ = enableEvents;
    if (eventFd_ < 0 && enableEvents) {
        eventFd_ = ::eventfd(0, EFD_NONBLOCK);
    }
}

}  // namespace urma_mock
}  // namespace datasystem
