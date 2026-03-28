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
 * Description: shared disk detecter.
 */

#include "datasystem/common/shared_memory/shared_disk_detecter.h"

#include <cerrno>
#include <chrono>
#include <asm-generic/errno-base.h>
#include <fcntl.h>
#include <memory>
#include <mutex>

#include <unistd.h>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#ifdef WITH_TESTS
#include "datasystem/common/inject/inject_point.h"
#endif
namespace datasystem {
namespace memory {

SharedDiskDetecter::SharedDiskDetecter(const std::string &path, uint64_t intervalMs)
    : path_(path), intervalMs_(intervalMs), available_(true), running_(true)
{
    thread_ = std::make_unique<Thread>([this]() { Run(); });
}

SharedDiskDetecter::~SharedDiskDetecter()
{
    running_ = false;
    cond_.notify_all();
    thread_->join();
    thread_.reset();
}

bool SharedDiskDetecter::IsAvailable()
{
    return available_;
}

void SharedDiskDetecter::Run()
{
    std::unique_lock<std::mutex> l(mtx_);
    while (running_) {
        Detect();
        auto waitTimeoutMs = available_ ? intervalMs_ : intervalMs_ / 2;
        auto status = cond_.wait_for(l, std::chrono::milliseconds(waitTimeoutMs));
        if (status != std::cv_status::timeout) {
            break;
        }
    }
}

void SharedDiskDetecter::Detect()
{
    constexpr uint64_t minBytes = 1024ul * 1024ul;
    // 1. check space is full
    auto freeBytes = GetFreeSpaceBytes(path_);
#ifdef WITH_TESTS
    INJECT_POINT("disk_detecter.free_bytes", [&freeBytes]() { freeBytes = 0; });
#endif
    if (freeBytes < minBytes) {
        available_ = false;
        LOG(WARNING) << "[Disk Detect] Space is full, just remain " << freeBytes << " bytes";
        return;
    }

    // 2. check io
    constexpr int logIntervalSeconds = 8;
    std::string filename = path_ + "/datasystem_test_file";
    constexpr int permission = 0640;
    int fd = 0;
    Status s = OpenFile(filename, O_RDWR | O_CREAT | O_TRUNC | O_SYNC, permission, &fd);
    if (s.IsError()) {
        if (errno != EINTR || errno != EMFILE) {
            available_ = false;
            LOG_EVERY_T(WARNING, logIntervalSeconds) << "[Disk Detect] Disk IO failed: " << StrErr(errno);
        }
        return;
    }
    (void)DeleteFile(filename.c_str());
    char buffer[] = "Disk I/O test data";
    auto bytes_written = write(fd, buffer, strlen(buffer));
    if (bytes_written == -1) {
        if (errno != EINTR) {
            available_ = false;
            LOG_EVERY_T(WARNING, logIntervalSeconds) << "[Disk Detect] Disk IO failed: " << StrErr(errno);
        }
        RETRY_ON_EINTR(close(fd));
        return;
    }

    if (!available_.exchange(true)) {
        LOG(INFO) << "[Disk Detect] Disk normal now.";
    }
    RETRY_ON_EINTR(close(fd));
}
}  // namespace memory
}  // namespace datasystem