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
 * Description: Reproduce fd exhaustion on ZmqFrontend::InitFrontend failure path.
 */
#include <algorithm>
#include <cerrno>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <memory>
#include <string>

#include <sys/resource.h>
#include <unistd.h>

#include "common.h"

#include "datasystem/common/rpc/rpc_channel.h"
#include "datasystem/common/rpc/zmq/zmq_context.h"
#include "datasystem/common/rpc/zmq/zmq_stub_conn.h"

namespace datasystem {
namespace st {
namespace {
size_t CountOpenFds()
{
    DIR *dir = opendir("/proc/self/fd");
    if (dir == nullptr) {
        return 0;
    }
    size_t count = 0;
    struct dirent *entry = nullptr;
    while ((entry = readdir(dir)) != nullptr) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }
        count++;
    }
    closedir(dir);
    return count;
}

bool IsProcessFdExhausted()
{
    int fd = open("/dev/null", O_RDONLY);
    if (fd >= 0) {
        (void)close(fd);
        return false;
    }
    return errno == EMFILE;
}

class NoFileLimitGuard {
public:
    NoFileLimitGuard()
    {
        valid_ = (getrlimit(RLIMIT_NOFILE, &original_) == 0);
    }

    ~NoFileLimitGuard()
    {
        if (valid_ && changed_) {
            (void)setrlimit(RLIMIT_NOFILE, &original_);
        }
    }

    bool IsValid() const
    {
        return valid_;
    }

    const struct rlimit &Original() const
    {
        return original_;
    }

    bool SetSoftLimit(rlim_t soft)
    {
        if (!valid_) {
            return false;
        }
        struct rlimit target = original_;
        target.rlim_cur = std::min<rlim_t>(soft, original_.rlim_max);
        if (target.rlim_cur == original_.rlim_cur) {
            return true;
        }
        if (setrlimit(RLIMIT_NOFILE, &target) != 0) {
            return false;
        }
        changed_ = true;
        return true;
    }

private:
    bool valid_ = false;
    bool changed_ = false;
    struct rlimit original_ = {};
};
}  // namespace

class ZmqFdLeakReproTest : public CommonTest {};

TEST_F(ZmqFdLeakReproTest, InitFrontendConnectFailureCanExhaustFd)
{
    NoFileLimitGuard limitGuard;
    ASSERT_TRUE(limitGuard.IsValid()) << "getrlimit(RLIMIT_NOFILE) failed: " << strerror(errno);

    const size_t baselineFd = CountOpenFds();
    ASSERT_GT(baselineFd, 0U) << "Cannot inspect /proc/self/fd";

    // Keep enough headroom for test process internals while forcing EMFILE quickly.
    const rlim_t targetSoftLimit = std::min<rlim_t>(
        limitGuard.Original().rlim_cur, static_cast<rlim_t>(std::max<size_t>(baselineFd + 128, 256)));
    if (targetSoftLimit <= baselineFd + 32) {
        GTEST_SKIP() << "Not enough RLIMIT_NOFILE headroom to run leak reproduction. baseline=" << baselineFd
                     << ", current soft=" << limitGuard.Original().rlim_cur;
    }
    ASSERT_TRUE(limitGuard.SetSoftLimit(targetSoftLimit)) << "setrlimit(RLIMIT_NOFILE) failed: " << strerror(errno);

    auto ctx = std::make_shared<ZmqContext>();
    DS_ASSERT_OK(ctx->Init());

    // invalid:// is rejected by zmq_connect, forcing InitFrontend error path after socket creation.
    auto invalidChannel = std::make_shared<RpcChannel>("invalid://127.0.0.1:31501", RpcCredential());
    ZmqFrontend frontend(ctx, invalidChannel, nullptr);

    bool seenEmfile = false;
    size_t peakFd = baselineFd;
    const size_t maxAttempts = std::max<size_t>(256, (targetSoftLimit - baselineFd) * 8);
    for (size_t i = 0; i < maxAttempts; ++i) {
        std::shared_ptr<ZmqSocket> out;
        Status rc = frontend.InitFrontend(ctx, invalidChannel, out);
        EXPECT_TRUE(rc.IsError());
        EXPECT_TRUE(out == nullptr);
        peakFd = std::max(peakFd, CountOpenFds());

        // InitFrontend returns a generic status when socket creation fails,
        // while the detailed EMFILE reason is emitted by CreateZmqSocket log.
        if (rc.GetMsg().find("Too many open files") != std::string::npos || IsProcessFdExhausted()) {
            seenEmfile = true;
            break;
        }
    }

    // Cleanup leaked sockets by terminating the context.
    ctx->Close(false);

    EXPECT_GT(peakFd, baselineFd + 8)
        << "No obvious fd growth observed. baseline=" << baselineFd << ", peak=" << peakFd;
    EXPECT_TRUE(!seenEmfile) << "Did not detect process EMFILE in InitFrontend failure path after " << maxAttempts
                            << " attempts. baseline fd=" << baselineFd << ", peak fd=" << peakFd
                            << ", soft limit=" << targetSoftLimit;
}
}  // namespace st
}  // namespace datasystem
