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
 * Description: MemMmap class test.
 */
#include "datasystem/common/shared_memory/shared_disk_detecter.h"

#include <asm-generic/errno.h>
#include <fcntl.h>

#include <gtest/gtest.h>
#include <sys/types.h>
#include <unistd.h>

#include "common.h"
#include "../../../common/binmock/binmock.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/utils/status.h"
#include "gmock/gmock.h"

using namespace ::testing;
using namespace ::datasystem::memory;

namespace datasystem {
namespace ut {
class SharedDiskDetecterTest : public CommonTest {
public:
    ssize_t FakeWrite(int fd, const void *buf, size_t n);
    Status FakeOpen(const std::string &pathname, int flags, mode_t mode, int *fileDescriptor);
};

ssize_t SharedDiskDetecterTest::FakeWrite(int fd, const void *buf, size_t n)
{
    (void)fd;
    (void)buf;
    (void)n;
    errno = EROFS;
    return -1;
}

Status SharedDiskDetecterTest::FakeOpen(const std::string &pathname, int flags, mode_t mode, int *fileDescriptor)
{
    (void)pathname;
    (void)flags;
    (void)mode;
    (void)fileDescriptor;
    errno = EROFS;
    return Status(StatusCode::K_RUNTIME_ERROR, "");
}

TEST_F(SharedDiskDetecterTest, TestDiskFullAndRecovery)
{
    if (IsArmArchitecture()) {
        GTEST_SKIP() << "Skipped on ARM architecture";
    }
    uint64_t intervalMs = 10;
    uint64_t remainBytes = 100;
    char path[PATH_MAX + 1] = { 0 };
    auto ret = getcwd(path, sizeof(path));
    ASSERT_TRUE(ret != nullptr);
    BINEXPECT_CALL(&GetFreeSpaceBytes, (_)).WillRepeatedly(Return(remainBytes));
    SharedDiskDetecter detecter(path, intervalMs);
    ASSERT_TRUE(detecter.IsAvailable());
    int sleepUs = 20'000;
    usleep(sleepUs);
    ASSERT_FALSE(detecter.IsAvailable());
    RELEASE_STUBS
    BINEXPECT_CALL(&write, (_, _, _)).WillRepeatedly(Return(1));
    int sleepAfterReleaseUs = 40'000;
    usleep(sleepAfterReleaseUs);
    ASSERT_TRUE(detecter.IsAvailable());
}

TEST_F(SharedDiskDetecterTest, TestDiskIOFailure)
{
    if (IsArmArchitecture()) {
        GTEST_SKIP() << "Skipped on ARM architecture";
    }
    uint64_t intervalMs = 10;
    char path[PATH_MAX + 1] = { 0 };
    auto ret = getcwd(path, sizeof(path));
    ASSERT_TRUE(ret != nullptr);
    SharedDiskDetecter detecter(path, intervalMs);
    ASSERT_TRUE(detecter.IsAvailable());
    BINEXPECT_CALL(&write, (_, _, _)).WillRepeatedly(Invoke(this, &SharedDiskDetecterTest::FakeWrite));
    int sleepUs = 20'000;
    usleep(sleepUs);
    ASSERT_FALSE(detecter.IsAvailable());
    RELEASE_STUBS
    BINEXPECT_CALL(&write, (_, _, _)).WillRepeatedly(Return(1));
    usleep(sleepUs);
    ASSERT_TRUE(detecter.IsAvailable());
}

TEST_F(SharedDiskDetecterTest, TestDiskOpenFailure)
{
    if (IsArmArchitecture()) {
        GTEST_SKIP() << "Skipped on ARM architecture";
    }
    uint64_t intervalMs = 10;
    char path[PATH_MAX + 1] = { 0 };
    auto ret = getcwd(path, sizeof(path));
    ASSERT_TRUE(ret != nullptr);
    BINEXPECT_CALL((Status(*)(const std::string &, int, mode_t, int *))OpenFile, (_, _, _, _))
        .WillRepeatedly(Invoke(this, &SharedDiskDetecterTest::FakeOpen));
    SharedDiskDetecter detecter(path, intervalMs);
    ASSERT_TRUE(detecter.IsAvailable());
    int sleepUs = 20'000;
    usleep(sleepUs);
    ASSERT_FALSE(detecter.IsAvailable());
    RELEASE_STUBS
    BINEXPECT_CALL(&write, (_, _, _)).WillRepeatedly(Return(1));
    int sleepAfterReleaseUs = 40'000;
    usleep(sleepAfterReleaseUs);
    ASSERT_TRUE(detecter.IsAvailable());
}
}  // namespace ut
}  // namespace datasystem