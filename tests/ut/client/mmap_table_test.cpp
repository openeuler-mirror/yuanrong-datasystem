/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: Mmap table class test.
 */
#include "datasystem/client/mmap_table.h"

#ifdef __linux__
#include <linux/memfd.h>
#endif
#include <sys/mman.h>
#include <sys/syscall.h>

#include "datasystem/common/util/status_helper.h"
#include "common.h"

using namespace datasystem::client;

namespace datasystem {
namespace ut {
class MmapTableTest : public CommonTest {
public:
    void SetUp() override
    {
        mmapTable_ = std::make_unique<MmapTable>(false);
        int32_t mmapSize = 1024;
        clientFd1_ = CreateFd(mmapSize);
        clientFd2_ = CreateFd(mmapSize);
        ASSERT_GE(clientFd1_, 0);
        ASSERT_GE(clientFd2_, 0);
    };

    // every TEST_F macro will call TearDown when end
    void TearDown() override
    {
        close(clientFd1_);
        close(clientFd2_);
    };

    int32_t CreateFd(int32_t size)
    {
        std::string tmpfs = "MmapTableTest";
        int32_t fd = syscall(SYS_memfd_create, tmpfs.c_str(), MFD_ALLOW_SEALING);
        int ret = ftruncate(fd, static_cast<off_t>(size));
        if (ret != 0) {
            return -1;
        }
        return fd;
    }

    int32_t clientFd1_;
    int32_t clientFd2_;
    std::unique_ptr<MmapTable> mmapTable_;
};

TEST_F(MmapTableTest, TestMmapTableBasicFunction)
{
    LOG(INFO) << "Test mmap table basic function.";

    // Add to the mmapTable.
    int fakeFd = -1;
    int workerFd = 10;
    DS_ASSERT_OK(mmapTable_->MmapAndStoreFd(clientFd1_, workerFd, 1024));
    DS_ASSERT_OK(mmapTable_->MmapAndStoreFd(clientFd2_, workerFd, 1024));

    uint8_t *pointer;
    DS_ASSERT_OK(mmapTable_->LookupFdPointer(workerFd, &pointer));
    DS_ASSERT_NOT_OK(mmapTable_->LookupFdPointer(fakeFd, &pointer));

    auto existed = mmapTable_->FindFd(workerFd);
    ASSERT_EQ(existed, true);
    existed = mmapTable_->FindFd(fakeFd);
    ASSERT_EQ(existed, false);
}

TEST_F(MmapTableTest, TestMmapTableEntryInvalidParameter)
{
    LOG(INFO) << "Test mmap table entry invalid parameter.";

    int fakeFd = -1;
    auto entry = std::make_unique<MmapTableEntry>(clientFd1_, 0);
    Status status = entry->Init(false);
    ASSERT_EQ(status.GetCode(), StatusCode::K_INVALID);

    auto entry1 = std::make_unique<MmapTableEntry>(fakeFd, 5120);
    status = entry1->Init(false);
    ASSERT_EQ(status.GetCode(), StatusCode::K_RUNTIME_ERROR);
}
 
TEST_F(MmapTableTest, TestGetMmapEntry)
{
    LOG(INFO) << "Test mmap table decrease mmap ref.";
 
    int workerFd1 = 10;
    int workerFd2 = 11;
    int32_t mmapSize = 1024;
    DS_ASSERT_OK(mmapTable_->MmapAndStoreFd(clientFd1_, workerFd1, mmapSize));
    DS_ASSERT_OK(mmapTable_->MmapAndStoreFd(clientFd2_, workerFd2, mmapSize));
    ASSERT_TRUE(mmapTable_->FindFd(workerFd1));
    ASSERT_TRUE(mmapTable_->FindFd(workerFd2));
 
    ASSERT_TRUE(mmapTable_->GetMmapEntryByFd(workerFd1) != nullptr);
    ASSERT_TRUE(mmapTable_->GetMmapEntryByFd(workerFd2) != nullptr);
 
    ASSERT_TRUE(mmapTable_->FindFd(workerFd1));
    ASSERT_TRUE(mmapTable_->FindFd(workerFd2));
 
    mmapTable_->CleanInvalidMmapTable();
    ASSERT_FALSE(mmapTable_->FindFd(workerFd1));
}
}  // namespace ut
}  // namespace datasystem