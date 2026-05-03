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
 * Description: Testing StreamMetaShm.
 */
#include <gtest/gtest.h>

#include <securec.h>

#include "ut/common.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/common/shared_memory/shm_unit.h"
#include "datasystem/common/stream_cache/stream_meta_shm.h"

namespace datasystem {
namespace ut {
class StreamMetaShmTest : public CommonTest {
public:
    const uint64_t streamMetaShmSize_ = 64;
    const uint64_t maxStreamSize_ = 4 * 1024;
};

TEST_F(StreamMetaShmTest, BasicTest)
{
    size_t maxSize = 1024 * 1024ul * 1024ul;
    DS_ASSERT_OK(datasystem::memory::Allocator::Instance()->Init(maxSize));

    auto shmUnitOfStreamMeta = std::make_unique<ShmUnit>();
    DS_ASSERT_OK(shmUnitOfStreamMeta->AllocateMemory("", streamMetaShmSize_, false, ServiceType::STREAM));
    auto rc = memset_s(shmUnitOfStreamMeta->GetPointer(), streamMetaShmSize_, 0, streamMetaShmSize_);
    ASSERT_EQ(rc, 0);
    auto streamMetaShm = std::make_unique<StreamMetaShm>("stream0", shmUnitOfStreamMeta->GetPointer(),
                                                         streamMetaShmSize_, maxStreamSize_);
    DS_ASSERT_OK(streamMetaShm->Init());

    DS_ASSERT_NOT_OK(streamMetaShm->TryDecUsage(1));
    DS_ASSERT_OK(streamMetaShm->TryIncUsage(1));
    DS_ASSERT_OK(streamMetaShm->TryDecUsage(1));
    DS_ASSERT_NOT_OK(streamMetaShm->TryDecUsage(1));
    DS_ASSERT_OK(streamMetaShm->TryIncUsage(maxStreamSize_));
    DS_ASSERT_NOT_OK(streamMetaShm->TryIncUsage(1));
    DS_ASSERT_OK(streamMetaShm->TryDecUsage(1));
    DS_ASSERT_OK(streamMetaShm->TryIncUsage(1));
}

}  // namespace ut
}  // namespace datasystem
