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

#include <gtest/gtest.h>

#include <mutex>

#include <unistd.h>

#include "common.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/worker/object_cache/data_migrator/handler/async_resource_releaser.h"
#include "eviction_manager_common.h"

using namespace datasystem::object_cache;

namespace datasystem {
namespace ut {

class AsyncResourceReleaserTest : public CommonTest, public EvictionManagerCommon {
public:
    AsyncResourceReleaserTest() = default;
    ~AsyncResourceReleaserTest() override = default;

    void SetUp() override
    {
        CommonTest::SetUp();

        (void)memory::Allocator::Instance()->Init(64ul * 1024ul * 1024ul);

        objectTable_ = std::make_shared<ObjectTable>();
        AsyncResourceReleaser::Instance().Init(objectTable_);
    }

    void TearDown() override
    {
        AsyncResourceReleaser::Instance().Shutdown();
    }

protected:
    const uint64_t dataSize_ = 16;
};

TEST_F(AsyncResourceReleaserTest, ReleaseNotFoundReturnsOk)
{
    DS_ASSERT_OK(AsyncResourceReleaser::Instance().Release("not_exist", 1));
}

TEST_F(AsyncResourceReleaserTest, ReleaseErasesWhenVersionNotAdvanced)
{
    DS_ASSERT_OK(CreateObject("k1", dataSize_));

    DS_ASSERT_OK(AsyncResourceReleaser::Instance().Release("k1", 1));

    DS_ASSERT_NOT_OK(objectTable_->Contains("k1"));
}

TEST_F(AsyncResourceReleaserTest, ReleaseSkipsWhenVersionAdvanced)
{
    DS_ASSERT_OK(CreateObject("k1", dataSize_));

    DS_ASSERT_OK(AsyncResourceReleaser::Instance().Release("k1", 0));

    DS_ASSERT_OK(objectTable_->Contains("k1"));
}

}  // namespace ut
}  // namespace datasystem
