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

#include "ut/common.h"
#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/worker/object_cache/data_migrator/handler/async_resource_releaser.h"
#include "datasystem/worker/object_cache/worker_oc_eviction_manager.h"
#include "eviction_manager_common.h"
#include "tests/ut/worker/object_cache/test_metadata_route.h"

using namespace datasystem::object_cache;

namespace datasystem {
namespace ut {

namespace {
const HostPort LOCAL_ADDR("127.0.0.1", 31501);
const HostPort MASTER_ADDR("127.0.0.1", 31500);
}  // namespace

class AsyncResourceReleaserTest : public CommonTest, public EvictionManagerCommon {
public:
    AsyncResourceReleaserTest() = default;
    ~AsyncResourceReleaserTest() override = default;

    void SetUp() override
    {
        CommonTest::SetUp();

        (void)memory::Allocator::Instance()->Init(64ul * 1024ul * 1024ul);

        objectTable_ = std::make_shared<ObjectTable>();
        allocator = memory::Allocator::Instance();
        allocator->Init(maxMemorySize);
        akSkManager_ = std::make_shared<AkSkManager>(0);
        evictionManager_ = std::make_shared<WorkerOcEvictionManager>(objectTable_, LOCAL_ADDR, MASTER_ADDR,
                                                                     GetTestMetadataRoute());
        globalRefTable_ = std::make_shared<ObjectGlobalRefTable<ClientKey>>();
        DS_ASSERT_OK(evictionManager_->Init(globalRefTable_, akSkManager_));
        AsyncResourceReleaser::Instance().Init(objectTable_, evictionManager_);
    }

    void TearDown() override
    {
        AsyncResourceReleaser::Instance().Shutdown();
        evictionManager_.reset();
        globalRefTable_.reset();
        akSkManager_.reset();
        objectTable_.reset();
    }

protected:
    const uint64_t dataSize_ = 16;
    std::shared_ptr<AkSkManager> akSkManager_;
    std::shared_ptr<ObjectGlobalRefTable<ClientKey>> globalRefTable_;
    std::shared_ptr<WorkerOcEvictionManager> evictionManager_;
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

// Verifies the root-cause fix for issue #864: a SPILL migration that erases an object from the ObjectTable must also
// erase the corresponding EvictionList entry, otherwise the next rebalance candidate scan re-selects the gone object,
// logs "Key not found" every batch, and does pointless work. Release is the single point where the SPILL success path
// erases objects, so pairing eviction-list Erase here keeps the eviction list truthful at the source.
TEST_F(AsyncResourceReleaserTest, ReleaseErasesEvictionEntryAlongsideObjectTable)
{
    DS_ASSERT_OK(CreateObject("k1", dataSize_));
    evictionManager_->Add("k1");
    std::vector<EvictionList::Node> nodes;
    EvictionList::Node oldest;
    DS_ASSERT_OK(evictionManager_->GetAllObjectsInfo(nodes, oldest));
    ASSERT_EQ(nodes.size(), size_t(1));
    EXPECT_EQ(nodes[0].objectKey, "k1");

    DS_ASSERT_OK(AsyncResourceReleaser::Instance().Release("k1", 1));

    DS_ASSERT_NOT_OK(objectTable_->Contains("k1"));
    std::vector<EvictionList::Node> after;
    EvictionList::Node afterOldest;
    DS_ASSERT_OK(evictionManager_->GetAllObjectsInfo(after, afterOldest));
    EXPECT_TRUE(after.empty()) << "stale eviction-list entry must be purged alongside the object-table entry";
}

// Precision guard: when Release skips because the object version advanced (the object was overwritten and is still
// live), the eviction-list entry MUST remain. Erasing it there would drop a live object from the eviction list and
// break foreground eviction accounting.
TEST_F(AsyncResourceReleaserTest, ReleaseDoesNotEraseEvictionWhenVersionAdvanced)
{
    DS_ASSERT_OK(CreateObject("k1", dataSize_));
    evictionManager_->Add("k1");

    DS_ASSERT_OK(AsyncResourceReleaser::Instance().Release("k1", 0));

    DS_ASSERT_OK(objectTable_->Contains("k1"));
    std::vector<EvictionList::Node> after;
    EvictionList::Node afterOldest;
    DS_ASSERT_OK(evictionManager_->GetAllObjectsInfo(after, afterOldest));
    ASSERT_EQ(after.size(), size_t(1));
    EXPECT_EQ(after[0].objectKey, "k1");
}

}  // namespace ut
}  // namespace datasystem
