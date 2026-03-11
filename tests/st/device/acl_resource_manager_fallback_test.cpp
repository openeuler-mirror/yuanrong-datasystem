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

#include <string>
#include <vector>

#include "common.h"
#include "datasystem/common/device/ascend/acl_resource_manager.h"
#include "mock/ascend_device_manager_mock.cpp"

namespace datasystem {
namespace st {
using namespace acl;

namespace {
class TestAclResourceManager : public AclResourceManager {
public:
    void Configure(uint64_t hostSize, uint64_t deviceSize, MemcopyPolicy d2hPolicy, MemcopyPolicy h2dPolicy)
    {
        hostMemSize = hostSize;
        deviceMemSize = deviceSize;
        policyD2H = d2hPolicy;
        policyH2D = h2dPolicy;
    }
};

DeviceBatchCopyHelper BuildHelper(const std::vector<std::string> &payloads, std::vector<std::vector<char>> &destBuffers)
{
    DeviceBatchCopyHelper helper;
    destBuffers.clear();
    destBuffers.reserve(payloads.size());
    helper.bufferMetas.reserve(payloads.size());
    helper.srcList.reserve(payloads.size());
    helper.dstList.reserve(payloads.size());
    helper.dataSizeList.reserve(payloads.size());
    size_t firstBlobOffset = 0;
    for (const auto &payload : payloads) {
        destBuffers.emplace_back(payload.size(), '\0');
        helper.bufferMetas.emplace_back(BufferMetaInfo{ .blobCount = 1, .firstBlobOffset = firstBlobOffset,
                                                        .size = payload.size() });
        helper.srcList.emplace_back(const_cast<char *>(payload.data()));
        helper.dstList.emplace_back(destBuffers.back().data());
        helper.dataSizeList.emplace_back(payload.size());
        helper.batchSize++;
        firstBlobOffset++;
    }
    return helper;
}
}  // namespace

class AclResourceManagerFallbackTest : public CommonTest {
protected:
    void SetUp() override
    {
        BINEXPECT_CALL(&AclDeviceManager::Instance, ()).WillRepeatedly([]() {
            return AclDeviceManagerMock::Instance();
        });
    }
};

TEST_F(AclResourceManagerFallbackTest, FallbackToDirectWhenHostPinPoolTooSmall)
{
    TestAclResourceManager resourceMgr;
    resourceMgr.Configure(60, 1024, MemcopyPolicy::FFTS, MemcopyPolicy::FFTS);
    AclMemCopyPool copyPool(&resourceMgr);

    std::vector<std::string> payloads = { std::string(40, 'a'), std::string(30, 'b') };
    std::vector<std::vector<char>> destBuffers;
    auto helper = BuildHelper(payloads, destBuffers);

    DS_ASSERT_OK(copyPool.MemcpyBatchH2D(0, helper, MemcopyPolicy::FFTS));
    ASSERT_EQ(std::string(destBuffers[0].begin(), destBuffers[0].end()), payloads[0]);
    ASSERT_EQ(std::string(destBuffers[1].begin(), destBuffers[1].end()), payloads[1]);
}

TEST_F(AclResourceManagerFallbackTest, FallbackToDirectWhenDevicePinPoolTooSmall)
{
    TestAclResourceManager resourceMgr;
    resourceMgr.Configure(1024, 100, MemcopyPolicy::FFTS, MemcopyPolicy::FFTS);
    AclMemCopyPool copyPool(&resourceMgr);

    std::vector<std::string> payloads = { std::string(60, 'c') };
    std::vector<std::vector<char>> destBuffers;
    auto helper = BuildHelper(payloads, destBuffers);

    DS_ASSERT_OK(copyPool.MemcpyBatchH2D(0, helper, MemcopyPolicy::FFTS));
    ASSERT_EQ(std::string(destBuffers[0].begin(), destBuffers[0].end()), payloads[0]);
}
}  // namespace st
}  // namespace datasystem
