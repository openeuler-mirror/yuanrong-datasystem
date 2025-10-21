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
 * Description: ObjInterface test.
 */

#include "common.h"
#include "datasystem/common/object_cache/object_base.h"

namespace datasystem {
namespace ut {
class ObjectInterfaceTest : public CommonTest {};

class FakeObject : public ObjectInterface {
public:
    ~FakeObject() override = default;

    Status FreeResources() override
    {
        return Status::OK();
    }
};

TEST_F(ObjectInterfaceTest, TestGetterSetters)
{
    std::shared_ptr<ObjectInterface> interface = std::make_shared<FakeObject>();
    auto &testObject = *interface;
    ASSERT_EQ(testObject.IsInvalid(), false);
    ASSERT_EQ(testObject.IsSealed(), false);
    ASSERT_EQ(testObject.IsPublished(), false);
    ASSERT_EQ(testObject.GetLifeState(), ObjectLifeState::OBJECT_INVALID);
    ASSERT_EQ(testObject.GetCreateTime(), 0ul);
    testObject.SetDataSize(1);
    testObject.SetMetadataSize(1);
    ASSERT_EQ(testObject.GetDataSize(), 0ul);
    ASSERT_EQ(testObject.GetMetadataSize(), 0ul);
    ASSERT_EQ(testObject.GetAddress(), std::string());
    testObject.SetLifeState(ObjectLifeState::OBJECT_INVALID);
    testObject.SetCreateTime(0);
    testObject.SetAddress(std::string());
    ASSERT_EQ(testObject.Erase("field"), Status::OK());
    ASSERT_EQ(testObject.IsShm(), false);
}
}  // namespace ut
}  // namespace datasystem