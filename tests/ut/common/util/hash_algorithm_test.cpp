/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: Hash algorithm test.
 */
#include "ut/common.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/hash_algorithm.h"

namespace datasystem {
namespace ut {
class HashAlgorithmTest : public CommonTest {};

TEST_F(HashAlgorithmTest, ExampleHash)
{
    std::string data = "string-of-any-length";
    EXPECT_EQ(MurmurHash3_32(data), 3643825436u);
    EXPECT_EQ(MurmurHash3_32(reinterpret_cast<const uint8_t *>(data.data()), data.size(), 1234), 209224272u);

    data = "string-of-any-length0";
    EXPECT_EQ(MurmurHash3_32(data), 202734715u);
    EXPECT_EQ(MurmurHash3_32(reinterpret_cast<const uint8_t *>(data.data()), data.size(), 1234), 3628035857u);
}

TEST_F(HashAlgorithmTest, RedirectInject)
{
    const uint32_t node0_redirect = 116852666;
    const uint32_t node1_redirect = 457913941;
    const uint32_t node2_redirect = 715827882;
    DS_EXPECT_OK(datasystem::inject::Set("add.node.redirect", "3*return()"));
    LOG(INFO) << "Injection point set, current count: " << datasystem::inject::GetExecuteCount("add.node.redirect");

    std::string data = "redirect_test_0";
    EXPECT_EQ(MurmurHash3_32(data), node0_redirect);
    data = "redirect_test_1";
    EXPECT_EQ(MurmurHash3_32(data), node1_redirect);
    data = "redirect_test_2";
    EXPECT_EQ(MurmurHash3_32(data), node2_redirect);
}

}  // namespace ut
}  // namespace datasystem
