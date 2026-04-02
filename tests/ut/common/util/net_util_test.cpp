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
 * Description: HostPort basic function test.
 */
#include "datasystem/common/util/net_util.h"

#include <net/if.h>

#include "ut/common.h"

namespace datasystem {
namespace ut {
class NetUtilTest : public CommonTest {};

TEST_F(NetUtilTest, TestParseAddress)
{
    LOG(INFO) << "Test HostPort Parse address.";
    {
        std::string addr = "0.0.0.0:8481";
        HostPort hostPort;
        DS_ASSERT_OK(hostPort.ParseString(addr));
        ASSERT_EQ(hostPort.Host(), "0.0.0.0");
        ASSERT_EQ(hostPort.Port(), 8481);
        ASSERT_EQ(hostPort.ToString(), "0.0.0.0:8481");
    }

    // Invalid input: large port
    {
        std::string addr = "0.0.0.0:100000";
        HostPort hostPort;
        Status status = hostPort.ParseString(addr);
        DS_ASSERT_NOT_OK(status);
    }

    // Invalid input: not number port
    {
        std::string addr = "0.0.0.0:Camille";
        HostPort hostPort;
        Status status = hostPort.ParseString(addr);
        DS_ASSERT_NOT_OK(status);
    }

    // Invalid input: not ip:port
    {
        std::string addr = "0.0.0.0::::8481";
        HostPort hostPort;
        Status status = hostPort.ParseString(addr);
        DS_ASSERT_NOT_OK(status);
        addr = "0.0.0.0.8481";
        status = hostPort.ParseString(addr);
        DS_ASSERT_NOT_OK(status);
    }
}

TEST_F(NetUtilTest, TestOperatorEqualAndOperatorNotEqual)
{
    LOG(INFO) << "Test HostPort operator== and operator!=";

    // Same host and same port.
    {
        HostPort one("0.0.0.0", 8481);
        HostPort another("0.0.0.0", 8481);
        ASSERT_TRUE(one == another);
        ASSERT_FALSE(one != another);
    }

    // Same host, not same port.
    {
        HostPort one("0.0.0.0", 8481);
        HostPort another("0.0.0.0", 8482);
        ASSERT_FALSE(one == another);
        ASSERT_TRUE(one != another);
    }

    // Not same host, same port.
    {
        HostPort one("0.0.0.1", 8481);
        HostPort another("0.0.0.2", 8481);
        ASSERT_FALSE(one == another);
        ASSERT_TRUE(one != another);
    }

    // Not same host, not same port.
    {
        HostPort one("0.0.0.1", 8481);
        HostPort another("0.0.0.2", 8482);
        ASSERT_FALSE(one == another);
        ASSERT_TRUE(one != another);
    }
}

TEST_F(NetUtilTest, TestMoveConstructor)
{
    LOG(INFO) << "Test HostPort move constructor.";
    HostPort one("0.0.0.0", 8481);
    HostPort another = std::move(one);
    ASSERT_EQ(another.Host(), "0.0.0.0");
    ASSERT_EQ(another.Port(), 8481);
    ASSERT_EQ(another.ToString(), "0.0.0.0:8481");
}

TEST_F(NetUtilTest, TestMoveAssignment)
{
    LOG(INFO) << "Test HostPort move constructor.";
    HostPort one("0.0.0.0", 8481);
    HostPort another("127.0.0.1", 2772);
    another = std::move(one);
    ASSERT_EQ(another.Host(), "0.0.0.0");
    ASSERT_EQ(another.Port(), 8481);
    ASSERT_EQ(another.ToString(), "0.0.0.0:8481");
}

TEST_F(NetUtilTest, TestToString)
{
    ASSERT_EQ(HostPort().ToString(), "");
    ASSERT_NE(HostPort("0.0.0.0", -1).ToString(), "");
    ASSERT_NE(HostPort("", 8481).ToString(), "");
}

TEST_F(NetUtilTest, TestHostPostHash)
{
    HostPort hp("127.0.0.1", 12598);
    ASSERT_EQ(hp.hash(), 9516083459692046967u);
    ASSERT_EQ(std::hash<HostPort>{}(hp), 14224256174011997484u);
}
}  // namespace ut
}  // namespace datasystem
