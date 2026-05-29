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
 * Description: RPC diagnostic helper tests.
 */
#include "datasystem/common/util/rpc_diagnostic.h"

#include "ut/common.h"

namespace datasystem {
namespace ut {
class RpcDiagnosticTest : public CommonTest {};

TEST_F(RpcDiagnosticTest, FormatServiceRpcDiag)
{
    RpcDiagnosticInfo info{ "QueryMeta", "192.0.2.10:45954", "198.51.100.13:31501" };
    Status status(StatusCode::K_RPC_DEADLINE_EXCEEDED, "Request timeout");

    EXPECT_EQ(FormatRpcDiag(info, status),
              "[192.0.2.10:45954]-QueryMeta->[198.51.100.13:31501] Request timeout");
}

TEST_F(RpcDiagnosticTest, FormatClientRpcDiagWithoutSrc)
{
    RpcDiagnosticInfo info{ "Create", "", "192.0.2.12:45954" };
    Status status(StatusCode::K_RPC_UNAVAILABLE, "Rpc session is null");

    EXPECT_EQ(FormatRpcDiag(info, status), "Create->[192.0.2.12:45954] Rpc session is null");
}

TEST_F(RpcDiagnosticTest, StripStatusCodePrefix)
{
    RpcDiagnosticInfo info{ "QueryMeta", "192.0.2.10:45954", "198.51.100.13:31501" };
    Status status(StatusCode::K_RPC_DEADLINE_EXCEEDED,
                  "StatusCode: K_RPC_DEADLINE_EXCEEDED, Request timeout");

    EXPECT_EQ(FormatRpcDiag(info, status),
              "[192.0.2.10:45954]-QueryMeta->[198.51.100.13:31501] Request timeout");
}

TEST_F(RpcDiagnosticTest, StripLegacyStatusToStringPrefix)
{
    RpcDiagnosticInfo info{ "QueryMeta", "192.0.2.10:45954", "198.51.100.13:31501" };
    Status status(StatusCode::K_RPC_DEADLINE_EXCEEDED,
                  "code: [K_RPC_DEADLINE_EXCEEDED], msg: [Request timeout]");

    EXPECT_EQ(FormatRpcDiag(info, status),
              "[192.0.2.10:45954]-QueryMeta->[198.51.100.13:31501] Request timeout");
}

TEST_F(RpcDiagnosticTest, WithRpcDiagDoesNotWrapOk)
{
    RpcDiagnosticInfo info{ "Get", "", "192.0.2.12:45954" };
    Status status = WithRpcDiag(Status::OK(), info);

    EXPECT_TRUE(status.IsOk());
    EXPECT_TRUE(status.GetMsg().empty());
}

TEST_F(RpcDiagnosticTest, WithRpcDiagKeepsOriginalCode)
{
    RpcDiagnosticInfo info{ "Get", "", "192.0.2.12:45954" };
    Status status = WithRpcDiag(Status(StatusCode::K_RPC_UNAVAILABLE, "Request failed"), info);

    EXPECT_EQ(status.GetCode(), StatusCode::K_RPC_UNAVAILABLE);
    EXPECT_EQ(status.GetMsg(), "Get->[192.0.2.12:45954] Request failed");
}

TEST_F(RpcDiagnosticTest, WithRpcDiagHostPortOverloadsFormatEndpoints)
{
    HostPort src("192.0.2.10", 45954);
    HostPort dst("198.51.100.13", 31501);

    auto serviceStatus = WithRpcDiag(Status(StatusCode::K_RPC_DEADLINE_EXCEEDED, "Request timeout"), "QueryMeta", src,
                                     dst);
    EXPECT_EQ(serviceStatus.GetMsg(), "[192.0.2.10:45954]-QueryMeta->[198.51.100.13:31501] Request timeout");

    auto clientStatus = WithRpcDiag(Status(StatusCode::K_RPC_UNAVAILABLE, "Request failed"), "Get", dst);
    EXPECT_EQ(clientStatus.GetMsg(), "Get->[198.51.100.13:31501] Request failed");
}

TEST_F(RpcDiagnosticTest, WithRpcDiagHostPortOverloadDoesNotWrapOk)
{
    HostPort dst("198.51.100.13", 31501);
    auto status = WithRpcDiag(Status::OK(), "Get", dst);

    EXPECT_TRUE(status.IsOk());
    EXPECT_TRUE(status.GetMsg().empty());
}

TEST_F(RpcDiagnosticTest, FormatRpcDiagPreservesOriginalReasonFormatting)
{
    RpcDiagnosticInfo info{ "GetObjectRemote", "192.0.2.10:45954", "198.51.100.13:31501" };
    Status status(StatusCode::K_RPC_UNAVAILABLE, "Request failed\nLine of code : 382\nFile : worker.cpp");

    EXPECT_EQ(FormatRpcDiag(info, status),
              "[192.0.2.10:45954]-GetObjectRemote->[198.51.100.13:31501] "
              "Request failed\nLine of code : 382\nFile : worker.cpp");
}

TEST_F(RpcDiagnosticTest, WithRpcDiagPreservesStatusLocation)
{
    RpcDiagnosticInfo info{ "GetObjectRemote", "192.0.2.10:45954", "198.51.100.13:31501" };
    Status status(StatusCode::K_RPC_DEADLINE_EXCEEDED, 321, "/tmp/queue.h", "Request timeout");

    status = WithRpcDiag(status, info);

    EXPECT_EQ(status.GetCode(), StatusCode::K_RPC_DEADLINE_EXCEEDED);
    EXPECT_NE(status.GetMsg().find("[192.0.2.10:45954]-GetObjectRemote->[198.51.100.13:31501]"),
              std::string::npos);
    EXPECT_NE(status.GetMsg().find("Request timeout"), std::string::npos);
    EXPECT_NE(status.GetMsg().find("Line of code : 321"), std::string::npos);
    EXPECT_NE(status.GetMsg().find("File         : queue.h"), std::string::npos);
}

TEST_F(RpcDiagnosticTest, AvoidDoubleWrap)
{
    RpcDiagnosticInfo info{ "QueryMeta", "192.0.2.10:45954", "198.51.100.13:31501" };
    Status status(StatusCode::K_RPC_DEADLINE_EXCEEDED,
                  "[192.0.2.10:45954]-QueryMeta->[198.51.100.13:31501] Request timeout");

    EXPECT_EQ(FormatRpcDiag(info, status),
              "[192.0.2.10:45954]-QueryMeta->[198.51.100.13:31501] Request timeout");
}

TEST_F(RpcDiagnosticTest, ClientDoesNotWrapNestedServiceRpcDiag)
{
    RpcDiagnosticInfo info{ "Get", "", "192.0.2.12:45954" };
    Status status(StatusCode::K_RPC_DEADLINE_EXCEEDED,
                  "[192.0.2.12:45954]-GetObjectRemote->[198.51.100.13:31501] Request timeout");

    EXPECT_EQ(FormatRpcDiag(info, status),
              "[192.0.2.12:45954]-GetObjectRemote->[198.51.100.13:31501] Request timeout");
}
}  // namespace ut
}  // namespace datasystem
