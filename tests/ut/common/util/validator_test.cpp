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
 * Description: Flag validator basic function test.
 */
#include "datasystem/common/util/validator.h"
#include <cstdint>
#include "datasystem/common/util/uuid_generator.h"

#include "common.h"

namespace datasystem {
namespace ut {
class ValidatorTest : public CommonTest {};

TEST_F(ValidatorTest, TestValidator1)
{
    EXPECT_TRUE(Validator::ValidateInt32("FlagName", 65));
    EXPECT_TRUE(Validator::ValidatePort("FlagName", 65535));
    EXPECT_FALSE(Validator::ValidatePort("FlagName", 65536));
    EXPECT_TRUE(Validator::ValidateRealPath("FlagName", "/tmp"));
    EXPECT_FALSE(Validator::ValidateRealPath("FlagName", "/path/not/exist"));
    EXPECT_TRUE(Validator::ValidatePathString("FlagName", "/path/To/Dir/"));
    EXPECT_FALSE(Validator::ValidateL2CacheType("FlagName", "whatever"));
    EXPECT_TRUE(Validator::ValidateRocksdbModeType("FlagName", "async"));
    EXPECT_FALSE(Validator::ValidateRocksdbModeType("FlagName", "whatever"));
    std::vector<std::string> validPaths = { "/home/sn/ttt", "~/home/sn/ttt", "!/home/sn/ttt", "qqq/" };
    std::vector<std::string> notValidPath = { "/ /sdaa", " /wdq//w", "///", "~//ef", "/home/ sn/ttt" };
    for (auto &path : validPaths) {
        EXPECT_TRUE(Validator::ValidatePathString("FlagName", path));
    }
    for (auto &path : notValidPath) {
        EXPECT_FALSE(Validator::ValidatePathString("FlagName", path));
    }
}

TEST_F(ValidatorTest, TestIsRegexMatch)
{
    re2::RE2 simpleIdRe{ "^[a-zA-Z0-9_]*$" };
    EXPECT_TRUE(Validator::IsRegexMatch(simpleIdRe, "wqeiqwo"));
    EXPECT_TRUE(Validator::IsRegexMatch(simpleIdRe, "wqeiqwoTUYTU38_90492"));
    EXPECT_FALSE(Validator::IsRegexMatch(simpleIdRe, "wqei;qwo"));
    EXPECT_FALSE(Validator::IsRegexMatch(simpleIdRe, "wqeiqwo?"));
    // stream
    re2::RE2 simpleIdRe1{ "^[a-zA-Z0-9\\~\\.\\-\\/_!@#%\\^\\&\\*\\(\\)\\+\\=\\:;]*$" };
    EXPECT_TRUE(Validator::IsRegexMatch(simpleIdRe1, "wqeiqwo"));
    EXPECT_TRUE(Validator::IsRegexMatch(simpleIdRe1, "wqeiqwoTUYTU38_90492"));
    EXPECT_TRUE(Validator::IsRegexMatch(simpleIdRe1, "wqei;qwo"));
    EXPECT_FALSE(Validator::IsRegexMatch(simpleIdRe1, "wqeiqwo?"));
    EXPECT_FALSE(Validator::IsRegexMatch(simpleIdRe1, "wqeiqwo$"));
}

TEST_F(ValidatorTest, TestValidateHostPortString)
{
    EXPECT_TRUE(Validator::ValidateHostPortString("FlagName", ""));
    EXPECT_TRUE(Validator::ValidateHostPortString("FlagName", "0.0.0.0:0"));
    EXPECT_TRUE(Validator::ValidateHostPortString("FlagName", "255.255.255.255:65535"));
    EXPECT_TRUE(Validator::ValidateHostPortString("FlagName", "[::1]:65535"));
    EXPECT_TRUE(Validator::ValidateHostPortString("FlagName", "[ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff]:65535"));
    EXPECT_TRUE(Validator::ValidateHostPortString("FlagName", "[fe80::361e:6bff:fe49:4f40%enp4s0f0]:54321"));
    EXPECT_FALSE(Validator::ValidateHostPortString("FlagName", "[fe80::361e:6bff:fe49:4f40]:54321"));
    EXPECT_FALSE(Validator::ValidateHostPortString("FlagName", "::1:65535"));
    EXPECT_FALSE(Validator::ValidateHostPortString("FlagName", "255.255.255.255: 65535"));
    EXPECT_FALSE(Validator::ValidateHostPortString("FlagName", "255.255.255.255:65535 "));
    EXPECT_FALSE(Validator::ValidateHostPortString("FlagName", "255.255.255.255:65536"));
    EXPECT_FALSE(Validator::ValidateHostPortString("FlagName", "255"));
    EXPECT_FALSE(Validator::ValidateHostPortString("FlagName", ":65535"));
    EXPECT_FALSE(Validator::ValidateHostPortString("FlagName", "255.255.255.255:"));
    EXPECT_FALSE(Validator::ValidateHostPortString("FlagName", "255.255.255.:65535"));
}

TEST_F(ValidatorTest, TestValidator2)
{
    EXPECT_EQ(Validator::ValidateInt32("FlagName", -65), false);
    EXPECT_EQ(Validator::ValidatePort("FlagName", 1), true);
    EXPECT_EQ(Validator::ValidatePathString("FlagName", "./p0/dir/1/0/"), true);
    EXPECT_EQ(Validator::ValidatePathString("FlagName", "./测试/华为/1/0/"), true);
    EXPECT_EQ(Validator::ValidatePathString("FlagName", "./@测试/!华为/1/0/"), true);
}

TEST_F(ValidatorTest, TestValidatorStrings)
{
    std::string oversizeFilename(256, 'a');
    std::string oversizePath(4096, 'b');
    std::string uuid = GetStringUuid();
    EXPECT_FALSE(Validator::ValidateStringLenNameMax("FlagName", oversizeFilename));
    EXPECT_FALSE(Validator::ValidateStringLenPathMax("FlagName", oversizePath));
    EXPECT_TRUE(Validator::ValidateUuid("FlagName", uuid));
    EXPECT_FALSE(Validator::ValidateUuid("FlagName", uuid + "01234"));
    EXPECT_FALSE(Validator::ValidateUuid("FlagName", "Qewqr&yy-wqeq-uiyg-8uyd-yuimlokjyu78"));
    EXPECT_TRUE(Validator::ValidateEligibleChar("FlagName", "~/datasystem/unix_domain_socket_dir"));
    EXPECT_TRUE(Validator::ValidateStringLenPathMax("FlagName", "~/datasystem/unix_domain_socket_dir"));
    EXPECT_TRUE(Validator::ValidatePathString("FlagName", "~/datasystem/unix_domain_socket_dir"));
}

TEST_F(ValidatorTest, TestValidatorThreadNum)
{
    EXPECT_EQ(Validator::ValidateThreadNum("threadPoolSize", -1), false);
    EXPECT_EQ(Validator::ValidateThreadNum("threadPoolSize", 2048), true);
    EXPECT_EQ(Validator::ValidateThreadNum("threadPoolSize", 4097), false);
    EXPECT_EQ(Validator::ValidateThreadNum("eviction_thread_num", static_cast<uint32_t>(4097)), false);
    EXPECT_EQ(Validator::ValidateThreadNum("spill_thread_num", static_cast<uint32_t>(4097)), false);
}

TEST_F(ValidatorTest, IsIdFormat)
{
    EXPECT_TRUE(Validator::IsIdFormat("woqednuielwhdu329UG-!@#%;"));
    EXPECT_FALSE(Validator::IsIdFormat("$tj9420j"));
}

TEST_F(ValidatorTest, IsInPortRange)
{
    std::vector<std::string> validPort {"65530", "65528", "43267", "4325"};
    std::vector<std::string> inValidPort {"65536", "3245676", "243333"};
    for (const auto &t : validPort) {
        EXPECT_TRUE(Validator::IsInPortRange(t));
    }
    for (const auto &t : inValidPort) {
        EXPECT_FALSE(Validator::IsInPortRange(t));
    }
}

TEST_F(ValidatorTest, ValidateDomainNamePort)
{
    EXPECT_TRUE(Validator::ValidateDomainNamePort("qqq", "qwdqwdUL-HUL.dbilqwu-dqHIIIL:32131"));
    EXPECT_FALSE(Validator::ValidateDomainNamePort("qqq", "-qwdqwdUL-HUL.dbilqwu-dqHIIIL:32131"));
    EXPECT_FALSE(Validator::ValidateDomainNamePort("qqq", "qwdqwdUL-HUL.dbilqwu-dqHIIIL:82131"));
}

TEST_F(ValidatorTest, TestEncryptKitFlag)
{
    ASSERT_TRUE(Validator::ValidateEncryptKit("encrypt_kit", ENCRYPT_KIT_PLAINTEXT));
    ASSERT_FALSE(Validator::ValidateEncryptKit("encrypt_kit", ""));
    ASSERT_FALSE(Validator::ValidateEncryptKit("encrypt_kit", "something"));
}

TEST_F(ValidatorTest, TestValidateIntType)
{
    ASSERT_FALSE(Validator::ValidateInt32("FlagName", 0));
    ASSERT_TRUE(Validator::ValidateInt32("FlagName", 1));

    ASSERT_FALSE(Validator::ValidateUint32("FlagName", 0));
    ASSERT_TRUE(Validator::ValidateUint32("FlagName", 1));
}

TEST_F(ValidatorTest, TestValidateArenaPerTenant)
{
    ASSERT_FALSE(Validator::ValidateArenaPerTenant("FlagName", 0));
    ASSERT_FALSE(Validator::ValidateArenaPerTenant("FlagName", 33));

    ASSERT_TRUE(Validator::ValidateArenaPerTenant("FlagName", 1));
    ASSERT_TRUE(Validator::ValidateArenaPerTenant("FlagName", 32));
}

TEST_F(ValidatorTest, TestValidateEtcdAddresses)
{
    ASSERT_FALSE(Validator::ValidateEtcdAddresses("FlagName", "255.255.255.255"));
    ASSERT_FALSE(Validator::ValidateEtcdAddresses("FlagName", "255.255.255.255:"));
    ASSERT_FALSE(Validator::ValidateEtcdAddresses("FlagName", "255.255.255:65536"));
    ASSERT_FALSE(Validator::ValidateEtcdAddresses("FlagName", "255.255.255.255:65536"));
    ASSERT_FALSE(Validator::ValidateEtcdAddresses("FlagName", "255.255.255.255: 65535"));
    ASSERT_FALSE(Validator::ValidateEtcdAddresses("FlagName", "255.255.255.255:65535 "));
    ASSERT_FALSE(Validator::ValidateEtcdAddresses("FlagName", " 255.255.255.255:65535"));
    ASSERT_FALSE(Validator::ValidateEtcdAddresses("FlagName", "255.255.255.255:65534, 255.255.255.255:65535"));

    ASSERT_TRUE(Validator::ValidateEtcdAddresses("FlagName", "0.0.0.0:0"));
    ASSERT_TRUE(Validator::ValidateEtcdAddresses("FlagName", "255.255.255.255:65535"));
    ASSERT_TRUE(Validator::ValidateEtcdAddresses("FlagName", "255.255.255.255:65534,255.255.255.255:65535"));
    ASSERT_TRUE(Validator::ValidateEtcdAddresses("FlagName", "0.0.0.0:0,0.0.0.0:1"));

    ASSERT_TRUE(Validator::ValidateEtcdAddresses("FlagName", "yr-core-etcd.default.svc.cluster.local:65535"));
    ASSERT_TRUE(
        Validator::ValidateEtcdAddresses("FlagName", "localhost:1,yr-core-etcd.default.svc.cluster.local:2379"));
    ASSERT_TRUE(Validator::ValidateEtcdAddresses(
        "FlagName", "huahuahua.com:6553,test.com:0,yr-core-etcd.default.svc.cluster.local:2379"));

    ASSERT_FALSE(Validator::ValidateEtcdAddresses("FlagName", "test:0,yr-core-etcd.default.svc.cluster.local:6"));
    ASSERT_FALSE(Validator::ValidateEtcdAddresses(
        "FlagName", "huahuahua.com:6553,test:0,yr-core-etcd.default.svc.cluster.local:65536"));
}

TEST_F(ValidatorTest, ValidateFailed)
{
    const uint32_t invalidVal = 33;
    ASSERT_FALSE(Validator::ValidateSharedDiskArenaPerTenant("", invalidVal));

    ASSERT_FALSE(Validator::ValidateEligibleChar("", "￥"));
    ASSERT_FALSE(Validator::ValidatePathString("", "￥"));
    ASSERT_FALSE(Validator::ValidateRpcThreadNum("", -1));
    ASSERT_FALSE(Validator::ValidateClientNum("", -1));
    ASSERT_FALSE(Validator::ValidateIAMKit("", "xxx"));
    ASSERT_FALSE(Validator::IsInNonNegativeInt32(INT64_MAX));
    ASSERT_FALSE(Validator::ValidateInt32(-1));
    ASSERT_FALSE(Validator::ValidateSharedMemSize("", 0));
    ASSERT_FALSE(Validator::ValidateSharedMemSize("", Validator::MB_LIMIT + 1));
    ASSERT_FALSE(Validator::ValidateSharedDiskSize("", Validator::MB_LIMIT + 1));
    ASSERT_FALSE(Validator::ValidateLocalCacheMemSize("", 0));
    ASSERT_FALSE(Validator::ValidateLocalCacheMemSize("", Validator::MB_LIMIT + 1));
    ASSERT_FALSE(Validator::ValidatePageSize("", 0));
    ASSERT_FALSE(Validator::ValidateSpillFileMaxSize("", 0));
    ASSERT_FALSE(Validator::ValidateSpillOpenFileLimit("", 0));
    ASSERT_FALSE(Validator::ValidateMaxRpcSessionNum("", 0));
    ASSERT_FALSE(Validator::ValidateUnixDomainSocketDir("", ""));
    ASSERT_FALSE(Validator::ValidateUnixDomainSocketDir("", "￥"));
}
}  // namespace ut
}  // namespace datasystem
