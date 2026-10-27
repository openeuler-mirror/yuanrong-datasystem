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
 * Description: AK/SK test.
 */
#include <gtest/gtest.h>
#include <atomic>
#include <thread>
#include <utility>

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/utils/sensitive_value.h"

#include "ut/common.h"

namespace datasystem {
namespace ut {
class AkSkTest : public CommonTest {
public:
    void VerifyAkSk(const std::string &clientAk, const std::string &clientSk, const std::string &serverAk,
                    const std::string &serverSk, AkSkType serverType, bool expectOk)
    {
        AkSkManager manager;
        DS_ASSERT_OK(manager.SetServerAkSk(serverType, serverAk, serverSk));
        AkSkData key;
        key.accessKey = clientAk;
        key.secretKey = SensitiveValue(clientSk);
        const std::string body("test-only-request\0bytes", 23);
        std::string signature;
        const auto digest = Digest(body);
        DS_ASSERT_OK(Signature::GetSignatureFromDigest(key, digest, signature));
        const auto timestamp = GetSystemClockTimeStampUs();
        const auto status = manager.VerifySignatureFromDigest(signature, timestamp, clientAk, digest);
        EXPECT_EQ(status.IsOk(), expectOk);
    }

protected:
    static AkSkDigest Digest(const std::string &body)
    {
        AkSkDigest digest{};
        EXPECT_NE(SHA256(reinterpret_cast<const unsigned char *>(body.data()), body.size(), digest.data()), nullptr);
        return digest;
    }

    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    std::string errorKey_ = "axfasdfasdfacxvzfasdfasdfasdfasdfasdfasdf";
};

TEST_F(AkSkTest, SimpleAkSk)
{
    VerifyAkSk(accessKey_, secretKey_, accessKey_, secretKey_, AkSkType::SYSTEM, true);
    VerifyAkSk(accessKey_, accessKey_, accessKey_, secretKey_, AkSkType::SYSTEM, false);
    VerifyAkSk(secretKey_, secretKey_, accessKey_, secretKey_, AkSkType::SYSTEM, false);
    VerifyAkSk(errorKey_, accessKey_, accessKey_, secretKey_, AkSkType::SYSTEM, false);
    VerifyAkSk(errorKey_, secretKey_, accessKey_, secretKey_, AkSkType::SYSTEM, false);
    VerifyAkSk(accessKey_, errorKey_, accessKey_, secretKey_, AkSkType::SYSTEM, false);
}

TEST_F(AkSkTest, AkSkTimeout)
{
    AkSkManager manager;
    DS_ASSERT_OK(manager.SetServerAkSk(AkSkType::SYSTEM, accessKey_, secretKey_));
    AkSkData key;
    key.accessKey = accessKey_;
    key.secretKey = SensitiveValue(secretKey_);
    const std::string body = "test-only-expired-request";
    std::string signature;
    const auto digest = Digest(body);
    DS_ASSERT_OK(Signature::GetSignatureFromDigest(key, digest, signature));
    EXPECT_EQ(manager.VerifySignatureFromDigest(signature, 1, accessKey_, digest).GetCode(),
              K_NOT_AUTHORIZED);
}

TEST_F(AkSkTest, DigestSignaturePreservesOriginalAlgorithm)
{
    AkSkData key;
    key.accessKey = "test-only-access";
    key.secretKey = SensitiveValue("test-only-secret");
    const std::pair<std::string, const char *> samples[] = {
        {"", "b855224f9174671fa804ebf99343c86f0e7059a9b23e9e758109bba35328f574"},
        {std::string("binary\0request", 14), "13ec23cac94b34d71dc06627ab15943b0ebf2a0e83d0d54eb768d6ee98eb5fbf"}
    };
    for (const auto &sample : samples) {
        std::string signature;
        DS_ASSERT_OK(Signature::GetSignatureFromDigest(key, Digest(sample.first), signature));
        EXPECT_EQ(signature, std::string(sample.second, 65));
    }
}

TEST_F(AkSkTest, ClientKeySnapshotStaysPairedDuringRotation)
{
    Signature empty;
    AkSkData initial;
    initial.secretKey = "test-only-previous";
    DS_ASSERT_OK(empty.SnapshotClientKey(initial));
    EXPECT_TRUE(initial.accessKey.empty());
    EXPECT_TRUE(initial.secretKey.Empty());
    Signature provider("test-only-access-a", SensitiveValue("test-only-secret-a"));
    std::atomic<bool> invalid{false};
    std::thread rotate([&] {
        for (unsigned i = 0; i < 1000; ++i) {
            const bool alternate = (i % 2) != 0;
            if (provider.SetClientAkSk(alternate ? "test-only-access-a" : "test-only-access-b",
                                      SensitiveValue(alternate ? "test-only-secret-a" : "test-only-secret-b")).IsError()) {
                invalid = true;
            }
        }
    });
    for (unsigned i = 0; i < 1000; ++i) {
        AkSkData snapshot;
        if (provider.SnapshotClientKey(snapshot).IsError()) {
            invalid = true;
            continue;
        }
        const std::string secret(snapshot.secretKey.GetData(), snapshot.secretKey.GetSize());
        const bool paired = (snapshot.accessKey == "test-only-access-a" && secret == "test-only-secret-a")
                            || (snapshot.accessKey == "test-only-access-b" && secret == "test-only-secret-b");
        if (!paired) {
            invalid = true;
        }
    }
    rotate.join();
    EXPECT_FALSE(invalid.load());
}

}  // namespace ut
}  // namespace datasystem
