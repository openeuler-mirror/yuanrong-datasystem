/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 */
#include <gtest/gtest.h>

#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#include <hixl/hixl.h>

#include "internal/backend/ascend/ascend_backend.h"

namespace datasystem {
namespace {

constexpr uint16_t K_CONTROL_PORT = 55400;
constexpr size_t K_CONNECTION_LIMIT = 4096;
constexpr uint64_t K_BUFFER_ADDRESS = 0x100000;
constexpr uint64_t K_BUFFER_LENGTH = 0x100;

class AscendConnectionsLltTest : public ::testing::TestWithParam<bool> {
public:
    ~AscendConnectionsLltTest() override = default;

protected:
    void SetUp() override
    {
        SaveEnv("YR_TE_HIXL_ENDPOINT", "127.0.0.1:55401");
        SaveEnv("YR_TE_HIXL_ROUTE", "roce");
        SaveEnv("YR_TE_HIXL_AUTO_CONNECT", GetParam() ? "on" : "off");
        hixl::fake = hixl::FakeState{};
        backend_ = std::make_unique<AscendBackend>();
        ASSERT_TRUE(backend_->InitializeLocal("127.0.0.1", K_CONTROL_PORT, 0).IsOk());
    }

    void TearDown() override
    {
        backend_.reset();
        for (const auto &entry : savedEnv_) {
            if (entry.present) {
                setenv(entry.name.c_str(), entry.value.c_str(), 1);
            } else {
                unsetenv(entry.name.c_str());
            }
        }
    }

    ConnectionSpec Spec(const std::string &peer) const
    {
        return ConnectionSpec{ "127.0.0.1", K_CONTROL_PORT, 0, peer, K_CONTROL_PORT, 0 };
    }

    Result Read(const ConnectionSpec &spec)
    {
        return backend_->TransferSyncRead(spec,
                                          { TransferReadOp{ K_BUFFER_ADDRESS, K_BUFFER_ADDRESS, K_BUFFER_LENGTH } }, 0);
    }

    std::unique_ptr<AscendBackend> backend_;

private:
    struct SavedEnv {
        std::string name;
        std::string value;
        bool present;
    };

    void SaveEnv(const char *name, const char *value)
    {
        const char *previous = std::getenv(name);
        savedEnv_.push_back(SavedEnv{ name, previous == nullptr ? "" : previous, previous != nullptr });
        setenv(name, value, 1);
    }

    std::vector<SavedEnv> savedEnv_;
};

TEST_P(AscendConnectionsLltTest, AbortOneAliasPreservesOtherAlias)
{
    const auto first = Spec("first");
    const auto second = Spec("second");
    ASSERT_TRUE(backend_->InitRecv(first, "owner-endpoint").IsOk());
    ASSERT_TRUE(backend_->InitRecv(second, "owner-endpoint").IsOk());
    ASSERT_TRUE(Read(first).IsOk());
    const auto disconnects = hixl::fake.disconnectCalls;
    backend_->AbortConnection(first);
    EXPECT_EQ(hixl::fake.disconnectCalls, disconnects);
    EXPECT_FALSE(backend_->IsConnectionReady(first));
    EXPECT_TRUE(backend_->IsConnectionReady(second));
    EXPECT_TRUE(Read(second).IsOk());
    EXPECT_EQ(Read(first).GetCode(), ErrorCode::kNotReady);
}

TEST_P(AscendConnectionsLltTest, FailedTransferInvalidatesEveryAliasWithModeSpecificCleanup)
{
    const auto first = Spec("first");
    const auto second = Spec("second");
    ASSERT_TRUE(backend_->InitRecv(first, "owner-endpoint").IsOk());
    ASSERT_TRUE(backend_->InitRecv(second, "owner-endpoint").IsOk());
    hixl::fake.transferResult = hixl::TIMEOUT;
    hixl::fake.disconnectResult = hixl::TIMEOUT;
    const auto disconnects = hixl::fake.disconnectCalls;
    EXPECT_EQ(Read(first).GetCode(), ErrorCode::kNotReady);
    EXPECT_EQ(hixl::fake.disconnectCalls, disconnects + (GetParam() ? 0 : 1));
    if (GetParam()) {
        EXPECT_EQ(hixl::fake.connections.count("owner-endpoint"), 0);
    }
    EXPECT_FALSE(backend_->IsConnectionReady(first));
    EXPECT_FALSE(backend_->IsConnectionReady(second));
    const auto transfers = hixl::fake.transferCalls;
    EXPECT_EQ(Read(second).GetCode(), ErrorCode::kNotReady);
    EXPECT_EQ(hixl::fake.transferCalls, transfers);
    const auto connects = hixl::fake.connectCalls;
    if (GetParam()) {
        EXPECT_TRUE(backend_->InitRecv(second, "owner-endpoint").IsOk());
        EXPECT_EQ(hixl::fake.disconnectCalls, disconnects);
    } else {
        EXPECT_EQ(backend_->InitRecv(second, "owner-endpoint").GetCode(), ErrorCode::kNotReady);
        EXPECT_EQ(hixl::fake.connectCalls, connects);
    }
    hixl::fake.disconnectResult = hixl::SUCCESS;
    hixl::fake.transferResult = hixl::SUCCESS;
    EXPECT_TRUE(backend_->InitRecv(second, "owner-endpoint").IsOk());
    EXPECT_TRUE(Read(second).IsOk());
}

TEST_P(AscendConnectionsLltTest, AbortBeforeFirstTransferAcceptsNotConnected)
{
    if (!GetParam()) {
        GTEST_SKIP() << "explicit-connect mode creates the vendor connection during InitRecv";
    }
    const auto spec = Spec("owner");
    ASSERT_TRUE(backend_->InitRecv(spec, "owner-endpoint").IsOk());
    ASSERT_EQ(hixl::fake.connections.count("owner-endpoint"), 0);
    const auto disconnects = hixl::fake.disconnectCalls;
    backend_->AbortConnection(spec);
    EXPECT_EQ(hixl::fake.disconnectCalls, disconnects + 1);
    EXPECT_FALSE(backend_->IsConnectionReady(spec));
    EXPECT_TRUE(backend_->InitRecv(spec, "owner-endpoint").IsOk());
    EXPECT_TRUE(backend_->IsConnectionReady(spec));
}

TEST_P(AscendConnectionsLltTest, RegistrationChangeCannotReuseFailedDisconnect)
{
    const auto spec = Spec("owner");
    ASSERT_TRUE(backend_->InitRecv(spec, "owner-endpoint").IsOk());
    ASSERT_TRUE(Read(spec).IsOk());
    hixl::fake.disconnectResult = hixl::TIMEOUT;
    ASSERT_TRUE(backend_->RegisterLocalMemory(K_BUFFER_ADDRESS, K_BUFFER_LENGTH).IsOk());
    EXPECT_EQ(Read(spec).GetCode(), ErrorCode::kNotReady);
    const auto connects = hixl::fake.connectCalls;
    EXPECT_EQ(backend_->InitRecv(spec, "owner-endpoint").GetCode(), ErrorCode::kNotReady);
    EXPECT_EQ(hixl::fake.connectCalls, connects);
    hixl::fake.disconnectResult = hixl::SUCCESS;
    EXPECT_TRUE(backend_->InitRecv(spec, "owner-endpoint").IsOk());
    EXPECT_TRUE(Read(spec).IsOk());
}

TEST_P(AscendConnectionsLltTest, PeerGenerationInvalidationClosesEveryAlias)
{
    const auto first = Spec("first");
    const auto second = Spec("second");
    ASSERT_TRUE(backend_->InitRecv(first, "owner-endpoint").IsOk());
    ASSERT_TRUE(backend_->InitRecv(second, "owner-endpoint").IsOk());
    ASSERT_TRUE(Read(first).IsOk());
    const auto disconnects = hixl::fake.disconnectCalls;
    backend_->InvalidatePeerConnections(first);
    EXPECT_EQ(hixl::fake.disconnectCalls, disconnects + 1);
    EXPECT_FALSE(backend_->IsConnectionReady(first));
    EXPECT_FALSE(backend_->IsConnectionReady(second));
    EXPECT_TRUE(backend_->InitRecv(first, "owner-endpoint").IsOk());
    EXPECT_TRUE(Read(first).IsOk());
    EXPECT_FALSE(backend_->IsConnectionReady(second));
}

TEST_P(AscendConnectionsLltTest, CapacityRejectsNewConnectionsWithoutEvictingExisting)
{
    for (size_t i = 0; i < K_CONNECTION_LIMIT; ++i) {
        ASSERT_TRUE(backend_->InitRecv(Spec("peer-" + std::to_string(i)), "endpoint-" + std::to_string(i)).IsOk());
    }
    EXPECT_EQ(backend_->InitRecv(Spec("overflow"), "overflow-endpoint").GetCode(), ErrorCode::kNotReady);
    EXPECT_TRUE(Read(Spec("peer-0")).IsOk());
    backend_->AbortConnection(Spec("peer-0"));
    EXPECT_TRUE(backend_->InitRecv(Spec("replacement"), "replacement-endpoint").IsOk());
}

INSTANTIATE_TEST_SUITE_P(ConnectionModes, AscendConnectionsLltTest, ::testing::Bool());

}  // namespace
}  // namespace datasystem
