/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 */
#include <gtest/gtest.h>

#include <unistd.h>

#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#include "datasystem/transfer_engine/transfer_engine.h"
#include "internal/control_plane/socket_rpc_transport.h"

namespace datasystem {
namespace {

class RpcPortEnvGuard final {
public:
    RpcPortEnvGuard(const char *minValue, const char *maxValue)
    {
        SaveAndSet("YR_TE_RPC_PORT_MIN", minValue, minWasSet_, minOld_);
        SaveAndSet("YR_TE_RPC_PORT_MAX", maxValue, maxWasSet_, maxOld_);
    }

    ~RpcPortEnvGuard()
    {
        Restore("YR_TE_RPC_PORT_MIN", minWasSet_, minOld_);
        Restore("YR_TE_RPC_PORT_MAX", maxWasSet_, maxOld_);
    }

    RpcPortEnvGuard(const RpcPortEnvGuard &) = delete;
    RpcPortEnvGuard &operator=(const RpcPortEnvGuard &) = delete;

private:
    static void SaveAndSet(const char *name, const char *value, bool &wasSet, std::string &old)
    {
        const char *current = std::getenv(name);
        wasSet = current != nullptr;
        if (wasSet) {
            old = current;
        }
        if (value == nullptr) {
            ::unsetenv(name);
        } else {
            ::setenv(name, value, 1);
        }
    }

    static void Restore(const char *name, bool wasSet, const std::string &old)
    {
        if (wasSet) {
            ::setenv(name, old.c_str(), 1);
        } else {
            ::unsetenv(name);
        }
    }

    bool minWasSet_ = false;
    bool maxWasSet_ = false;
    std::string minOld_;
    std::string maxOld_;
};

class InitializationBackend final : public IDataPlaneBackend {
public:
    ~InitializationBackend() override = default;

    bool RequiresAclRuntime() const override
    {
        return false;
    }

    Result InitializeLocal(const std::string &localHost, uint16_t localPort, int32_t localDeviceId) override
    {
        initializedHost = localHost;
        initializedPort = localPort;
        initializedDeviceId = localDeviceId;
        ++initializeCount;
        return initializeResult;
    }

    void FinalizeLocal() override
    {
        ++finalizeCount;
    }

    Result CreateRootInfo(std::string *rootInfoBytes) override
    {
        if (rootInfoBytes != nullptr) {
            *rootInfoBytes = "root";
        }
        return Result::OK();
    }

    Result InitRecv(const ConnectionSpec &, const std::string &) override
    {
        return Result::OK();
    }

    Result InitSend(const ConnectionSpec &, const std::string &) override
    {
        return Result::OK();
    }

    Result PostRecv(const ConnectionSpec &, uint64_t, uint64_t) override
    {
        return Result::OK();
    }

    Result PostSend(const ConnectionSpec &, uint64_t, uint64_t) override
    {
        return Result::OK();
    }

    Result WaitRecv(const ConnectionSpec &, uint64_t) override
    {
        return Result::OK();
    }

    Result initializeResult = Result::OK();
    std::string initializedHost;
    uint16_t initializedPort = 0;
    int32_t initializedDeviceId = -1;
    int initializeCount = 0;
    int finalizeCount = 0;
};

TEST(TransferEngineInitializationLltTest, DynamicPortIsBoundBeforeBackendInitialization)
{
    auto backend = std::make_shared<InitializationBackend>();
    TransferEngine engine(backend);

    ASSERT_TRUE(engine.Initialize("127.0.0.1:0", "ascend", "npu:3").IsOk());
    const int32_t rpcPort = engine.GetRpcPort();
    ASSERT_GT(rpcPort, 0);
    EXPECT_EQ(backend->initializedHost, "127.0.0.1");
    EXPECT_EQ(backend->initializedPort, rpcPort);
    EXPECT_EQ(backend->initializedDeviceId, 3);

    int fd = -1;
    EXPECT_TRUE(ConnectTo("127.0.0.1", static_cast<uint16_t>(rpcPort), &fd, 1000).IsOk());
    if (fd >= 0) {
        close(fd);
    }
    EXPECT_TRUE(engine.Finalize().IsOk());
}

TEST(TransferEngineInitializationLltTest, DynamicPortsDoNotConflictAcrossInstances)
{
    auto firstBackend = std::make_shared<InitializationBackend>();
    auto secondBackend = std::make_shared<InitializationBackend>();
    TransferEngine first(firstBackend);
    TransferEngine second(secondBackend);

    ASSERT_TRUE(first.Initialize("127.0.0.1:0", "ascend", "npu:0").IsOk());
    ASSERT_TRUE(second.Initialize("127.0.0.1:0", "ascend", "npu:1").IsOk());
    EXPECT_GT(first.GetRpcPort(), 0);
    EXPECT_GT(second.GetRpcPort(), 0);
    EXPECT_NE(first.GetRpcPort(), second.GetRpcPort());
    EXPECT_TRUE(second.Finalize().IsOk());
    EXPECT_TRUE(first.Finalize().IsOk());
}

TEST(TransferEngineInitializationLltTest, ExplicitPortRemainsUnchanged)
{
    auto dynamicBackend = std::make_shared<InitializationBackend>();
    TransferEngine dynamicEngine(dynamicBackend);
    ASSERT_TRUE(dynamicEngine.Initialize("127.0.0.1:0", "ascend", "npu:0").IsOk());
    const int32_t selectedPort = dynamicEngine.GetRpcPort();
    ASSERT_GT(selectedPort, 0);
    ASSERT_TRUE(dynamicEngine.Finalize().IsOk());

    auto explicitBackend = std::make_shared<InitializationBackend>();
    TransferEngine explicitEngine(explicitBackend);
    ASSERT_TRUE(explicitEngine.Initialize("127.0.0.1:" + std::to_string(selectedPort), "ascend", "npu:0").IsOk());
    EXPECT_EQ(explicitEngine.GetRpcPort(), selectedPort);
    EXPECT_EQ(explicitBackend->initializedPort, selectedPort);
    EXPECT_TRUE(explicitEngine.Finalize().IsOk());
}

TEST(TransferEngineInitializationLltTest, PortRangeConstrainsDynamicallyAssignedPort)
{
    constexpr int kRangeMin = 25000;
    constexpr int kRangeMax = 25099;
    RpcPortEnvGuard guard("25000", "25099");

    auto backend = std::make_shared<InitializationBackend>();
    TransferEngine engine(backend);

    ASSERT_TRUE(engine.Initialize("127.0.0.1:0", "ascend", "npu:0").IsOk());
    const int32_t rpcPort = engine.GetRpcPort();
    EXPECT_GE(rpcPort, kRangeMin);
    EXPECT_LE(rpcPort, kRangeMax);
    EXPECT_EQ(backend->initializedPort, rpcPort);

    int fd = -1;
    EXPECT_TRUE(ConnectTo("127.0.0.1", static_cast<uint16_t>(rpcPort), &fd, 1000).IsOk());
    if (fd >= 0) {
        close(fd);
    }
    EXPECT_TRUE(engine.Finalize().IsOk());
}

TEST(TransferEngineInitializationLltTest, InvalidPortRangeFallsBackToOsAssignment)
{
    {
        RpcPortEnvGuard guard("20000", "10000");
        auto backend = std::make_shared<InitializationBackend>();
        TransferEngine engine(backend);
        ASSERT_TRUE(engine.Initialize("127.0.0.1:0", "ascend", "npu:0").IsOk());
        EXPECT_GT(engine.GetRpcPort(), 0);
        EXPECT_TRUE(engine.Finalize().IsOk());
    }
    {
        RpcPortEnvGuard guard("20000", nullptr);
        auto backend = std::make_shared<InitializationBackend>();
        TransferEngine engine(backend);
        ASSERT_TRUE(engine.Initialize("127.0.0.1:0", "ascend", "npu:0").IsOk());
        EXPECT_GT(engine.GetRpcPort(), 0);
        EXPECT_TRUE(engine.Finalize().IsOk());
    }
    {
        RpcPortEnvGuard guard("40000", "40099");
        auto backend = std::make_shared<InitializationBackend>();
        TransferEngine engine(backend);
        ASSERT_TRUE(engine.Initialize("127.0.0.1:0", "ascend", "npu:0").IsOk());
        EXPECT_GT(engine.GetRpcPort(), 0);
        EXPECT_TRUE(engine.Finalize().IsOk());
    }
    {
        // Misspelled values must not take effect as truncated ports.
        RpcPortEnvGuard guard("25000abc", "25099");
        auto backend = std::make_shared<InitializationBackend>();
        TransferEngine engine(backend);
        ASSERT_TRUE(engine.Initialize("127.0.0.1:0", "ascend", "npu:0").IsOk());
        EXPECT_GT(engine.GetRpcPort(), 0);
        EXPECT_TRUE(engine.Finalize().IsOk());
    }
    {
        RpcPortEnvGuard guard("", "25099");
        auto backend = std::make_shared<InitializationBackend>();
        TransferEngine engine(backend);
        ASSERT_TRUE(engine.Initialize("127.0.0.1:0", "ascend", "npu:0").IsOk());
        EXPECT_GT(engine.GetRpcPort(), 0);
        EXPECT_TRUE(engine.Finalize().IsOk());
    }
    {
        RpcPortEnvGuard guard("99999999999", "25099");
        auto backend = std::make_shared<InitializationBackend>();
        TransferEngine engine(backend);
        ASSERT_TRUE(engine.Initialize("127.0.0.1:0", "ascend", "npu:0").IsOk());
        EXPECT_GT(engine.GetRpcPort(), 0);
        EXPECT_TRUE(engine.Finalize().IsOk());
    }
}

TEST(TransferEngineInitializationLltTest, ExplicitPortBypassesPortRange)
{
    RpcPortEnvGuard guard("25000", "25099");

    int listenFd = -1;
    ASSERT_TRUE(CreateListenSocket("127.0.0.1", 0, 1, listenFd).IsOk());
    uint16_t freePort = 0;
    ASSERT_TRUE(GetSocketLocalPort(listenFd, &freePort).IsOk());
    close(listenFd);

    auto backend = std::make_shared<InitializationBackend>();
    TransferEngine engine(backend);
    ASSERT_TRUE(engine.Initialize("127.0.0.1:" + std::to_string(freePort), "ascend", "npu:0").IsOk());
    EXPECT_EQ(engine.GetRpcPort(), static_cast<int32_t>(freePort));
    EXPECT_EQ(backend->initializedPort, freePort);
    EXPECT_TRUE(engine.Finalize().IsOk());
}

TEST(TransferEngineInitializationLltTest, BackendFailureReleasesDynamicPortAndRollsBack)
{
    auto backend = std::make_shared<InitializationBackend>();
    backend->initializeResult = Result(ErrorCode::kRuntimeError, "injected initialize failure");
    TransferEngine engine(backend);

    Result initRc = engine.Initialize("127.0.0.1:0", "ascend", "npu:0");
    EXPECT_EQ(initRc.GetCode(), ErrorCode::kRuntimeError);
    ASSERT_GT(backend->initializedPort, 0);
    EXPECT_EQ(backend->initializeCount, 1);
    EXPECT_EQ(backend->finalizeCount, 1);
    EXPECT_EQ(engine.GetRpcPort(), -1);

    int listenFd = -1;
    EXPECT_TRUE(CreateListenSocket("127.0.0.1", backend->initializedPort, 1, listenFd).IsOk());
    if (listenFd >= 0) {
        close(listenFd);
    }
}

}  // namespace
}  // namespace datasystem
