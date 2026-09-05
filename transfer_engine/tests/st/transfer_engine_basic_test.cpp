#include <gtest/gtest.h>

#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#include "internal/backend/mock_data_plane_backend.h"
#include "datasystem/transfer_engine/transfer_engine.h"

namespace datasystem {
namespace {

class ScopedEnvVar {
public:
    ScopedEnvVar(std::string key, std::string value) : key_(std::move(key))
    {
        const char *old = std::getenv(key_.c_str());
        if (old != nullptr) {
            hadOld_ = true;
            oldValue_ = old;
        }
        (void)setenv(key_.c_str(), value.c_str(), 1);
    }

    ~ScopedEnvVar()
    {
        if (hadOld_) {
            (void)setenv(key_.c_str(), oldValue_.c_str(), 1);
        } else {
            (void)unsetenv(key_.c_str());
        }
    }

private:
    std::string key_;
    bool hadOld_ = false;
    std::string oldValue_;
};

Result BatchReadOne(TransferEngine *engine, const std::string &peerHost, uint16_t peerPort, uint64_t remoteAddr,
                    uint64_t localAddr, uint64_t length)
{
    return engine->BatchTransferSyncRead(
        peerHost + ":" + std::to_string(peerPort), {static_cast<uintptr_t>(localAddr)},
        {static_cast<uintptr_t>(remoteAddr)}, {static_cast<size_t>(length)});
}

TEST(TransferEngineBasicTest, HixlProtocolSelection)
{
    auto initializeWithProtocol = [](const std::string &protocol, uint16_t port) {
        ScopedEnvVar backendOverride("TRANSFER_ENGINE_BACKEND", "");
        auto backend = std::make_shared<MockDataPlaneBackend>();
        TransferEngine engine(backend);
        return engine.Initialize("127.0.0.1:" + std::to_string(port), protocol, "npu:0");
    };

    EXPECT_TRUE(initializeWithProtocol("", 59951).IsOk());
    EXPECT_TRUE(initializeWithProtocol("ascend", 59952).IsOk());
    EXPECT_TRUE(initializeWithProtocol("hixl", 59953).IsOk());
    EXPECT_EQ(initializeWithProtocol("p2p", 59954).GetCode(), ErrorCode::kInvalid);
}

TEST(TransferEngineBasicTest, RejectsLegacyBackendOverride)
{
    ScopedEnvVar backendOverride("TRANSFER_ENGINE_BACKEND", "p2p");
    auto backend = std::make_shared<MockDataPlaneBackend>();
    TransferEngine engine(backend);
    EXPECT_EQ(engine.Initialize("127.0.0.1:59955", "hixl", "npu:0").GetCode(), ErrorCode::kInvalid);
}

TEST(TransferEngineBasicTest, SyncReadArgsInvalid)
{
    auto backend = std::make_shared<MockDataPlaneBackend>();
    TransferEngine requester(backend);
    std::vector<uint8_t> src(64, 1);
    std::vector<uint8_t> dst(64, 0);

    EXPECT_EQ(requester.Initialize("127.0.0.1:57051", "hixl", "npu:x").GetCode(), ErrorCode::kInvalid);
    ASSERT_TRUE(requester.Initialize("127.0.0.1:57052", "hixl", "npu:2").IsOk());
    EXPECT_EQ(BatchReadOne(&requester, "", 57051, reinterpret_cast<uintptr_t>(src.data()),
                           reinterpret_cast<uintptr_t>(dst.data()), dst.size())
                  .GetCode(),
              ErrorCode::kInvalid);
}

TEST(TransferEngineBasicTest, SyncReadSameDeviceMockOk)
{
    std::vector<uint8_t> src(64, 1);
    std::vector<uint8_t> dst(64, 0);
    auto sharedState = std::make_shared<MockDataPlaneBackend::SharedState>();
    TransferEngine owner(std::make_shared<MockDataPlaneBackend>(sharedState));
    TransferEngine requester(std::make_shared<MockDataPlaneBackend>(sharedState));

    ASSERT_TRUE(owner.Initialize("127.0.0.1:58051", "hixl", "npu:0").IsOk());
    ASSERT_TRUE(requester.Initialize("127.0.0.1:58052", "hixl", "npu:0").IsOk());
    ASSERT_TRUE(owner.RegisterMemory(reinterpret_cast<uintptr_t>(src.data()), src.size()).IsOk());
    Result rc = BatchReadOne(&requester, "127.0.0.1", 58051, reinterpret_cast<uintptr_t>(src.data()),
                             reinterpret_cast<uintptr_t>(dst.data()), dst.size());
    EXPECT_TRUE(rc.IsOk()) << rc.ToString();
    EXPECT_EQ(dst, src);
}

}  // namespace
}  // namespace datasystem
