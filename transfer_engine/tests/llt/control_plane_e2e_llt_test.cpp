#include <gtest/gtest.h>

#include <atomic>
#include <mutex>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "internal/control_plane/control_plane.h"

namespace datasystem {
namespace {

class FakeControlService final : public ITransferControlService {
public:
    Result ExchangeRootInfo(const ExchangeRootInfoRequest &req, ExchangeRootInfoResponse *rsp) override
    {
        if (rsp == nullptr) {
            return Result(ErrorCode::kRuntimeError, "rsp is null");
        }
        {
            std::lock_guard<std::mutex> lock(lastRequesterHostMutex);
            lastRequesterHost = req.requesterHost;
        }
        lastRootInfoSize.store(static_cast<int32_t>(req.rootInfo.size()));
        exchangeCallCount.fetch_add(1);

        rsp->code = 0;
        rsp->msg = "ok_exchange";
        rsp->ownerDeviceId = ownerDeviceId;
        return Result::OK();
    }

    Result QueryConnReady(const QueryConnReadyRequest &req, QueryConnReadyResponse *rsp) override
    {
        (void)req;
        if (rsp == nullptr) {
            return Result(ErrorCode::kRuntimeError, "rsp is null");
        }
        queryCallCount.fetch_add(1);
        rsp->code = 0;
        rsp->msg = "ok_query";
        rsp->ready = true;
        return Result::OK();
    }

    Result ReadTrigger(const ReadTriggerRequest &req, ReadTriggerResponse *rsp) override
    {
        if (rsp == nullptr) {
            return Result(ErrorCode::kRuntimeError, "rsp is null");
        }
        lastReadLength.store(static_cast<int64_t>(req.length));
        readCallCount.fetch_add(1);
        rsp->code = 0;
        rsp->msg = "ok_read";
        return Result::OK();
    }

    Result BatchReadTrigger(const BatchReadTriggerRequest &req, BatchReadTriggerResponse *rsp) override
    {
        if (rsp == nullptr) {
            return Result(ErrorCode::kRuntimeError, "rsp is null");
        }
        readCallCount.fetch_add(static_cast<int32_t>(req.items.size()));
        rsp->code = 0;
        rsp->msg = "ok_batch_read";
        rsp->failedItemIndex = -1;
        return Result::OK();
    }

    int32_t ownerDeviceId = 7;
    std::atomic<int32_t> exchangeCallCount {0};
    std::atomic<int32_t> queryCallCount {0};
    std::atomic<int32_t> readCallCount {0};
    std::atomic<int32_t> lastRootInfoSize {0};
    std::atomic<int64_t> lastReadLength {0};
    std::mutex lastRequesterHostMutex;
    std::string lastRequesterHost;
};

// 中文说明：该用例验证单线程场景下三类 RPC 的端到端收发与解析是否正确。
TEST(RpcFrameworkLltTest, SingleThreadRoundTrip)
{
    constexpr uint16_t kPort = 55101;
    auto service = std::make_shared<FakeControlService>();
    auto server = std::make_shared<SocketControlServer>();
    ASSERT_TRUE(server->Start("127.0.0.1", kPort, service).IsOk());

    SocketControlClient client;

    ExchangeRootInfoRequest exchangeReq;
    exchangeReq.requesterHost = "127.0.0.1";
    exchangeReq.requesterPort = 66001;
    exchangeReq.requesterDeviceId = 1;
    exchangeReq.ownerDeviceId = -1;
    exchangeReq.rootInfo = "root_payload";

    ExchangeRootInfoResponse exchangeRsp;
    ASSERT_TRUE(client.ExchangeRootInfo("127.0.0.1", kPort, exchangeReq, &exchangeRsp).IsOk());
    EXPECT_EQ(exchangeRsp.code, 0);
    EXPECT_EQ(exchangeRsp.msg, "ok_exchange");
    EXPECT_EQ(exchangeRsp.ownerDeviceId, 7);

    QueryConnReadyRequest queryReq;
    queryReq.requesterHost = "127.0.0.1";
    queryReq.requesterPort = 66001;
    queryReq.requesterDeviceId = 1;
    queryReq.ownerDeviceId = 7;
    QueryConnReadyResponse queryRsp;
    ASSERT_TRUE(client.QueryConnReady("127.0.0.1", kPort, queryReq, &queryRsp).IsOk());
    EXPECT_EQ(queryRsp.code, 0);
    EXPECT_EQ(queryRsp.msg, "ok_query");
    EXPECT_TRUE(queryRsp.ready);

    ReadTriggerRequest readReq;
    readReq.requestId = 42;
    readReq.requesterHost = "127.0.0.1";
    readReq.requesterPort = 66001;
    readReq.requesterDeviceId = 1;
    readReq.ownerDeviceId = 7;
    readReq.remoteAddr = 0x1000;
    readReq.length = 4096;
    ReadTriggerResponse readRsp;
    ASSERT_TRUE(client.ReadTrigger("127.0.0.1", kPort, readReq, &readRsp).IsOk());
    EXPECT_EQ(readRsp.code, 0);
    EXPECT_EQ(readRsp.msg, "ok_read");

    EXPECT_EQ(service->exchangeCallCount.load(), 1);
    EXPECT_EQ(service->queryCallCount.load(), 1);
    EXPECT_EQ(service->readCallCount.load(), 1);
    EXPECT_EQ(service->lastRootInfoSize.load(), 12);
    EXPECT_EQ(service->lastReadLength.load(), 4096);

    server->Stop();
}

// 中文说明：该用例验证并发场景下多客户端同时发起 QueryConnReady 时，RPC 框架收发与解析稳定性。
TEST(RpcFrameworkLltTest, ConcurrentQueryConnReady)
{
    constexpr uint16_t kPort = 55102;
    constexpr int kThreadCount = 16;
    constexpr int kReqPerThread = 100;
    auto service = std::make_shared<FakeControlService>();
    auto server = std::make_shared<SocketControlServer>();
    ASSERT_TRUE(server->Start("127.0.0.1", kPort, service).IsOk());

    std::atomic<int32_t> successCount {0};
    std::vector<std::thread> workers;
    workers.reserve(kThreadCount);

    for (int t = 0; t < kThreadCount; ++t) {
        workers.emplace_back([&]() {
            SocketControlClient client;
            for (int i = 0; i < kReqPerThread; ++i) {
                QueryConnReadyRequest req;
                req.requesterHost = "127.0.0.1";
                req.requesterPort = 66002;
                req.requesterDeviceId = 2;
                req.ownerDeviceId = 7;
                QueryConnReadyResponse rsp;
                Result rc = client.QueryConnReady("127.0.0.1", kPort, req, &rsp);
                if (rc.IsOk() && rsp.code == 0 && rsp.ready) {
                    successCount.fetch_add(1);
                }
            }
        });
    }

    for (auto &th : workers) {
        th.join();
    }

    EXPECT_EQ(successCount.load(), kThreadCount * kReqPerThread);
    EXPECT_EQ(service->queryCallCount.load(), kThreadCount * kReqPerThread);

    server->Stop();
}

// 中文说明：该用例验证消息解析边界，确保包含 '\\0' 的二进制 root_info 也能被完整传输与解析。
TEST(RpcFrameworkLltTest, BinaryRootInfoParsed)
{
    constexpr uint16_t kPort = 55103;
    auto service = std::make_shared<FakeControlService>();
    auto server = std::make_shared<SocketControlServer>();
    ASSERT_TRUE(server->Start("127.0.0.1", kPort, service).IsOk());

    std::string binaryRootInfo;
    binaryRootInfo.resize(1024);
    for (size_t i = 0; i < binaryRootInfo.size(); ++i) {
        binaryRootInfo[i] = static_cast<char>(i % 256);
    }
    binaryRootInfo[17] = '\0';
    binaryRootInfo[513] = '\0';

    SocketControlClient client;
    ExchangeRootInfoRequest req;
    req.requesterHost = "127.0.0.1";
    req.requesterPort = 66003;
    req.requesterDeviceId = 3;
    req.ownerDeviceId = -1;
    req.rootInfo = binaryRootInfo;

    ExchangeRootInfoResponse rsp;
    ASSERT_TRUE(client.ExchangeRootInfo("127.0.0.1", kPort, req, &rsp).IsOk());
    EXPECT_EQ(rsp.code, 0);
    EXPECT_EQ(service->lastRootInfoSize.load(), 1024);

    server->Stop();
}

}  // namespace
}  // namespace datasystem
