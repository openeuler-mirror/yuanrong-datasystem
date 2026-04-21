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
 * Description: Test interface to MetaStoreServer - a lightweight etcd-compatible service
 */

#include <cassert>
#include <string>
#include <thread>
#include <unordered_map>
#include <sstream>
#include <vector>

#include "common.h"
#include "datasystem/common/kvstore/metastore/metastore_server.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/utils/status.h"
#include "etcd/api/etcdserverpb/rpc.grpc.pb.h"
#include <grpcpp/grpcpp.h>

using namespace datasystem;

namespace datasystem {
namespace ut {

class MetaStoreServerTest : public CommonTest {
protected:
    MetaStoreServerTest() : server_(nullptr), serverPort_(0), isServerRunning_(false)
    {
    }

    ~MetaStoreServerTest() = default;

    void SetUp() override
    {
        CommonTest::SetUp();
        server_ = std::make_unique<MetaStoreServer>();
        serverPort_ = GetFreePort();
        serverAddress_ = "0.0.0.0:" + std::to_string(serverPort_);

        LOG(INFO) << "Starting MetaStoreServer on " << serverAddress_;

        // Start server in a separate thread
        serverThread_ = std::thread([this]() {
            Status rc = server_->Start(serverAddress_);
            if (!rc.IsOk()) {
                LOG(ERROR) << "Failed to start MetaStoreServer: " << rc.ToString();
            }
        });

        // Wait for server to start
        WaitForServerToStart();
    }

    void TearDown() override
    {
        if (server_ && isServerRunning_) {
            LOG(INFO) << "Stopping MetaStoreServer...";
            server_->Stop();
            isServerRunning_ = false;
        }

        if (serverThread_.joinable()) {
            serverThread_.join();
        }

        CommonTest::TearDown();
    }

    std::unique_ptr<etcdserverpb::KV::Stub> CreateKVStub() const
    {
        grpc::ChannelArguments args;
        auto channel = grpc::CreateCustomChannel("localhost:" + std::to_string(serverPort_),
            grpc::InsecureChannelCredentials(), args);

        if (!channel->WaitForConnected(std::chrono::system_clock::now() + std::chrono::seconds(5))) {
            LOG(ERROR) << "Failed to connect to MetaStoreServer on port " << serverPort_;
            return nullptr;
        }

        return etcdserverpb::KV::NewStub(channel);
    }

    std::unique_ptr<etcdserverpb::Lease::Stub> CreateLeaseStub() const
    {
        grpc::ChannelArguments args;
        auto channel = grpc::CreateCustomChannel("localhost:" + std::to_string(serverPort_),
            grpc::InsecureChannelCredentials(), args);

        if (!channel->WaitForConnected(std::chrono::system_clock::now() + std::chrono::seconds(5))) {
            LOG(ERROR) << "Failed to connect to MetaStoreServer on port " << serverPort_;
            return nullptr;
        }

        return etcdserverpb::Lease::NewStub(channel);
    }

    std::unique_ptr<etcdserverpb::Watch::Stub> CreateWatchStub() const
    {
        grpc::ChannelArguments args;
        auto channel = grpc::CreateCustomChannel("localhost:" + std::to_string(serverPort_),
            grpc::InsecureChannelCredentials(), args);

        if (!channel->WaitForConnected(std::chrono::system_clock::now() + std::chrono::seconds(5))) {
            LOG(ERROR) << "Failed to connect to MetaStoreServer on port " << serverPort_;
            return nullptr;
        }

        return etcdserverpb::Watch::NewStub(channel);
    }

protected:

    void WaitForServerToStart()
    {
        // Try to connect to server with timeout
        const int maxAttempts = 5;
        const int delayMs = 200;

        for (int i = 0; i < maxAttempts; ++i) {
            auto stub = CreateKVStub();
            if (stub) {
                LOG(INFO) << "MetaStoreServer is running and accessible";
                isServerRunning_ = true;
                return;
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));
        }

        FAIL() << "Failed to connect to MetaStoreServer after "
               << maxAttempts * delayMs << " milliseconds";
    }

    static int GetFreePort()
    {
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) {
            return 0;
        }

        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
        addr.sin_port = 0;

        if (bind(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
            close(sock);
            return 0;
        }

        socklen_t len = sizeof(addr);
        if (getsockname(sock, (struct sockaddr *)&addr, &len) < 0) {
            close(sock);
            return 0;
        }

        int port = ntohs(addr.sin_port);
        close(sock);
        return port;
    }

    std::unique_ptr<MetaStoreServer> server_;
    std::thread serverThread_;
    std::string serverAddress_;
    int serverPort_;
    bool isServerRunning_;
};

TEST_F(MetaStoreServerTest, TestServerInitialization)
{
    LOG(INFO) << "Test MetaStoreServer initialization";
    ASSERT_TRUE(server_ != nullptr);
    ASSERT_TRUE(isServerRunning_);
}

TEST_F(MetaStoreServerTest, TestPutGetKeyValue)
{
    LOG(INFO) << "Test MetaStoreServer put/get key/value";
    ASSERT_TRUE(isServerRunning_);

    auto stub = CreateKVStub();
    ASSERT_TRUE(stub != nullptr);

    grpc::ClientContext context;
    etcdserverpb::PutRequest putReq;
    putReq.set_key("test_key");
    putReq.set_value("test_value");

    etcdserverpb::PutResponse putResp;
    grpc::Status grpcStatus = stub->Put(&context, putReq, &putResp);
    EXPECT_TRUE(grpcStatus.ok()) << "Put request failed: " << grpcStatus.error_message();

    // Get the value
    grpc::ClientContext getContext;
    etcdserverpb::RangeRequest rangeReq;
    rangeReq.set_key("test_key");
    etcdserverpb::RangeResponse rangeResp;
    grpcStatus = stub->Range(&getContext, rangeReq, &rangeResp);
    EXPECT_TRUE(grpcStatus.ok()) << "Get request failed: " << grpcStatus.error_message();
    EXPECT_EQ(rangeResp.kvs_size(), 1);
    EXPECT_EQ(rangeResp.kvs(0).key(), "test_key");
    EXPECT_EQ(rangeResp.kvs(0).value(), "test_value");
}

TEST_F(MetaStoreServerTest, TestDeleteKeyValue)
{
    LOG(INFO) << "Test MetaStoreServer delete key";
    ASSERT_TRUE(isServerRunning_);

    auto stub = CreateKVStub();
    ASSERT_TRUE(stub != nullptr);

    // First, put a key
    grpc::ClientContext context;
    etcdserverpb::PutRequest putReq;
    putReq.set_key("delete_test_key");
    putReq.set_value("delete_test_value");
    etcdserverpb::PutResponse putResp;
    grpc::Status grpcStatus = stub->Put(&context, putReq, &putResp);
    EXPECT_TRUE(grpcStatus.ok()) << "Put request failed: " << grpcStatus.error_message();

    // Now delete it
    grpc::ClientContext deleteContext;
    etcdserverpb::DeleteRangeRequest deleteReq;
    deleteReq.set_key("delete_test_key");
    etcdserverpb::DeleteRangeResponse deleteResp;
    grpcStatus = stub->DeleteRange(&deleteContext, deleteReq, &deleteResp);
    EXPECT_TRUE(grpcStatus.ok()) << "Delete request failed: " << grpcStatus.error_message();
    EXPECT_EQ(deleteResp.deleted(), 1);

    // Verify it's deleted
    grpc::ClientContext getContext;
    etcdserverpb::RangeRequest rangeReq;
    rangeReq.set_key("delete_test_key");
    etcdserverpb::RangeResponse rangeResp;
    grpcStatus = stub->Range(&getContext, rangeReq, &rangeResp);
    EXPECT_TRUE(grpcStatus.ok()) << "Get request failed: " << grpcStatus.error_message();
    EXPECT_EQ(rangeResp.kvs_size(), 0);
}

TEST_F(MetaStoreServerTest, TestKeyRangeQuery)
{
    LOG(INFO) << "Test MetaStoreServer key range query";
    ASSERT_TRUE(isServerRunning_);

    auto stub = CreateKVStub();
    ASSERT_TRUE(stub != nullptr);

    // Put some keys
    for (int i = 1; i <= 5; ++i) {
        grpc::ClientContext putContext;
        etcdserverpb::PutRequest putReq;
        putReq.set_key("key" + std::to_string(i));
        putReq.set_value("value" + std::to_string(i));
        etcdserverpb::PutResponse putResp;
        grpc::Status grpcStatus = stub->Put(&putContext, putReq, &putResp);
        EXPECT_TRUE(grpcStatus.ok()) << "Put request failed: " << grpcStatus.error_message();
    }

    // Query keys from key2 to key4
    grpc::ClientContext rangeContext;
    etcdserverpb::RangeRequest rangeReq;
    rangeReq.set_key("key2");
    rangeReq.set_range_end("key5"); // exclusive
    etcdserverpb::RangeResponse rangeResp;
    grpc::Status grpcStatus = stub->Range(&rangeContext, rangeReq, &rangeResp);
    EXPECT_TRUE(grpcStatus.ok()) << "Range request failed: " << grpcStatus.error_message();
    EXPECT_EQ(rangeResp.kvs_size(), 3); // key2, key3, key4

    std::vector<std::string> expectedKeys = {"key2", "key3", "key4"};
    for (int i = 0; i < rangeResp.kvs_size(); ++i) {
        EXPECT_EQ(rangeResp.kvs(i).key(), expectedKeys[i]);
    }
}

TEST_F(MetaStoreServerTest, TestLeaseOperations)
{
    LOG(INFO) << "Test MetaStoreServer lease operations";
    ASSERT_TRUE(isServerRunning_);

    auto leaseStub = CreateLeaseStub();
    ASSERT_TRUE(leaseStub != nullptr);

    // Create a lease with TTL of 5 seconds
    grpc::ClientContext context;
    etcdserverpb::LeaseGrantRequest grantReq;
    grantReq.set_ttl(5);
    etcdserverpb::LeaseGrantResponse grantResp;
    grpc::Status grpcStatus = leaseStub->LeaseGrant(&context, grantReq, &grantResp);
    EXPECT_TRUE(grpcStatus.ok()) << "LeaseGrant request failed: " << grpcStatus.error_message();
    EXPECT_GT(grantResp.id(), 0);

    // Attach key to lease
    auto kvStub = CreateKVStub();
    grpc::ClientContext putContext;
    etcdserverpb::PutRequest putReq;
    putReq.set_key("lease_test_key");
    putReq.set_value("lease_test_value");
    putReq.set_lease(grantResp.id());
    etcdserverpb::PutResponse putResp;
    grpcStatus = kvStub->Put(&putContext, putReq, &putResp);
    EXPECT_TRUE(grpcStatus.ok()) << "Put with lease failed: " << grpcStatus.error_message();

    // Verify key exists
    grpc::ClientContext getContext;
    etcdserverpb::RangeRequest rangeReq;
    rangeReq.set_key("lease_test_key");
    etcdserverpb::RangeResponse rangeResp;
    grpcStatus = kvStub->Range(&getContext, rangeReq, &rangeResp);
    EXPECT_TRUE(grpcStatus.ok()) << "Get request failed: " << grpcStatus.error_message();
    EXPECT_EQ(rangeResp.kvs_size(), 1);
}

TEST_F(MetaStoreServerTest, TestConcurrentOperations)
{
    LOG(INFO) << "Test MetaStoreServer concurrent operations";
    ASSERT_TRUE(isServerRunning_);

    const int kThreadCount = 10;
    const int kOperationsPerThread = 100;
    std::vector<std::thread> threads;
    std::atomic<int> successfulOperations{0};
    std::atomic<int> failedOperations{0};

    // Run concurrent put/get operations
    for (int i = 0; i < kThreadCount; ++i) {
        threads.emplace_back([this, i, kOperationsPerThread, &successfulOperations, &failedOperations]() {
            auto stub = CreateKVStub();
            if (!stub) {
                LOG(ERROR) << "Failed to create stub in thread " << i;
                return;
            }

            for (int j = 0; j < kOperationsPerThread; ++j) {
                std::string key = "concurrent_key_" + std::to_string(i) + "_" + std::to_string(j);
                std::string value = "concurrent_value_" + std::to_string(i) + "_" + std::to_string(j);

                // Put operation
                grpc::ClientContext putContext;
                etcdserverpb::PutRequest putReq;
                putReq.set_key(key);
                putReq.set_value(value);
                etcdserverpb::PutResponse putResp;
                grpc::Status putStatus = stub->Put(&putContext, putReq, &putResp);

                if (putStatus.ok()) {
                    // Get operation
                    grpc::ClientContext getContext;
                    etcdserverpb::RangeRequest getReq;
                    getReq.set_key(key);
                    etcdserverpb::RangeResponse getResp;
                    grpc::Status getStatus = stub->Range(&getContext, getReq, &getResp);

                    if (getStatus.ok() && getResp.kvs_size() == 1) {
                        successfulOperations++;
                    } else {
                        failedOperations++;
                    }
                } else {
                    failedOperations++;
                }
            }
        });
    }

    // Join all threads
    for (auto& thread : threads) {
        thread.join();
    }

    LOG(INFO) << "Concurrent operations complete: " << successfulOperations << " succeeded, " << failedOperations << " failed";
    EXPECT_EQ(failedOperations, 0);
    EXPECT_EQ(successfulOperations, kThreadCount * kOperationsPerThread);
}

TEST_F(MetaStoreServerTest, TestServerStop)
{
    LOG(INFO) << "Test MetaStoreServer stop functionality";
    ASSERT_TRUE(isServerRunning_);

    // Create a client and verify connection
    auto stub = CreateKVStub();
    ASSERT_TRUE(stub != nullptr);

    grpc::ClientContext putContext;
    etcdserverpb::PutRequest putReq;
    putReq.set_key("shutdown_test_key");
    putReq.set_value("shutdown_test_value");
    etcdserverpb::PutResponse putResp;
    grpc::Status grpcStatus = stub->Put(&putContext, putReq, &putResp);
    EXPECT_TRUE(grpcStatus.ok()) << "Put request failed: " << grpcStatus.error_message();

    // Stop the server
    server_->Stop();
    isServerRunning_ = false;

    // Verify we can no longer connect
    grpc::ClientContext rangeContext;
    rangeContext.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
    etcdserverpb::RangeRequest rangeReq;
    rangeReq.set_key("shutdown_test_key");
    etcdserverpb::RangeResponse rangeResp;
    grpcStatus = stub->Range(&rangeContext, rangeReq, &rangeResp);
    EXPECT_FALSE(grpcStatus.ok());
}

TEST_F(MetaStoreServerTest, TestWatchConcurrentPuts)
{
    LOG(INFO) << "Test MetaStoreServer watch with concurrent puts";
    ASSERT_TRUE(isServerRunning_);

    auto watchStub = CreateWatchStub();
    ASSERT_TRUE(watchStub != nullptr);

    const int kThreadCount = 5;
    const int kPutsPerThread = 20;
    const int kTotalKeys = kThreadCount * kPutsPerThread;
    const std::string kKeyPrefix = "watch_test_key_";

    std::atomic<int> receivedEvents{0};
    std::unordered_map<std::string, bool> receivedKeys;
    std::mutex receivedKeysMutex;
    std::condition_variable watchDone;
    bool watchFinished = false;

    // Start watch in a separate thread
    std::thread watchThread([&]() {
        grpc::ClientContext context;
        auto stream = watchStub->Watch(&context);

        LOG(INFO) << "Watch thread started, sending watch request";

        // Send watch request
        etcdserverpb::WatchRequest watchReq;
        auto* createReq = watchReq.mutable_create_request();
        createReq->set_key(kKeyPrefix);
        // Set range_end to kKeyPrefix + one higher ASCII character to watch all keys with the prefix
        std::string rangeEnd = kKeyPrefix;
        rangeEnd.back() += 1;
        createReq->set_range_end(rangeEnd);

        if (!stream->Write(watchReq)) {
            LOG(ERROR) << "Failed to send watch request";
            return;
        }

        LOG(INFO) << "Watch request sent, waiting for events";

        etcdserverpb::WatchResponse watchResp;
        while (stream->Read(&watchResp)) {
            LOG(INFO) << "Received watch response, events size: " << watchResp.events_size();
            for (const auto& event : watchResp.events()) {
                if (event.type() == mvccpb::Event::PUT) {
                    std::lock_guard<std::mutex> lock(receivedKeysMutex);
                    receivedKeys[event.kv().key()] = true;
                    receivedEvents++;
                    LOG(INFO) << "Received watch event for key: " << event.kv().key()
                              << ", total: " << receivedEvents << "/" << kTotalKeys;
                }
            }

            // Check if we've received all expected events
            {
                std::lock_guard<std::mutex> lock(receivedKeysMutex);
                if (receivedEvents >= kTotalKeys) {
                    watchFinished = true;
                    watchDone.notify_one();
                    LOG(INFO) << "All events received, notifying main thread";
                    break;
                }
            }
        }
        LOG(INFO) << "Watch stream closed";
    });

    // Wait a little for watch to be established
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Start concurrent put operations
    std::vector<std::thread> putThreads;
    std::atomic<int> successfulPuts{0};

    for (int i = 0; i < kThreadCount; ++i) {
        putThreads.emplace_back([this, i, kPutsPerThread, kKeyPrefix, &successfulPuts]() {
            auto stub = CreateKVStub();
            if (!stub) {
                LOG(ERROR) << "Failed to create stub in thread " << i;
                return;
            }

            for (int j = 0; j < kPutsPerThread; ++j) {
                std::string key = kKeyPrefix + std::to_string(i) + "_" + std::to_string(j);
                std::string value = "value_" + std::to_string(i) + "_" + std::to_string(j);

                grpc::ClientContext putContext;
                etcdserverpb::PutRequest putReq;
                putReq.set_key(key);
                putReq.set_value(value);
                etcdserverpb::PutResponse putResp;
                grpc::Status putStatus = stub->Put(&putContext, putReq, &putResp);

                if (putStatus.ok()) {
                    successfulPuts++;
                } else {
                    LOG(ERROR) << "Put failed for key " << key << ": " << putStatus.error_message();
                }
            }
        });
    }

    // Wait for all put threads to complete
    for (auto& thread : putThreads) {
        thread.join();
    }

    LOG(INFO) << "All puts completed: " << successfulPuts << " successful out of " << kTotalKeys;
    EXPECT_EQ(successfulPuts, kTotalKeys);

    // Wait for watch to receive all events (with timeout)
    {
        std::unique_lock<std::mutex> lock(receivedKeysMutex);
        if (!watchFinished) {
            LOG(INFO) << "Waiting for events, received " << receivedEvents << "/" << kTotalKeys << " so far";
            bool timeout = !watchDone.wait_for(lock, std::chrono::seconds(15), [&watchFinished]() { return watchFinished; });
            if (timeout) {
                LOG(ERROR) << "Timeout waiting for events, received " << receivedEvents << "/" << kTotalKeys;
                // Set watchFinished to true to ensure watch thread exits
                watchFinished = true;
            }
        }
    }

    // Join the watch thread (to avoid zombie threads)
    if (watchThread.joinable()) {
        LOG(INFO) << "Joining watch thread...";
        watchThread.join();
        LOG(INFO) << "Watch thread joined";
    }

    // Verify we received all events
    LOG(INFO) << "Watch received " << receivedEvents << " events out of " << kTotalKeys;
    EXPECT_EQ(receivedEvents, kTotalKeys);

    // Verify all keys were received
    {
        std::lock_guard<std::mutex> lock(receivedKeysMutex);
        for (int i = 0; i < kThreadCount; ++i) {
            for (int j = 0; j < kPutsPerThread; ++j) {
                std::string key = kKeyPrefix + std::to_string(i) + "_" + std::to_string(j);
                EXPECT_TRUE(receivedKeys.count(key)) << "Missing key: " << key;
            }
        }
    }
}

TEST_F(MetaStoreServerTest, TestWatchTxnPuts)
{
    LOG(INFO) << "Test MetaStoreServer watch with transaction puts and deletes";
    ASSERT_TRUE(isServerRunning_);

    auto watchStub = CreateWatchStub();
    ASSERT_TRUE(watchStub != nullptr);

    auto kvStub = CreateKVStub();
    ASSERT_TRUE(kvStub != nullptr);

    const std::string kKeyPrefix = "txn_watch_test_key_";
    const int kTxnCount = 3;
    const int kPutsPerTxn = 5;
    const int kDeletesPerTxn = 2;  // Delete 2 keys per transaction after putting
    const int kTotalEvents = kTxnCount * (kPutsPerTxn + kDeletesPerTxn);

    std::atomic<int> receivedEvents{0};
    std::unordered_map<std::string, mvccpb::Event_EventType> receivedEventsMap;
    std::mutex receivedKeysMutex;
    std::condition_variable watchDone;
    bool watchFinished = false;

    // Start watch in a separate thread
    std::thread watchThread([&]() {
        grpc::ClientContext context;
        auto stream = watchStub->Watch(&context);

        LOG(INFO) << "Watch thread started, sending watch request";

        // Send watch request
        etcdserverpb::WatchRequest watchReq;
        auto* createReq = watchReq.mutable_create_request();
        createReq->set_key(kKeyPrefix);
        // Set range_end to kKeyPrefix + one higher ASCII character to watch all keys with the prefix
        std::string rangeEnd = kKeyPrefix;
        rangeEnd.back() += 1;
        createReq->set_range_end(rangeEnd);

        if (!stream->Write(watchReq)) {
            LOG(ERROR) << "Failed to send watch request";
            return;
        }

        LOG(INFO) << "Watch request sent, waiting for events";

        etcdserverpb::WatchResponse watchResp;
        while (stream->Read(&watchResp)) {
            LOG(INFO) << "Received watch response, events size: " << watchResp.events_size();
            for (const auto& event : watchResp.events()) {
                std::lock_guard<std::mutex> lock(receivedKeysMutex);
                receivedEventsMap[event.kv().key()] = event.type();
                receivedEvents++;
                std::string eventType = (event.type() == mvccpb::Event::PUT) ? "PUT" : "DELETE";
                LOG(INFO) << "Received watch event for key: " << event.kv().key()
                          << " (" << eventType << ")"
                          << ", total: " << receivedEvents << "/" << kTotalEvents;
            }

            // Check if we've received all expected events
            {
                std::lock_guard<std::mutex> lock(receivedKeysMutex);
                if (receivedEvents >= kTotalEvents) {
                    watchFinished = true;
                    watchDone.notify_one();
                    LOG(INFO) << "All events received, notifying main thread";
                    break;
                }
            }
        }
        LOG(INFO) << "Watch stream closed";
    });

    // Wait a little for watch to be established
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Perform transaction puts first
    for (int txnIdx = 0; txnIdx < kTxnCount; ++txnIdx) {
        grpc::ClientContext txnContext;
        etcdserverpb::TxnRequest txnReq;

        // Add compare operations (empty for this test)
        // Add success operations - puts
        for (int putIdx = 0; putIdx < kPutsPerTxn; ++putIdx) {
            std::string key = kKeyPrefix + std::to_string(txnIdx) + "_" + std::to_string(putIdx);
            std::string value = "txn_value_" + std::to_string(txnIdx) + "_" + std::to_string(putIdx);

            auto* reqOp = txnReq.add_success();
            auto* putReq = reqOp->mutable_request_put();
            putReq->set_key(key);
            putReq->set_value(value);
        }

        etcdserverpb::TxnResponse txnResp;
        grpc::Status grpcStatus = kvStub->Txn(&txnContext, txnReq, &txnResp);
        EXPECT_TRUE(grpcStatus.ok()) << "Txn put request failed: " << grpcStatus.error_message();
        EXPECT_TRUE(txnResp.succeeded());
        EXPECT_EQ(txnResp.responses_size(), kPutsPerTxn);
    }

    LOG(INFO) << "All transaction puts completed";

    // Now perform transaction deletes
    for (int txnIdx = 0; txnIdx < kTxnCount; ++txnIdx) {
        grpc::ClientContext txnContext;
        etcdserverpb::TxnRequest txnReq;

        // Add compare operations (empty for this test)
        // Add success operations - deletes
        for (int delIdx = 0; delIdx < kDeletesPerTxn; ++delIdx) {
            std::string key = kKeyPrefix + std::to_string(txnIdx) + "_" + std::to_string(delIdx);

            auto* reqOp = txnReq.add_success();
            auto* deleteReq = reqOp->mutable_request_delete_range();
            deleteReq->set_key(key);
        }

        etcdserverpb::TxnResponse txnResp;
        grpc::Status grpcStatus = kvStub->Txn(&txnContext, txnReq, &txnResp);
        EXPECT_TRUE(grpcStatus.ok()) << "Txn delete request failed: " << grpcStatus.error_message();
        EXPECT_TRUE(txnResp.succeeded());
        EXPECT_EQ(txnResp.responses_size(), kDeletesPerTxn);
    }

    LOG(INFO) << "All transaction deletes completed, total events: " << kTotalEvents;

    // Wait for watch to receive all events (with timeout)
    {
        std::unique_lock<std::mutex> lock(receivedKeysMutex);
        if (!watchFinished) {
            LOG(INFO) << "Waiting for events, received " << receivedEvents << "/" << kTotalEvents << " so far";
            bool timeout = !watchDone.wait_for(lock, std::chrono::seconds(15), [&watchFinished]() { return watchFinished; });
            if (timeout) {
                LOG(ERROR) << "Timeout waiting for events, received " << receivedEvents << "/" << kTotalEvents;
                // Set watchFinished to true to ensure watch thread exits
                watchFinished = true;
            }
        }
    }

    // Join the watch thread (to avoid zombie threads)
    if (watchThread.joinable()) {
        LOG(INFO) << "Joining watch thread...";
        watchThread.join();
        LOG(INFO) << "Watch thread joined";
    }

    // Verify we received all events
    LOG(INFO) << "Watch received " << receivedEvents << " events out of " << kTotalEvents;
    EXPECT_EQ(receivedEvents, kTotalEvents);

    // Verify all keys and their event types
    {
        std::lock_guard<std::mutex> lock(receivedKeysMutex);
        for (int txnIdx = 0; txnIdx < kTxnCount; ++txnIdx) {
            // Verify PUT events for all keys
            for (int putIdx = 0; putIdx < kPutsPerTxn; ++putIdx) {
                std::string key = kKeyPrefix + std::to_string(txnIdx) + "_" + std::to_string(putIdx);
                EXPECT_TRUE(receivedEventsMap.count(key)) << "Missing key: " << key;
            }
            // Verify DELETE events for the first kDeletesPerTxn keys
            for (int delIdx = 0; delIdx < kDeletesPerTxn; ++delIdx) {
                std::string key = kKeyPrefix + std::to_string(txnIdx) + "_" + std::to_string(delIdx);
                // The last event for these keys should be DELETE

                EXPECT_EQ(receivedEventsMap[key], mvccpb::Event::DELETE)
                    << "Key " << key << " should have DELETE as last event";
            }
        }
    }
}

} // namespace ut
} // namespace datasystem
