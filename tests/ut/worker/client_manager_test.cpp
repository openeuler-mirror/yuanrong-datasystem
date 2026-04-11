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
 * Description: Test ClientManager class
 */
#include "datasystem/worker/client_manager/client_manager.h"

#include <gtest/gtest.h>
#include <sys/socket.h>

#include <atomic>
#include <chrono>
#include <string>
#include <thread>

#include "ut/common.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/rpc/unix_sock_fd.h"
#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/common/util/uuid_generator.h"

DS_DECLARE_uint32(max_client_num);

using namespace datasystem::worker;

namespace datasystem {
namespace ut {
class ClientInfoTest : public CommonTest {
};

class ClientManagerTest : public CommonTest {
};

TEST_F(ClientInfoTest, TestInvalid)
{
    int socketFd = 1;
    auto clientId = ClientKey::Intern("client1");
    bool uniqueCount = false;
    ClientInfo clientInfo(socketFd, clientId, uniqueCount);
    std::shared_ptr<ShmUnit> shmUnit;
    EXPECT_FALSE(clientInfo.RemoveShmUnit(shmUnit));
}

TEST_F(ClientManagerTest, TestAddShmUnitUniqueCount)
{
    ClientManager &clientMgr = ClientManager::Instance();
    DS_ASSERT_OK(clientMgr.Init());
    auto clientId = ClientKey::Intern(GetBytesUuid());
    int socketFd = 1;
    DS_ASSERT_OK(clientMgr.AddClient(clientId, socketFd, true));
    int getSocketFd;
    DS_ASSERT_OK(clientMgr.GetClientSocketFd(clientId, getSocketFd));
    ASSERT_TRUE(socketFd == getSocketFd);
    auto shmUnit = std::make_shared<ShmUnit>();
    shmUnit->id = ShmKey::Intern(GetBytesUuid());
    shmUnit->fd = socketFd;
    DS_ASSERT_OK(clientMgr.AddShmUnit(clientId, shmUnit));
    ASSERT_EQ(shmUnit->refCount, 1);
    DS_ASSERT_OK(clientMgr.AddShmUnit(clientId, shmUnit));
    ASSERT_EQ(shmUnit->refCount, 1);
    DS_ASSERT_OK(clientMgr.RemoveShmUnit(clientId, shmUnit));
    ASSERT_EQ(shmUnit->refCount, 0);
}

TEST_F(ClientManagerTest, TestAddShmUnit)
{
    ClientManager &clientMgr = ClientManager::Instance();
    DS_ASSERT_OK(clientMgr.Init());
    auto clientId = ClientKey::Intern(GetBytesUuid());
    int socketFd = 1;
    DS_ASSERT_OK(clientMgr.AddClient(clientId, socketFd, false));
    int getSocketFd;
    DS_ASSERT_OK(clientMgr.GetClientSocketFd(clientId, getSocketFd));
    ASSERT_TRUE(socketFd == getSocketFd);
    std::shared_ptr<ClientInfo> clientInfo = clientMgr.GetClientInfo(clientId);
    ASSERT_TRUE(clientInfo != nullptr);

    auto shmUnit = std::make_shared<ShmUnit>();
    shmUnit->id = ShmKey::Intern(GetBytesUuid());
    shmUnit->fd = socketFd;

    DS_ASSERT_OK(clientMgr.AddShmUnit(clientId, shmUnit));
    DS_ASSERT_OK(clientMgr.AddShmUnit(clientId, shmUnit));
    DS_ASSERT_OK(clientMgr.AddShmUnit(clientId, shmUnit));
    ASSERT_GE(shmUnit->refCount, 3);
    std::unordered_map<ShmKey, uint32_t> shmUnitIds;
    clientInfo->GetShmUnitIds(shmUnitIds);
    ASSERT_TRUE(shmUnitIds.find(shmUnit->id) != shmUnitIds.end());
    ASSERT_EQ(shmUnitIds[shmUnit->id], static_cast<uint32_t>(3));

    DS_ASSERT_OK(clientMgr.RemoveShmUnit(clientId, shmUnit));
    ASSERT_EQ(shmUnit->refCount, 2);
    clientInfo->GetShmUnitIds(shmUnitIds);
    ASSERT_TRUE(shmUnitIds.find(shmUnit->id) != shmUnitIds.end());
    ASSERT_EQ(shmUnitIds[shmUnit->id], size_t(2));

    DS_ASSERT_OK(clientMgr.RemoveShmUnit(clientId, shmUnit));
    DS_ASSERT_OK(clientMgr.RemoveShmUnit(clientId, shmUnit));
    ASSERT_EQ(shmUnit->refCount, 0);
    clientInfo->GetShmUnitIds(shmUnitIds);
    ASSERT_TRUE(shmUnitIds.find(shmUnit->id) == shmUnitIds.end());

    DS_ASSERT_OK(clientMgr.RemoveShmUnit(clientId, shmUnit));
    ASSERT_EQ(shmUnit->refCount, 0);
}

TEST_F(ClientManagerTest, TestSessionQuantityChange)
{
    ClientManager &clientMgr = ClientManager::Instance();
    DS_ASSERT_OK(clientMgr.Init());
    auto clientId = ClientKey::Intern(GetBytesUuid());
    int socketFd = 1;
    DS_ASSERT_OK(clientMgr.AddClient(clientId, socketFd));
    auto clientInfoPtr = clientMgr.GetClientInfo(clientId);
    EXPECT_NE(clientInfoPtr, nullptr);

    // add sessionId
    std::string readerSessionId = GetBytesUuid();
    ASSERT_TRUE(clientInfoPtr->AddReaderSessionId(readerSessionId));
    std::string writerSessionId = GetBytesUuid();
    ASSERT_TRUE(clientInfoPtr->AddWriterSessionId(writerSessionId));

    // get sessionId
    std::unordered_set<std::string> readerSessionIds;
    std::unordered_set<std::string> writerSessionIds;
    clientInfoPtr->GetReaderSessionIds(readerSessionIds);
    clientInfoPtr->GetWriterSessionIds(writerSessionIds);
    EXPECT_EQ(readerSessionIds.size(), size_t(1));
    EXPECT_EQ(writerSessionIds.size(), size_t(1));

    // remove sessionId
    DS_ASSERT_OK(clientInfoPtr->RemoveReaderSessionId(readerSessionId));
    DS_ASSERT_OK(clientInfoPtr->RemoveWriterSessionId(writerSessionId));

    // only for ut coverage
    clientInfoPtr->GetClientId();
    clientInfoPtr->SetUniqueCount(false);
}

TEST_F(ClientManagerTest, TestCheckClientId)
{
    ClientManager &clientMgr = ClientManager::Instance();
    DS_ASSERT_OK(clientMgr.Init());
    auto clientId = ClientKey::Intern(GetBytesUuid());
    int socketFd = 1;
    DS_ASSERT_OK(clientMgr.AddClient(clientId, socketFd));
    std::vector<std::string> clientIds;
    DS_ASSERT_OK(clientMgr.CheckClientId(clientId));
    DS_ASSERT_NOT_OK(clientMgr.CheckClientId(ClientKey::Intern("123344")));
}

TEST_F(ClientManagerTest, TestRemoveShmUnitOfAllClient)
{
    ClientManager &clientMgr = ClientManager::Instance();
    DS_ASSERT_OK(clientMgr.Init());
    auto clientId = ClientKey::Intern(GetBytesUuid());
    int socketFd = 1;
    DS_ASSERT_OK(clientMgr.AddClient(clientId, socketFd, true));
    int getSocketFd;
    DS_ASSERT_OK(clientMgr.GetClientSocketFd(clientId, getSocketFd));
    ASSERT_TRUE(socketFd == getSocketFd);
    auto shmUnit = std::make_shared<ShmUnit>();
    shmUnit->id = ShmKey::Intern(GetBytesUuid());
    shmUnit->fd = socketFd;
    DS_ASSERT_OK(clientMgr.AddShmUnit(clientId, shmUnit));
    ASSERT_GE(shmUnit->refCount, 1);
    DS_ASSERT_OK(clientMgr.RemoveShmUnit(shmUnit));
    ASSERT_EQ(shmUnit->refCount, 0);
}

TEST_F(ClientManagerTest, TestRunCallbackByUDSHeartbeat)
{
    ClientManager &clientMgr = ClientManager::Instance();
    DS_ASSERT_OK(clientMgr.Init());
    uint32_t lockId;
    auto clientId = ClientKey::Intern("clientId");
    // Create a socket
    std::string sockPath = "/tmp/" + GetStringUuid();
    unlink(sockPath.data());
    UnixSockFd sockFd;
    sockaddr_un addr{};
    DS_ASSERT_OK(sockFd.CreateUnixSocket());
    DS_ASSERT_OK(UnixSockFd::SetUpSockPath(sockPath, addr));
    DS_ASSERT_OK(sockFd.BindUds(addr, RPC_SOCK_MODE));
    int socketFd = sockFd.GetFd();

    // Manager a client and register a callback
    ASSERT_TRUE(clientMgr.AddClient(clientId, true, socketFd, "", "", "", lockId));
    bool called = false;
    auto callback = [&called]() {
        LOG(INFO) << "run callback";
        called = true;
    };
    ASSERT_TRUE(clientMgr.RegisterLostHandler(clientId, callback, HeartbeatType::UDS_HEARTBEAT));
    ASSERT_FALSE(called);

    int ret = shutdown(socketFd, SHUT_RDWR);
    int waitCallback = 100'000;  // 100ms, Only 1 ms is required for the local test case test.
    usleep(waitCallback);

    ASSERT_TRUE(ret == 0);
    ASSERT_TRUE(called);

    clientMgr.RemoveClient(clientId);
}

TEST_F(ClientManagerTest, DISABLED_TestRunCallbackByRPCHeartbeat)
{
    datasystem::inject::Set("ClientManager.Init.heartbeatInterval", "call(500)");
    datasystem::inject::Set("ClientManager.IsClientLost.heartbeatThreshold", "call(1)");
    ClientManager &clientMgr = ClientManager::Instance();
    DS_ASSERT_OK(clientMgr.Init());
    uint32_t lockId;
    auto clientId = ClientKey::Intern("clientId");

    // Manager a client and register a callback
    ASSERT_TRUE(clientMgr.AddClient(clientId, true, -1, "", "", "", lockId));
    std::atomic<bool> called{ false };
    auto callback = [&called]() {
        LOG(INFO) << "run callback";
        called = true;
    };
    ASSERT_TRUE(clientMgr.RegisterLostHandler(clientId, callback, HeartbeatType::RPC_HEARTBEAT));
    ASSERT_FALSE(called);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    auto clientInfo = clientMgr.GetClientInfo(clientId);
    ASSERT_TRUE(clientInfo->IsClientLost());
    ASSERT_TRUE(called);
    clientMgr.RemoveClient(clientId);
}

TEST_F(ClientManagerTest, TestAddClientFailed)
{
    FLAGS_max_client_num = 10; // max client num is 10.
    ClientManager &clientMgr = ClientManager::Instance();
    DS_ASSERT_OK(clientMgr.Init());
    auto clientId = ClientKey::Intern(GetBytesUuid());
    uint32_t lockId;
    DS_ASSERT_OK(clientMgr.AddClient(clientId, true, -1, "", true, "", lockId));
    for (int i = 0; i < 20; i++) { // retry num is 20
        auto status = clientMgr.AddClient(clientId, true, -1, "", true, "", lockId);
        ASSERT_TRUE(status.GetMsg().find("Failed to insert client") != std::string::npos) << status.GetMsg();
    }
}

TEST_F(ClientManagerTest, TestRemovableClientCount)
{
    ClientManager &clientMgr = ClientManager::Instance();
    DS_ASSERT_OK(clientMgr.Init());
    size_t count = 100;
    uint32_t lockId;
    for (size_t i = 0; i < count; ++i) {
        auto clientId = ClientKey::Intern("client_id" + std::to_string(i));
        ASSERT_TRUE(clientMgr.AddClient(clientId, true, -1, "", true, "", lockId));
    }
    for (size_t i = 0; i < count; ++i) {
        auto clientId = ClientKey::Intern("client_id" + std::to_string(i));
        ASSERT_FALSE(clientMgr.AddClient(clientId, true, -1, "", true, "", lockId));
    }
    ASSERT_EQ(clientMgr.GetClientCount(), count);

    // Set removable tag, count decrease from 100 -> 70
    size_t removableCount = 30;
    int loop = 3;
    for (int k = 0; k < loop; ++k) {
        for (size_t i = 0; i < removableCount; ++i) {
            auto clientId = ClientKey::Intern("client_id" + std::to_string(i));
            ASSERT_TRUE(clientMgr.UpdateLastHeartbeat(clientId, true));
        }
        ASSERT_EQ(clientMgr.GetClientCount(), count - removableCount);
    }

    // Set not removable tag, count increase from 70 -> 100
    for (int k = 0; k < loop; ++k) {
        for (size_t i = 0; i < removableCount; ++i) {
            auto clientId = ClientKey::Intern("client_id" + std::to_string(i));
            ASSERT_TRUE(clientMgr.UpdateLastHeartbeat(clientId, false));
        }
        ASSERT_EQ(clientMgr.GetClientCount(), count);
    }

    for (size_t i = 0; i < removableCount; ++i) {
        auto clientId = ClientKey::Intern("client_id" + std::to_string(i));
        ASSERT_TRUE(clientMgr.UpdateLastHeartbeat(clientId, true));
    }
    ASSERT_EQ(clientMgr.GetClientCount(), count - removableCount);

    // Remove removable client, count is still 70.
    for (size_t i = 0; i < removableCount; ++i) {
        auto clientId = ClientKey::Intern("client_id" + std::to_string(i));
        clientMgr.RemoveClient(clientId);
    }
    ASSERT_EQ(clientMgr.GetClientCount(), count - removableCount);

    // Remove not removable client, count decrease 70 -> 40.
    size_t decrCount = removableCount * 2;
    for (size_t i = removableCount; i < decrCount; ++i) {
        auto clientId = ClientKey::Intern("client_id" + std::to_string(i));
        clientMgr.RemoveClient(clientId);
    }
    ASSERT_EQ(clientMgr.GetClientCount(), count - decrCount);
}
}  // namespace ut
}  // namespace datasystem
