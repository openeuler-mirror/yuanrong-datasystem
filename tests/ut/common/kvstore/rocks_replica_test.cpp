/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: Test Rocksdb Replica
 */

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/kvstore/rocksdb/replica.h"

#include <string>
#include <utility>

#include "common.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace ut {
class RocksReplicaTest : public CommonTest {};

class ReplicaRpcChannelMock : public ReplicaRpcChannel {
public:
    ReplicaRpcChannelMock(Replica *primary, Replica *backup) : primary_(primary), backup_(backup)
    {
    }
    Status TryPSync(const std::string &targetNodeId, const std::string &dbName, const std::string &backupNodeId,
                    rocksdb::SequenceNumber seq, const std::string &replicaId)
    {
        (void)targetNodeId;
        (void)dbName;
        INJECT_POINT("TryPSyncFailed");
        return primary_->HandleTryPSync(backupNodeId, seq, replicaId);
    }

    Status PushNewLogs(const std::string &targetNodeId, const std::string &dbName, PushLogAction action,
                       const std::vector<std::string> &logs)
    {
        (void)targetNodeId;
        (void)dbName;
        return backup_->ApplyLogs(std::make_pair(action, logs));
    }

    Status FetchMeta(const std::string &targetNodeId, const std::string &dbName, const std::string &backupNodeId,
                     std::vector<std::string> &fileList)
    {
        (void)targetNodeId;
        (void)dbName;
        return primary_->HandleFetchMeta(backupNodeId, fileList);
    }

    Status FetchFile(const std::string &targetNodeId, const std::string &dbName, const std::string &backupNodeId,
                     const std::string &file, const uint64_t offset, bool &isFinish, std::string &data,
                     uint32_t &crc32Calc)
    {
        (void)targetNodeId;
        (void)dbName;
        return primary_->HandleFetchFile(backupNodeId, file, offset, isFinish, data, crc32Calc);
    }

private:
    Replica *primary_;
    Replica *backup_;
};

TEST_F(RocksReplicaTest, TestPSync)
{
    auto destPath = GetTestCaseDataDir();
    Replica primary("db1", destPath + "/node1", "node1", nullptr, true);
    Replica backup("db1", destPath + "/node2", "node2", nullptr, true);
    DS_ASSERT_OK(primary.Init());
    DS_ASSERT_OK(backup.Init());
    primary.SetReplicaType(ReplicaType::Primary);
    backup.SetReplicaType(ReplicaType::Backup);
    ReplicaRpcChannelMock mockChannel(&primary, &backup);
    primary.RegisterRpcChannel(&mockChannel);
    backup.RegisterRpcChannel(&mockChannel);

    auto masterStore = primary.GetObjectRocksStore();
    DS_ASSERT_OK(masterStore->CreateTable("table"));
    auto followerStore = backup.GetObjectRocksStore();
    DS_ASSERT_OK(followerStore->CreateTable("table"));
    // Start PSync
    backup.AddPrimary("node1");

    DS_ASSERT_OK(masterStore->Put("table", "key", "value"));

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));
    std::string value;
    DS_ASSERT_OK(followerStore->Get("table", "key", value));
    ASSERT_EQ(value, "value");
}

TEST_F(RocksReplicaTest, TestPSyncRemoveRock)
{
    auto destPath = GetTestCaseDataDir();
    Replica primary("db1", destPath + "/node1", "node1", nullptr, true);
    Replica backup("db1", destPath + "/node2", "node2", nullptr, true);
    DS_ASSERT_OK(primary.Init());
    DS_ASSERT_OK(backup.Init());
    primary.SetReplicaType(ReplicaType::Primary);
    backup.SetReplicaType(ReplicaType::Backup);
    ReplicaRpcChannelMock mockChannel(&primary, &backup);
    primary.RegisterRpcChannel(&mockChannel);
    backup.RegisterRpcChannel(&mockChannel);

    auto masterStore = primary.GetObjectRocksStore();
    DS_ASSERT_OK(masterStore->CreateTable("table"));
    auto followerStore = backup.GetObjectRocksStore();
    DS_ASSERT_OK(followerStore->CreateTable("table"));

    // Start PSync
    backup.AddPrimary("node1");

    DS_ASSERT_OK(masterStore->Put("table", "key", "value"));
    LOG(INFO) << "remove db path" << destPath + "/node2/replicas/db1";
    RemoveAll(destPath + "/node2/replicas/db1");
    Timer timer;
    int waitTime = 10;
    while (timer.ElapsedSecond() < waitTime) {
        if (FileExist(destPath + "/node2/replicas/db1")) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));  // interval is 500ms
    }
    timer.Reset();
    std::vector<std::string> tables;
    while (timer.ElapsedSecond() < waitTime) {
        followerStore = backup.GetObjectRocksStore();
        if (followerStore != nullptr) {
            auto status = followerStore->ListTables(tables);
            if (status.IsOk()) {
                break;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));  // interval is 100ms
    }
    ASSERT_TRUE(!tables.empty());
}

TEST_F(RocksReplicaTest, TestFullSync)
{
    DS_ASSERT_OK(inject::Set("worker.HandleFetchFile.fetchSize", "call(10240)"));
    auto destPath = GetTestCaseDataDir();
    Replica primary("db1", destPath + "/node1", "node1", nullptr, true);
    Replica backup("db1", destPath + "/node2", "node2", nullptr, true);
    DS_ASSERT_OK(primary.Init());
    DS_ASSERT_OK(backup.Init());
    primary.SetReplicaType(ReplicaType::Primary);
    backup.SetReplicaType(ReplicaType::Backup);
    ReplicaRpcChannelMock mockChannel(&primary, &backup);
    primary.RegisterRpcChannel(&mockChannel);
    backup.RegisterRpcChannel(&mockChannel);

    auto masterStore = primary.GetObjectRocksStore();
    DS_ASSERT_OK(masterStore->CreateTable("table"));
    int logNum = 100000;
    for (int i = 0; i < logNum; i++) {
        std::string key = "key_" + std::to_string(i);
        DS_ASSERT_OK(masterStore->Put("table", key, "value"));
    }

    int waitWriteLog = 1000;
    std::this_thread::sleep_for(std::chrono::milliseconds(waitWriteLog));
    inject::Set("TryPSyncFailed", "1*return(K_NOT_FOUND)");
    // Start FullSync
    backup.AddPrimary("node1");
    int waitFullSync = 5000;
    std::this_thread::sleep_for(std::chrono::milliseconds(waitFullSync));
    std::string value;
    auto followerStore = backup.GetObjectRocksStore();
    DS_ASSERT_OK(followerStore->Get("table", "key_0", value));
    ASSERT_EQ(value, "value");
}
}  // namespace ut
}  // namespace datasystem
