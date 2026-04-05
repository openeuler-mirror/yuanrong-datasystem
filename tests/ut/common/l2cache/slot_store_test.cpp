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
 * Description: White-box tests for slot storage.
 */

#include <functional>
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include <fcntl.h>
#include <unistd.h>

#include <chrono>

#include <gtest/gtest.h>

#include "../../../common/binmock/binmock.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/l2cache/persistence_api.h"
#include "datasystem/common/l2cache/slot_client/slot_compactor.h"
#include "datasystem/common/l2cache/slot_client/slot_file_util.h"
#include "datasystem/common/l2cache/slot_client/slot_index_codec.h"
#include "datasystem/common/l2cache/slot_client/slot.h"
#include "datasystem/common/l2cache/slot_client/slot_manifest.h"
#include "datasystem/common/l2cache/slot_client/slot_snapshot.h"
#include "datasystem/common/l2cache/slot_client/slot_client.h"
#include "datasystem/common/l2cache/slot_client/slot_takeover_planner.h"
#include "datasystem/common/util/file_util.h"

DS_DECLARE_string(l2_cache_type);
DS_DECLARE_string(sfs_path);
DS_DECLARE_string(distributed_disk_path);
DS_DECLARE_string(cluster_name);
DS_DECLARE_uint32(distributed_disk_slot_num);
DS_DECLARE_uint32(distributed_disk_max_data_file_size_mb);
DS_DECLARE_uint32(distributed_disk_sync_interval_ms);
DS_DECLARE_uint64(distributed_disk_sync_batch_bytes);
DS_DECLARE_uint64(distributed_disk_compact_cutover_bytes);
DS_DECLARE_uint32(distributed_disk_compact_cutover_records);

namespace datasystem {
namespace ut {
using testing::_;
using testing::Return;

namespace {
constexpr char TARGET_WORKER_ADDRESS[] = "127.0.0.1:31501";
constexpr char SOURCE_WORKER_ADDRESS_A[] = "127.0.0.1:31502";
constexpr char SOURCE_WORKER_ADDRESS_B[] = "127.0.0.1:31503";

std::string ReadAll(const std::shared_ptr<std::stringstream> &content)
{
    return content->str();
}

std::string MakeTempDir()
{
    std::string pattern = "/tmp/slot_store_ut_XXXXXX";
    std::vector<char> buffer(pattern.begin(), pattern.end());
    buffer.push_back('\0');
    auto *dir = mkdtemp(buffer.data());
    EXPECT_NE(dir, nullptr);
    return dir == nullptr ? "" : std::string(dir);
}

bool IsBinMockSupported()
{
#if defined(__arm__) || defined(__aarch64__) || defined(__ARM_ARCH)
    return false;
#else
    return true;
#endif
}

std::string GetSlotRootPath(const std::string &baseDir, const std::string &workerAddress = {})
{
    if (workerAddress.empty()) {
        return BuildSlotStoreRoot(baseDir, FLAGS_cluster_name);
    }
    return BuildSlotStoreRootForWorker(baseDir, FLAGS_cluster_name, SanitizeSlotWorkerNamespace(workerAddress));
}

std::string GetSlotPath(const std::string &baseDir, uint32_t slotId, const std::string &workerAddress = {})
{
    return JoinPath(GetSlotRootPath(baseDir, workerAddress), FormatSlotDir(slotId));
}

std::string GetRecoverySlotPath(const std::string &sourcePath, const std::string &txnId)
{
    return sourcePath + ".recovery." + txnId;
}

std::shared_ptr<std::stringstream> MakeBody(const std::string &payload)
{
    auto body = std::make_shared<std::stringstream>();
    *body << payload;
    return body;
}

void ExpectSlotValue(Slot &manager, const std::string &key, uint64_t version, const std::string &payload)
{
    auto content = std::make_shared<std::stringstream>();
    ASSERT_TRUE(manager.Get(key, version, content).IsOk());
    ASSERT_EQ(ReadAll(content), payload);
}

void SavePayload(Slot &manager, const std::string &key, uint64_t version, const std::string &payload)
{
    ASSERT_TRUE(manager.Save(key, version, MakeBody(payload)).IsOk());
}

void RunPreloadCallbackStopCase(const std::string &baseDir, const std::string &sourceWorkerAddress, uint32_t slotId,
                                const std::string &keyPrefix, const Status &stopStatus, size_t &callbackInvoked)
{
    SlotClient client(baseDir);
    ASSERT_TRUE(client.Init().IsOk());

    Slot sourceManager(slotId, GetSlotPath(baseDir, slotId, sourceWorkerAddress), 1024);
    Slot targetManager(slotId, GetSlotPath(baseDir, slotId, TARGET_WORKER_ADDRESS), 1024);
    const std::vector<std::pair<std::string, std::string>> expected{
        { keyPrefix + "A", "aaaa" },
        { keyPrefix + "B", "bbbb" },
    };
    for (size_t i = 0; i < expected.size(); ++i) {
        SavePayload(sourceManager, expected[i].first, i + 1, expected[i].second);
    }

    callbackInvoked = 0;
    SlotPreloadCallback callback = [&callbackInvoked, &stopStatus](const SlotPreloadMeta &meta,
                                                                   const std::shared_ptr<std::stringstream> &content) {
        ++callbackInvoked;
        EXPECT_FALSE(meta.objectKey.empty());
        EXPECT_FALSE(content->str().empty());
        EXPECT_EQ(meta.size, content->str().size());
        return stopStatus;
    };

    ASSERT_TRUE(client.PreloadSlot(sourceWorkerAddress, slotId, callback).IsOk());
    for (size_t i = 0; i < expected.size(); ++i) {
        ExpectSlotValue(targetManager, expected[i].first, i + 1, expected[i].second);
    }
}

SlotTakeoverPlan BuildTakeoverPlan(const std::string &sourcePath, const std::string &targetPath, Slot &sourceManager,
                                   Slot &targetManager, const std::string &txnId)
{
    EXPECT_TRUE(targetManager.BootstrapManifestIfNeed().IsOk());
    SlotManifestData sourceManifest;
    SlotManifestData targetManifest;
    EXPECT_TRUE(SlotManifest::Load(sourcePath, sourceManifest).IsOk());
    EXPECT_TRUE(SlotManifest::Load(targetPath, targetManifest).IsOk());
    SlotSnapshot sourceSnapshot;
    EXPECT_TRUE(sourceManager.ReplayIndex(sourceSnapshot).IsOk());
    SlotTakeoverPlan plan;
    SlotTakeoverRequest request;
    request.mode = SlotTakeoverMode::MERGE;
    EXPECT_TRUE(SlotTakeoverPlanner::BuildPlan(sourcePath, sourcePath + ".recovery." + txnId, sourceManifest,
                                               sourceSnapshot, targetPath, targetManifest, request, txnId, plan)
                    .IsOk());
    EXPECT_TRUE(SlotTakeoverPlanner::DumpPlan(targetPath, plan).IsOk());
    return plan;
}

void PersistImportingManifest(const std::string &targetPath, const SlotTakeoverPlan &plan)
{
    SlotManifestData manifest;
    ASSERT_TRUE(SlotManifest::Load(targetPath, manifest).IsOk());
    manifest.state = SlotState::IN_OPERATION;
    manifest.opType = SlotOperationType::TRANSFER;
    manifest.opPhase = SlotOperationPhase::TRANSFER_PREPARED;
    manifest.role = SlotOperationRole::TARGET;
    manifest.txnId = plan.txnId;
    manifest.peerSlotPath = plan.sourceHomeSlotPath;
    manifest.recoverySlotPath = plan.sourceRecoverySlotPath;
    manifest.transferPlanPath = FormatTakeoverPlanFileName(plan.txnId);
    manifest.transferFileMap.clear();
    for (size_t i = 0; i < plan.dataMappings.size(); ++i) {
        if (i > 0) {
            manifest.transferFileMap.append(",");
        }
        manifest.transferFileMap.append(std::to_string(plan.dataMappings[i].sourceFileId));
        manifest.transferFileMap.append(":");
        manifest.transferFileMap.append(std::to_string(plan.dataMappings[i].targetFileId));
    }
    ASSERT_TRUE(SlotManifest::StoreAtomic(targetPath, manifest).IsOk());
}

void PersistCompactCommittingManifest(const std::string &slotPath, SlotManifestData &manifest,
                                      const std::string &pendingIndex, const std::vector<std::string> &pendingData,
                                      uint64_t compactEpochMs)
{
    manifest.state = SlotState::IN_OPERATION;
    manifest.opType = SlotOperationType::COMPACT;
    manifest.opPhase = SlotOperationPhase::COMPACT_COMMITTING;
    manifest.role = SlotOperationRole::LOCAL;
    manifest.txnId = std::to_string(compactEpochMs);
    manifest.pendingIndex = pendingIndex;
    manifest.pendingData = pendingData;
    manifest.lastCompactEpochMs = compactEpochMs;
    ASSERT_TRUE(SlotManifest::StoreAtomic(slotPath, manifest).IsOk());
}

void ExpectManifestNormal(const SlotManifestData &manifest)
{
    ASSERT_EQ(manifest.state, SlotState::NORMAL);
    ASSERT_EQ(manifest.opType, SlotOperationType::NONE);
    ASSERT_EQ(manifest.opPhase, SlotOperationPhase::NONE);
    ASSERT_EQ(manifest.role, SlotOperationRole::NONE);
}

void PrepareRecoveryDir(const SlotTakeoverPlan &plan)
{
    ASSERT_TRUE(RenameFile(plan.sourceHomeSlotPath, plan.sourceRecoverySlotPath).IsOk());
}

void MoveTakeoverDataFiles(const SlotTakeoverPlan &plan, size_t moveCount)
{
    for (size_t i = 0; i < std::min(moveCount, plan.dataMappings.size()); ++i) {
        const auto &mapping = plan.dataMappings[i];
        ASSERT_TRUE(RenameFile(JoinPath(plan.sourceRecoverySlotPath, mapping.sourceDataFile),
                               JoinPath(plan.targetSlotPath, mapping.targetDataFile))
                        .IsOk());
    }
}

void AppendImportBatch(const std::string &targetPath, const std::string &targetIndex, const SlotTakeoverPlan &plan,
                       bool appendPuts, bool appendEnd)
{
    auto targetIndexPath = JoinPath(targetPath, targetIndex);
    ASSERT_TRUE(SlotIndexCodec::AppendImportBegin(targetIndexPath, SlotImportRecord{ plan.txnId }).IsOk());
    if (appendPuts) {
        std::vector<SlotRecord> importRecords;
        size_t validBytes = 0;
        ASSERT_TRUE(
            SlotIndexCodec::ReadAllRecords(JoinPath(targetPath, plan.importIndexFile), importRecords, validBytes)
                .IsOk());
        for (const auto &record : importRecords) {
            if (record.type == SlotRecordType::PUT) {
                ASSERT_TRUE(SlotIndexCodec::AppendPut(targetIndexPath, record.put).IsOk());
            }
        }
    }
    if (appendEnd) {
        ASSERT_TRUE(SlotIndexCodec::AppendImportEnd(targetIndexPath, SlotImportRecord{ plan.txnId }).IsOk());
    }
}

constexpr size_t LARGE_SCALE_KEY_COUNT = 2560;
constexpr size_t LARGE_SCALE_TARGET_KEY_COUNT = 768;
constexpr size_t LARGE_SCALE_PAYLOAD_BYTES = 512;

std::string MakeLargeScaleKey(const std::string &prefix, size_t index)
{
    return prefix + "/key_" + std::to_string(index);
}

std::string MakeLargeScalePayload(const std::string &prefix, size_t index, uint64_t version, size_t payloadBytes)
{
    std::string header = prefix + "|" + std::to_string(index) + "|" + std::to_string(version) + "|";
    std::string payload;
    payload.reserve(std::max(payloadBytes, header.size()));
    payload.append(header);
    while (payload.size() < payloadBytes) {
        payload.push_back(static_cast<char>('a' + ((index + version + payload.size()) % 26)));
    }
    payload.resize(std::max(payloadBytes, header.size()));
    return payload;
}

bool ShouldDeleteAllVersions(size_t index)
{
    return index % 13 == 0;
}

uint64_t DeletedUpToVersion(size_t index)
{
    if (ShouldDeleteAllVersions(index)) {
        return std::numeric_limits<uint64_t>::max();
    }
    if (index % 11 == 0) {
        return 4;
    }
    if (index % 7 == 0) {
        return 2;
    }
    if (index % 5 == 0) {
        return 3;
    }
    return 0;
}

bool IsExpectedVersionVisible(size_t index, uint64_t version)
{
    auto deletedUpTo = DeletedUpToVersion(index);
    if (deletedUpTo == std::numeric_limits<uint64_t>::max()) {
        return false;
    }
    return version > deletedUpTo;
}

uint64_t ExpectedLatestVersion(size_t index)
{
    for (uint64_t version = 4; version >= 1; --version) {
        if (IsExpectedVersionVisible(index, version)) {
            return version;
        }
    }
    return 0;
}

void PopulateLargeScaleDataset(Slot &manager, const std::string &prefix, size_t keyCount, size_t payloadBytes)
{
    for (size_t index = 0; index < keyCount; ++index) {
        auto key = MakeLargeScaleKey(prefix, index);
        for (uint64_t version = 1; version <= 4; ++version) {
            SavePayload(manager, key, version, MakeLargeScalePayload(prefix, index, version, payloadBytes));
        }
        if (ShouldDeleteAllVersions(index)) {
            ASSERT_TRUE(manager.Delete(key, 4, true).IsOk());
            continue;
        }
        auto deletedUpTo = DeletedUpToVersion(index);
        if (deletedUpTo > 0) {
            ASSERT_TRUE(manager.Delete(key, deletedUpTo, false).IsOk());
        }
    }
}

void VerifyLargeScaleDataset(Slot &manager, const std::string &prefix, size_t keyCount, size_t payloadBytes)
{
    for (size_t index = 0; index < keyCount; ++index) {
        auto key = MakeLargeScaleKey(prefix, index);
        for (uint64_t version = 1; version <= 4; ++version) {
            auto exact = std::make_shared<std::stringstream>();
            auto rc = manager.Get(key, version, exact);
            if (IsExpectedVersionVisible(index, version)) {
                ASSERT_TRUE(rc.IsOk()) << "key=" << key << ", version=" << version;
                ASSERT_EQ(ReadAll(exact), MakeLargeScalePayload(prefix, index, version, payloadBytes));
            } else {
                ASSERT_EQ(rc.GetCode(), StatusCode::K_NOT_FOUND_IN_L2CACHE) << "key=" << key << ", version=" << version;
            }
        }

        auto latest = std::make_shared<std::stringstream>();
        auto latestRc = manager.GetWithoutVersion(key, 0, latest);
        auto expectedLatest = ExpectedLatestVersion(index);
        if (expectedLatest == 0) {
            ASSERT_EQ(latestRc.GetCode(), StatusCode::K_NOT_FOUND_IN_L2CACHE) << "key=" << key;
        } else {
            ASSERT_TRUE(latestRc.IsOk()) << "key=" << key;
            ASSERT_EQ(ReadAll(latest), MakeLargeScalePayload(prefix, index, expectedLatest, payloadBytes));
        }
    }
}

bool WaitUntil(const std::function<bool()> &predicate, int timeoutMs = 5000, int intervalMs = 20)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));
    }
    return predicate();
}
}  // namespace

class SlotStoreTest : public testing::Test {
public:
    void SetUp() override
    {
        baseDir_ = MakeTempDir() + "/sfs";
        ASSERT_FALSE(baseDir_.empty());
        ASSERT_TRUE(CreateDir(baseDir_, true).IsOk());
        FLAGS_l2_cache_type = "distributed_disk";
        FLAGS_distributed_disk_path = baseDir_;
        FLAGS_sfs_path = baseDir_;
        FLAGS_cluster_name = "ut_cluster";
        FLAGS_distributed_disk_slot_num = 4;
        FLAGS_distributed_disk_max_data_file_size_mb = 1;
        FLAGS_distributed_disk_compact_cutover_bytes = 64 * 1024;
        FLAGS_distributed_disk_compact_cutover_records = 128;
        SetSlotWorkerNamespace(SanitizeSlotWorkerNamespace(TARGET_WORKER_ADDRESS));
    }

    void TearDown() override
    {
        (void)inject::ClearAll();
        SetSlotWorkerNamespace(DEFAULT_WORKER_NAME);
        if (!baseDir_.empty()) {
            auto rootDir = baseDir_.substr(0, baseDir_.find("/sfs"));
            (void)RemoveAll(rootDir);
        }
    }

protected:
    std::string baseDir_;
};

TEST_F(SlotStoreTest, SlotIndexCodecTruncatesBrokenTail)
{
    auto dir = MakeTempDir() + "/codec";
    ASSERT_TRUE(CreateDir(dir, true).IsOk());
    auto indexPath = dir + "/index_active.log";
    ASSERT_TRUE(SlotIndexCodec::EnsureIndexFile(indexPath).IsOk());
    SlotPutRecord record;
    record.key = "k1";
    record.fileId = 1;
    record.offset = 0;
    record.size = 3;
    record.version = 7;
    ASSERT_TRUE(SlotIndexCodec::AppendPut(indexPath, record).IsOk());

    int fd = -1;
    ASSERT_TRUE(OpenFile(indexPath, O_WRONLY | O_APPEND, &fd).IsOk());
    ASSERT_TRUE(WriteFile(fd, "xyz", 3, FdFileSize(fd)).IsOk());
    close(fd);

    std::vector<SlotRecord> records;
    size_t validBytes = 0;
    ASSERT_TRUE(SlotIndexCodec::ReadAllRecords(indexPath, records, validBytes).IsOk());
    ASSERT_EQ(records.size(), 1u);
    ASSERT_EQ(FileSize(indexPath), static_cast<off_t>(validBytes));
    (void)RemoveAll(dir.substr(0, dir.find("/codec")));
}

TEST_F(SlotStoreTest, SlotIndexCodecRepairsShortHeader)
{
    auto dir = MakeTempDir() + "/short_header";
    ASSERT_TRUE(CreateDir(dir, true).IsOk());
    auto indexPath = dir + "/index_active.log";
    int fd = -1;
    ASSERT_TRUE(OpenFile(indexPath, O_CREAT | O_TRUNC | O_WRONLY, 0644, &fd).IsOk());
    ASSERT_TRUE(WriteFile(fd, "tiny", 4, 0).IsOk());
    close(fd);

    ASSERT_TRUE(SlotIndexCodec::EnsureIndexFile(indexPath).IsOk());
    ASSERT_GE(FileSize(indexPath), static_cast<off_t>(SlotIndexCodec::HEADER_SIZE));
    (void)RemoveAll(dir.substr(0, dir.find("/short_header")));
}

TEST_F(SlotStoreTest, SlotIndexCodecRepairsInvalidHeaderMagic)
{
    auto dir = MakeTempDir() + "/bad_header";
    ASSERT_TRUE(CreateDir(dir, true).IsOk());
    auto indexPath = dir + "/index_active.log";
    std::string brokenHeader(SlotIndexCodec::HEADER_SIZE, '\0');
    int fd = -1;
    ASSERT_TRUE(OpenFile(indexPath, O_CREAT | O_TRUNC | O_WRONLY, 0644, &fd).IsOk());
    ASSERT_TRUE(WriteFile(fd, brokenHeader.data(), brokenHeader.size(), 0).IsOk());
    close(fd);

    ASSERT_TRUE(SlotIndexCodec::EnsureIndexFile(indexPath).IsOk());

    std::vector<SlotRecord> records;
    size_t validBytes = 0;
    ASSERT_TRUE(SlotIndexCodec::ReadAllRecords(indexPath, records, validBytes).IsOk());
    ASSERT_TRUE(records.empty());
    ASSERT_EQ(validBytes, SlotIndexCodec::HEADER_SIZE);
    (void)RemoveAll(dir.substr(0, dir.find("/bad_header")));
}

TEST_F(SlotStoreTest, SlotIndexCodecRejectsEmptyKeys)
{
    auto dir = MakeTempDir() + "/empty_keys";
    ASSERT_TRUE(CreateDir(dir, true).IsOk());
    auto indexPath = dir + "/index_active.log";
    SlotPutRecord put;
    SlotDeleteRecord del;
    ASSERT_EQ(SlotIndexCodec::AppendPut(indexPath, put).GetCode(), StatusCode::K_INVALID);
    ASSERT_EQ(SlotIndexCodec::AppendDelete(indexPath, del).GetCode(), StatusCode::K_INVALID);
    (void)RemoveAll(dir.substr(0, dir.find("/empty_keys")));
}

TEST_F(SlotStoreTest, SlotIndexCodecTruncatesUnknownRecordTypeTail)
{
    auto dir = MakeTempDir() + "/unknown_type";
    ASSERT_TRUE(CreateDir(dir, true).IsOk());
    auto indexPath = dir + "/index_active.log";
    ASSERT_TRUE(SlotIndexCodec::EnsureIndexFile(indexPath).IsOk());

    int fd = -1;
    ASSERT_TRUE(OpenFile(indexPath, O_WRONLY | O_APPEND, &fd).IsOk());
    const uint8_t badType = 0x7f;
    ASSERT_TRUE(WriteFile(fd, &badType, sizeof(badType), FdFileSize(fd)).IsOk());
    close(fd);

    std::vector<SlotRecord> records;
    size_t validBytes = 0;
    ASSERT_TRUE(SlotIndexCodec::ReadAllRecords(indexPath, records, validBytes).IsOk());
    ASSERT_TRUE(records.empty());
    ASSERT_EQ(validBytes, SlotIndexCodec::HEADER_SIZE);
    ASSERT_EQ(FileSize(indexPath), static_cast<off_t>(SlotIndexCodec::HEADER_SIZE));
    (void)RemoveAll(dir.substr(0, dir.find("/unknown_type")));
}

TEST_F(SlotStoreTest, SlotIndexCodecTruncatesBrokenDeleteTail)
{
    auto dir = MakeTempDir() + "/broken_delete";
    ASSERT_TRUE(CreateDir(dir, true).IsOk());
    auto indexPath = dir + "/index_active.log";
    ASSERT_TRUE(SlotIndexCodec::EnsureIndexFile(indexPath).IsOk());
    SlotDeleteRecord record;
    record.key = "key_delete";
    record.version = 8;
    ASSERT_TRUE(SlotIndexCodec::AppendDelete(indexPath, record).IsOk());

    int fd = -1;
    ASSERT_TRUE(OpenFile(indexPath, O_WRONLY | O_APPEND, &fd).IsOk());
    const char brokenTail[] = { static_cast<char>(SlotRecordType::DELETE), 0x03, 0x00, 0x00 };
    ASSERT_TRUE(WriteFile(fd, brokenTail, sizeof(brokenTail), FdFileSize(fd)).IsOk());
    close(fd);

    std::vector<SlotRecord> records;
    size_t validBytes = 0;
    ASSERT_TRUE(SlotIndexCodec::ReadAllRecords(indexPath, records, validBytes).IsOk());
    ASSERT_EQ(records.size(), 1u);
    ASSERT_EQ(records[0].type, SlotRecordType::DELETE);
    ASSERT_EQ(validBytes, static_cast<size_t>(FileSize(indexPath)));
    (void)RemoveAll(dir.substr(0, dir.find("/broken_delete")));
}

TEST_F(SlotStoreTest, SlotManifestStoreAndLoad)
{
    auto dir = MakeTempDir() + "/manifest";
    SlotManifestData manifest;
    manifest.activeIndex = "index_active.log";
    manifest.activeData = { FormatDataFileName(1), FormatDataFileName(2) };
    manifest.pendingIndex = "index_compact_123.log";
    manifest.pendingData = { "data_000003.bin" };
    manifest.gcPending = true;
    manifest.obsoleteIndex = "index_old.log";
    manifest.obsoleteData = { "data_000009.bin" };
    manifest.lastCompactEpochMs = 123;
    ASSERT_TRUE(SlotManifest::StoreAtomic(dir, manifest).IsOk());
    SlotManifestData loaded;
    ASSERT_TRUE(SlotManifest::Load(dir, loaded).IsOk());
    ASSERT_EQ(loaded.activeIndex, manifest.activeIndex);
    ASSERT_EQ(loaded.activeData.size(), 2u);
    ExpectManifestNormal(loaded);
    ASSERT_EQ(loaded.pendingIndex, manifest.pendingIndex);
    ASSERT_EQ(loaded.pendingData, manifest.pendingData);
    ASSERT_TRUE(loaded.gcPending);
    ASSERT_EQ(loaded.obsoleteIndex, manifest.obsoleteIndex);
    ASSERT_EQ(loaded.obsoleteData, manifest.obsoleteData);
    ASSERT_EQ(loaded.lastCompactEpochMs, manifest.lastCompactEpochMs);
    (void)RemoveAll(dir.substr(0, dir.find("/manifest")));
}

TEST_F(SlotStoreTest, SlotManifestDecodeRejectsInvalidVersion)
{
    SlotManifestData manifest;
    auto rc = SlotManifest::Decode("version=abc\nstate=NORMAL\nactive_index=index_active.log\n", manifest);
    ASSERT_EQ(rc.GetCode(), StatusCode::K_INVALID);
}

TEST_F(SlotStoreTest, SlotManifestDecodeRejectsMalformedFields)
{
    SlotManifestData manifest;
    ASSERT_EQ(SlotManifest::Decode("version=1\nstate=UNKNOWN\nactive_index=index_active.log\n", manifest).GetCode(),
              StatusCode::K_INVALID);
    ASSERT_EQ(SlotManifest::Decode("version=1\nbroken_line\nactive_index=index_active.log\n", manifest).GetCode(),
              StatusCode::K_INVALID);
    ASSERT_EQ(SlotManifest::Decode("version=1\nstate=NORMAL\nactive_index=\n", manifest).GetCode(),
              StatusCode::K_INVALID);
}

TEST_F(SlotStoreTest, SlotManifestLoadRemovesTmpFile)
{
    auto dir = MakeTempDir() + "/manifest_tmp";
    SlotManifestData manifest;
    ASSERT_TRUE(SlotManifest::StoreAtomic(dir, manifest).IsOk());
    ASSERT_TRUE(AtomicWriteTextFile(dir + "/manifest.tmp", "stale").IsOk());

    SlotManifestData loaded;
    ASSERT_TRUE(SlotManifest::Load(dir, loaded).IsOk());
    ASSERT_FALSE(FileExist(dir + "/manifest.tmp"));
    (void)RemoveAll(dir.substr(0, dir.find("/manifest_tmp")));
}

TEST_F(SlotStoreTest, PersistenceApiAggregateSaveGetDelete)
{
    auto api = PersistenceApi::Create();
    ASSERT_TRUE(api->Init().IsOk());

    auto body1 = std::make_shared<std::stringstream>();
    *body1 << "value_v1";
    ASSERT_TRUE(api->Save("tenant/keyA", 1, 1000, body1).IsOk());

    auto body2 = std::make_shared<std::stringstream>();
    *body2 << "value_v2";
    ASSERT_TRUE(api->Save("tenant/keyA", 2, 1000, body2).IsOk());

    auto exact = std::make_shared<std::stringstream>();
    ASSERT_TRUE(api->Get("tenant/keyA", 1, 1000, exact).IsOk());
    ASSERT_EQ(ReadAll(exact), "value_v1");

    auto latest = std::make_shared<std::stringstream>();
    ASSERT_TRUE(api->GetWithoutVersion("tenant/keyA", 1000, 0, latest).IsOk());
    ASSERT_EQ(ReadAll(latest), "value_v2");

    ASSERT_TRUE(api->Del("tenant/keyA", 1, false).IsOk());

    auto deleted = std::make_shared<std::stringstream>();
    ASSERT_EQ(api->Get("tenant/keyA", 1, 1000, deleted).GetCode(), StatusCode::K_OK);
    ASSERT_EQ(ReadAll(deleted), "value_v2");

    auto newest = std::make_shared<std::stringstream>();
    ASSERT_TRUE(api->GetWithoutVersion("tenant/keyA", 1000, 0, newest).IsOk());
    ASSERT_EQ(ReadAll(newest), "value_v2");
}

TEST_F(SlotStoreTest, PersistenceApiAggregateSavePreservesWriteMode)
{
    auto api = PersistenceApi::Create();
    ASSERT_TRUE(api->Init().IsOk());

    auto body = std::make_shared<std::stringstream>();
    *body << "write_mode_payload";
    ASSERT_TRUE(api->Save("tenant/keyWriteModeApi", 7, 1000, body, 0, WriteMode::WRITE_BACK_L2_CACHE).IsOk());

    auto slotRoot = BuildSlotStoreRoot(FLAGS_distributed_disk_path, FLAGS_cluster_name);
    uint32_t slotId =
        static_cast<uint32_t>(std::hash<std::string>{}("tenant/keyWriteModeApi") % FLAGS_distributed_disk_slot_num);
    auto slotPath = slotRoot + "/" + FormatSlotDir(slotId);
    SlotManifestData manifest;
    ASSERT_TRUE(SlotManifest::Load(slotPath, manifest).IsOk());

    SlotSnapshot snapshot;
    ASSERT_TRUE(SlotSnapshot::Replay(slotPath, manifest, snapshot).IsOk());
    SlotSnapshotValue value;
    ASSERT_TRUE(snapshot.FindExact("tenant/keyWriteModeApi", 7, value).IsOk());
    ASSERT_EQ(value.writeMode, WriteMode::WRITE_BACK_L2_CACHE);
}

TEST_F(SlotStoreTest, DistributedDiskUsesDistributedDiskPathInsteadOfSfsPath)
{
    auto distributedDiskPath = MakeTempDir() + "/distributed_disk";
    auto legacySfsPath = MakeTempDir() + "/legacy_sfs";
    ASSERT_TRUE(CreateDir(distributedDiskPath, true).IsOk());
    ASSERT_TRUE(CreateDir(legacySfsPath, true).IsOk());

    FLAGS_distributed_disk_path = distributedDiskPath;
    FLAGS_sfs_path = legacySfsPath;

    auto api = PersistenceApi::Create();
    ASSERT_TRUE(api->Init().IsOk());

    auto body = std::make_shared<std::stringstream>();
    *body << "disk_root_payload";
    ASSERT_TRUE(api->Save("tenant/keyDiskRoot", 1, 1000, body).IsOk());

    auto distributedRoot = BuildSlotStoreRoot(FLAGS_distributed_disk_path, FLAGS_cluster_name);
    auto legacyRoot = BuildSlotStoreRoot(FLAGS_sfs_path, FLAGS_cluster_name);
    uint32_t slotId =
        static_cast<uint32_t>(std::hash<std::string>{}("tenant/keyDiskRoot") % FLAGS_distributed_disk_slot_num);
    ASSERT_TRUE(FileExist(JoinPath(distributedRoot, FormatSlotDir(slotId))));
    ASSERT_FALSE(FileExist(JoinPath(legacyRoot, FormatSlotDir(slotId))));

    ASSERT_TRUE(RemoveAll(distributedDiskPath.substr(0, distributedDiskPath.find("/distributed_disk"))).IsOk());
    ASSERT_TRUE(RemoveAll(legacySfsPath.substr(0, legacySfsPath.find("/legacy_sfs"))).IsOk());
}

TEST_F(SlotStoreTest, PersistenceApiNonAggregateModeKeepsOriginalSfsBehavior)
{
    FLAGS_l2_cache_type = "sfs";
    auto api = PersistenceApi::Create();
    ASSERT_TRUE(api->Init().IsOk());

    auto body = std::make_shared<std::stringstream>();
    *body << "plain_sfs";
    ASSERT_TRUE(api->Save("tenant/keyPlain", 1, 1000, body).IsOk());

    auto exact = std::make_shared<std::stringstream>();
    ASSERT_TRUE(api->Get("tenant/keyPlain", 1, 1000, exact).IsOk());
    ASSERT_EQ(ReadAll(exact), "plain_sfs");

    std::string encodedKey;
    ASSERT_TRUE(PersistenceApi::UrlEncode("tenant/keyPlain", encodedKey).IsOk());
    ASSERT_TRUE(FileExist(baseDir_ + "/datasystem/" + encodedKey + "/1"));
    ASSERT_FALSE(FileExist(BuildSlotStoreRoot(FLAGS_distributed_disk_path, FLAGS_cluster_name)));

    ASSERT_TRUE(api->Del("tenant/keyPlain", 1, true).IsOk());
    auto deleted = std::make_shared<std::stringstream>();
    ASSERT_EQ(api->GetWithoutVersion("tenant/keyPlain", 1000, 0, deleted).GetCode(),
              StatusCode::K_NOT_FOUND_IN_L2CACHE);
}

TEST_F(SlotStoreTest, ReplaySkipsInvalidPut)
{
    auto api = PersistenceApi::Create();
    ASSERT_TRUE(api->Init().IsOk());

    auto body = std::make_shared<std::stringstream>();
    *body << "abc";
    ASSERT_TRUE(api->Save("tenant/keyB", 5, 1000, body).IsOk());

    auto slotRoot = BuildSlotStoreRoot(FLAGS_distributed_disk_path, FLAGS_cluster_name);
    uint32_t slotId = static_cast<uint32_t>(std::hash<std::string>{}("tenant/keyB") % FLAGS_distributed_disk_slot_num);
    auto slotPath = slotRoot + "/" + FormatSlotDir(slotId);
    auto dataPath = slotPath + "/" + FormatDataFileName(1);
    ASSERT_TRUE(DeleteFile(dataPath).IsOk());

    SlotManifestData manifest;
    ASSERT_TRUE(SlotManifest::Load(slotPath, manifest).IsOk());
    SlotSnapshot snapshot;
    ASSERT_TRUE(SlotSnapshot::Replay(slotPath, manifest, snapshot).IsOk());
    SlotSnapshotValue value;
    ASSERT_EQ(snapshot.FindLatest("tenant/keyB", 0, value).GetCode(), StatusCode::K_NOT_FOUND_IN_L2CACHE);
}

TEST_F(SlotStoreTest, ReplayFindsSavedObjectAfterBootstrap)
{
    auto slotPath = MakeTempDir() + "/slot_bootstrap_replay";
    Slot manager(1, slotPath, 1024);
    auto body = std::make_shared<std::stringstream>();
    *body << "bootstrap_value";
    ASSERT_TRUE(manager.Save("tenant/keyBootstrap", 9, body).IsOk());

    SlotManifestData manifest;
    ASSERT_TRUE(SlotManifest::Load(slotPath, manifest).IsOk());
    ASSERT_EQ(manifest.activeData, std::vector<std::string>{ FormatDataFileName(1) });

    SlotSnapshot snapshot;
    ASSERT_TRUE(SlotSnapshot::Replay(slotPath, manifest, snapshot).IsOk());
    SlotSnapshotValue value;
    ASSERT_TRUE(snapshot.FindExact("tenant/keyBootstrap", 9, value).IsOk());
    ASSERT_EQ(value.fileId, 1u);
    (void)RemoveAll(slotPath.substr(0, slotPath.find("/slot_bootstrap_replay")));
}

TEST_F(SlotStoreTest, SlotReplayPreservesWriteMode)
{
    auto slotPath = MakeTempDir() + "/slot_write_mode";
    Slot manager(1, slotPath, 1024);
    auto body = std::make_shared<std::stringstream>();
    *body << "value";
    ASSERT_TRUE(manager.Save("tenant/keyWriteMode", 1, body, 0, WriteMode::WRITE_THROUGH_L2_CACHE).IsOk());

    SlotSnapshot snapshot;
    ASSERT_TRUE(manager.ReplayIndex(snapshot).IsOk());
    SlotSnapshotValue value;
    ASSERT_TRUE(snapshot.FindExact("tenant/keyWriteMode", 1, value).IsOk());
    ASSERT_EQ(value.writeMode, WriteMode::WRITE_THROUGH_L2_CACHE);
    (void)RemoveAll(slotPath.substr(0, slotPath.find("/slot_write_mode")));
}

TEST_F(SlotStoreTest, SlotFileUtilHelpersWork)
{
    ASSERT_EQ(JoinPath("", "b"), "b");
    ASSERT_EQ(JoinPath("/a", ""), "/a");
    ASSERT_EQ(JoinPath("/a", "b"), "/a/b");
    ASSERT_EQ(JoinPath("/a/", "/b"), "/a/b");
    ASSERT_EQ(FormatSlotDir(7), "slot_0007");
    ASSERT_EQ(FormatDataFileName(12), "data_00000012.bin");
    uint32_t fileId = 0;
    ASSERT_TRUE(ParseDataFileId("data_00000123.bin", fileId).IsOk());
    ASSERT_EQ(fileId, 123u);
    ASSERT_EQ(BuildSlotStoreRoot("/tmp/root", "cluster_x"),
              "/tmp/root/datasystem/cluster_x/slot_store/127.0.0.1_31501");
    ASSERT_EQ(BuildSlotStoreRootForWorker("/tmp/root", "cluster_x", ""),
              "/tmp/root/datasystem/cluster_x/slot_store/worker_local");
}

TEST_F(SlotStoreTest, FileUtilErrorPathsReturnFailure)
{
    std::string content;
    ASSERT_EQ(FsyncFd(-1).GetCode(), StatusCode::K_INVALID);
    ASSERT_EQ(FsyncDir(baseDir_ + "/dir_not_exist").GetCode(), StatusCode::K_IO_ERROR);
    ASSERT_EQ(ReadWholeFile(baseDir_ + "/missing_file", content).GetCode(), StatusCode::K_IO_ERROR);

    auto parentFile = baseDir_ + "/not_a_dir";
    ASSERT_TRUE(AtomicWriteTextFile(parentFile, "parent").IsOk());
    ASSERT_TRUE(FileExist(parentFile));
    ASSERT_TRUE(AtomicWriteTextFile(parentFile + "/child", "x").IsError());
}

TEST_F(SlotStoreTest, AtomicWriteTextFileReturnsErrorWhenOpenFails)
{
    if (!IsBinMockSupported()) {
        GTEST_SKIP() << "binmock is not supported on this architecture";
    }
    BINEXPECT_CALL((Status(*)(const std::string &, int, mode_t, int *))OpenFile, (_, _, _, _))
        .WillRepeatedly(Return(Status(StatusCode::K_IO_ERROR, __LINE__, __FILE__, "mock open failure")));
    ASSERT_EQ(AtomicWriteTextFile(baseDir_ + "/mock_manifest", "abc").GetCode(), StatusCode::K_IO_ERROR);
    RELEASE_STUBS
}

TEST_F(SlotStoreTest, SlotSnapshotDeleteAndFindLatest)
{
    SlotSnapshot snapshot;
    SlotPutRecord v1;
    v1.key = "tenant/keyC";
    v1.fileId = 1;
    v1.offset = 0;
    v1.size = 3;
    v1.version = 1;
    SlotPutRecord v2 = v1;
    v2.version = 2;
    v2.offset = 3;
    snapshot.ApplyPut(v1);
    snapshot.ApplyPut(v2);

    SlotSnapshotValue value;
    ASSERT_TRUE(snapshot.FindExact("tenant/keyC", 1, value).IsOk());
    ASSERT_EQ(value.version, 1u);
    ASSERT_TRUE(snapshot.FindLatest("tenant/keyC", 0, value).IsOk());
    ASSERT_EQ(value.version, 2u);

    SlotDeleteRecord del;
    del.key = "tenant/keyC";
    del.version = 1;
    snapshot.ApplyDelete(del);
    ASSERT_EQ(snapshot.FindExact("tenant/keyC", 1, value).GetCode(), StatusCode::K_NOT_FOUND_IN_L2CACHE);
    ASSERT_TRUE(snapshot.FindLatest("tenant/keyC", 0, value).IsOk());
    ASSERT_EQ(value.version, 2u);
}

TEST_F(SlotStoreTest, SlotSnapshotFindErrorBranches)
{
    SlotSnapshot snapshot;
    SlotPutRecord record;
    record.key = "tenant/keyMissing";
    record.fileId = 1;
    record.offset = 0;
    record.size = 3;
    record.version = 3;
    snapshot.ApplyPut(record);

    SlotSnapshotValue value;
    ASSERT_EQ(snapshot.FindExact("tenant/not_exist", 3, value).GetCode(), StatusCode::K_NOT_FOUND_IN_L2CACHE);
    ASSERT_EQ(snapshot.FindExact("tenant/keyMissing", 4, value).GetCode(), StatusCode::K_NOT_FOUND_IN_L2CACHE);
    ASSERT_EQ(snapshot.FindLatest("tenant/keyMissing", 3, value).GetCode(), StatusCode::K_NOT_FOUND_IN_L2CACHE);

    SlotDeleteRecord del;
    del.key = "tenant/keyMissing";
    del.version = 10;
    snapshot.ApplyDelete(del);
    ASSERT_EQ(snapshot.FindLatest("tenant/keyMissing", 0, value).GetCode(), StatusCode::K_NOT_FOUND_IN_L2CACHE);
}

TEST_F(SlotStoreTest, SlotSnapshotReplaySkipsOutOfRangePut)
{
    auto slotPath = MakeTempDir() + "/slot_invalid_put";
    ASSERT_TRUE(CreateDir(slotPath, true).IsOk());
    SlotManifestData manifest;
    ASSERT_TRUE(SlotManifest::StoreAtomic(slotPath, manifest).IsOk());
    ASSERT_TRUE(SlotIndexCodec::EnsureIndexFile(slotPath + "/index_active.log").IsOk());

    int fd = -1;
    ASSERT_TRUE(OpenFile(slotPath + "/" + FormatDataFileName(1), O_CREAT | O_TRUNC | O_WRONLY, 0644, &fd).IsOk());
    ASSERT_TRUE(WriteFile(fd, "abc", 3, 0).IsOk());
    close(fd);

    SlotPutRecord record;
    record.key = "tenant/key_range";
    record.fileId = 1;
    record.offset = 2;
    record.size = 4;
    record.version = 11;
    ASSERT_TRUE(SlotIndexCodec::AppendPut(slotPath + "/index_active.log", record).IsOk());

    SlotSnapshot snapshot;
    ASSERT_TRUE(SlotSnapshot::Replay(slotPath, manifest, snapshot).IsOk());
    SlotSnapshotValue value;
    ASSERT_EQ(snapshot.FindLatest("tenant/key_range", 0, value).GetCode(), StatusCode::K_NOT_FOUND_IN_L2CACHE);
    (void)RemoveAll(slotPath.substr(0, slotPath.find("/slot_invalid_put")));
}

TEST_F(SlotStoreTest, SlotRollsDataFileWhenCurrentFileFull)
{
    auto slotPath = MakeTempDir() + "/slot_roll";
    Slot manager(1, slotPath, 4);
    auto body1 = std::make_shared<std::stringstream>();
    *body1 << "abcd";
    ASSERT_TRUE(manager.Save("tenant/keyD", 1, body1).IsOk());

    auto body2 = std::make_shared<std::stringstream>();
    *body2 << "ef";
    ASSERT_TRUE(manager.Save("tenant/keyE", 2, body2).IsOk());

    SlotManifestData manifest;
    ASSERT_TRUE(SlotManifest::Load(slotPath, manifest).IsOk());
    ASSERT_EQ(manifest.activeData.size(), 2u);
    ASSERT_EQ(manifest.activeData.back(), FormatDataFileName(2));

    auto content = std::make_shared<std::stringstream>();
    ASSERT_TRUE(manager.Get("tenant/keyE", 2, content).IsOk());
    ASSERT_EQ(ReadAll(content), "ef");
    (void)RemoveAll(slotPath.substr(0, slotPath.find("/slot_roll")));
}

TEST_F(SlotStoreTest, SlotSaveRejectsNullBody)
{
    auto slotPath = MakeTempDir() + "/slot_null_body";
    Slot manager(3, slotPath, 1024);
    ASSERT_EQ(manager.Save("tenant/keyNull", 1, nullptr).GetCode(), StatusCode::K_INVALID);
    (void)RemoveAll(slotPath.substr(0, slotPath.find("/slot_null_body")));
}

TEST_F(SlotStoreTest, SlotSaveReadsStreamFromBeginning)
{
    auto slotPath = MakeTempDir() + "/slot_seek";
    Slot manager(4, slotPath, 1024);
    auto body = std::make_shared<std::stringstream>();
    *body << "seekable";
    body->seekg(0, std::ios::end);
    ASSERT_TRUE(manager.Save("tenant/keySeek", 9, body).IsOk());

    auto content = std::make_shared<std::stringstream>();
    ASSERT_TRUE(manager.Get("tenant/keySeek", 9, content).IsOk());
    ASSERT_EQ(ReadAll(content), "seekable");
    (void)RemoveAll(slotPath.substr(0, slotPath.find("/slot_seek")));
}

TEST_F(SlotStoreTest, SlotRepairTruncatesBrokenTail)
{
    auto slotPath = MakeTempDir() + "/slot_repair";
    Slot manager(2, slotPath, 1024);
    auto body = std::make_shared<std::stringstream>();
    *body << "hello";
    ASSERT_TRUE(manager.Save("tenant/keyF", 3, body).IsOk());

    SlotManifestData manifest;
    ASSERT_TRUE(SlotManifest::Load(slotPath, manifest).IsOk());
    auto indexPath = JoinPath(slotPath, manifest.activeIndex);
    int fd = -1;
    ASSERT_TRUE(OpenFile(indexPath, O_WRONLY | O_APPEND, &fd).IsOk());
    ASSERT_TRUE(WriteFile(fd, "broken", 6, FdFileSize(fd)).IsOk());
    close(fd);

    ASSERT_TRUE(manager.Repair().IsOk());

    auto content = std::make_shared<std::stringstream>();
    ASSERT_TRUE(manager.Get("tenant/keyF", 3, content).IsOk());
    ASSERT_EQ(ReadAll(content), "hello");
    (void)RemoveAll(slotPath.substr(0, slotPath.find("/slot_repair")));
}

TEST_F(SlotStoreTest, SlotGetRejectsNullContent)
{
    auto slotPath = MakeTempDir() + "/slot_null_content";
    Slot manager(5, slotPath, 1024);
    auto body = std::make_shared<std::stringstream>();
    *body << "body";
    ASSERT_TRUE(manager.Save("tenant/keyH", 1, body).IsOk());
    std::shared_ptr<std::stringstream> content;
    ASSERT_EQ(manager.Get("tenant/keyH", 1, content).GetCode(), StatusCode::K_INVALID);
    (void)RemoveAll(slotPath.substr(0, slotPath.find("/slot_null_content")));
}

TEST_F(SlotStoreTest, SlotGetUsesRuntimeSnapshotWithoutReplayIndex)
{
    auto slotPath = MakeTempDir() + "/slot_runtime_snapshot";
    Slot manager(50, slotPath, 1024);
    SavePayload(manager, "tenant/keyRuntime", 1, "runtime");

    ASSERT_TRUE(inject::Set("slotstore.Slot.ReplayIndex.Enter", "call()").IsOk());

    auto exact = std::make_shared<std::stringstream>();
    ASSERT_TRUE(manager.Get("tenant/keyRuntime", 1, exact).IsOk());
    ASSERT_EQ(ReadAll(exact), "runtime");
    auto latest = std::make_shared<std::stringstream>();
    ASSERT_TRUE(manager.GetWithoutVersion("tenant/keyRuntime", 0, latest).IsOk());
    ASSERT_EQ(ReadAll(latest), "runtime");
    ASSERT_EQ(inject::GetExecuteCount("slotstore.Slot.ReplayIndex.Enter"), 0u);

    SlotSnapshot snapshot;
    ASSERT_TRUE(manager.ReplayIndex(snapshot).IsOk());
    ASSERT_EQ(inject::GetExecuteCount("slotstore.Slot.ReplayIndex.Enter"), 1u);
    (void)RemoveAll(slotPath.substr(0, slotPath.find("/slot_runtime_snapshot")));
}

TEST_F(SlotStoreTest, SlotSealFlushesPendingWrites)
{
    const auto oldSyncIntervalMs = FLAGS_distributed_disk_sync_interval_ms;
    const auto oldSyncBatchBytes = FLAGS_distributed_disk_sync_batch_bytes;
    FLAGS_distributed_disk_sync_interval_ms = 60000;
    FLAGS_distributed_disk_sync_batch_bytes = 1ul << 30;

    auto slotPath = MakeTempDir() + "/slot_flush_on_seal";
    Slot manager(51, slotPath, 1024);
    ASSERT_TRUE(inject::Set("slotstore.SlotWriter.Flush.After", "call()").IsOk());

    SavePayload(manager, "tenant/keyFlushA", 1, "aaaa");
    SavePayload(manager, "tenant/keyFlushB", 2, "bbbb");
    ASSERT_EQ(inject::GetExecuteCount("slotstore.SlotWriter.Flush.After"), 0u);

    ASSERT_TRUE(manager.Seal("unit_test").IsOk());
    ASSERT_EQ(inject::GetExecuteCount("slotstore.SlotWriter.Flush.After"), 1u);

    SlotSnapshot snapshot;
    ASSERT_TRUE(manager.ReplayIndex(snapshot).IsOk());
    SlotSnapshotValue valueA;
    ASSERT_TRUE(snapshot.FindExact("tenant/keyFlushA", 1, valueA).IsOk());
    SlotSnapshotValue valueB;
    ASSERT_TRUE(snapshot.FindExact("tenant/keyFlushB", 2, valueB).IsOk());

    FLAGS_distributed_disk_sync_interval_ms = oldSyncIntervalMs;
    FLAGS_distributed_disk_sync_batch_bytes = oldSyncBatchBytes;
    (void)RemoveAll(slotPath.substr(0, slotPath.find("/slot_flush_on_seal")));
}

TEST_F(SlotStoreTest, SlotCompactKeepsVisiblePutsAndRemovesTombstones)
{
    auto slotPath = MakeTempDir() + "/slot_compact";
    Slot manager(6, slotPath, 1024);
    auto body1 = std::make_shared<std::stringstream>();
    *body1 << "v1";
    ASSERT_TRUE(manager.Save("tenant/keyCompactA", 1, body1).IsOk());
    auto body2 = std::make_shared<std::stringstream>();
    *body2 << "v2";
    ASSERT_TRUE(manager.Save("tenant/keyCompactA", 2, body2).IsOk());
    auto body3 = std::make_shared<std::stringstream>();
    *body3 << "drop";
    ASSERT_TRUE(manager.Save("tenant/keyCompactB", 1, body3).IsOk());
    auto body4 = std::make_shared<std::stringstream>();
    *body4 << "keep";
    ASSERT_TRUE(manager.Save("tenant/keyCompactC", 1, body4).IsOk());
    ASSERT_TRUE(manager.Delete("tenant/keyCompactB", 1, true).IsOk());

    SlotManifestData before;
    ASSERT_TRUE(SlotManifest::Load(slotPath, before).IsOk());
    auto oldIndex = before.activeIndex;
    auto oldData = before.activeData;

    ASSERT_TRUE(manager.Compact().IsOk());

    SlotManifestData after;
    ASSERT_TRUE(SlotManifest::Load(slotPath, after).IsOk());
    ExpectManifestNormal(after);
    ASSERT_FALSE(after.gcPending);
    ASSERT_NE(after.activeIndex, oldIndex);

    auto v1Content = std::make_shared<std::stringstream>();
    ASSERT_TRUE(manager.Get("tenant/keyCompactA", 1, v1Content).IsOk());
    ASSERT_EQ(ReadAll(v1Content), "v1");
    auto v2Content = std::make_shared<std::stringstream>();
    ASSERT_TRUE(manager.Get("tenant/keyCompactA", 2, v2Content).IsOk());
    ASSERT_EQ(ReadAll(v2Content), "v2");
    auto keepContent = std::make_shared<std::stringstream>();
    ASSERT_TRUE(manager.Get("tenant/keyCompactC", 1, keepContent).IsOk());
    ASSERT_EQ(ReadAll(keepContent), "keep");
    auto deleted = std::make_shared<std::stringstream>();
    ASSERT_EQ(manager.GetWithoutVersion("tenant/keyCompactB", 0, deleted).GetCode(),
              StatusCode::K_NOT_FOUND_IN_L2CACHE);

    std::vector<SlotRecord> records;
    size_t validBytes = 0;
    ASSERT_TRUE(SlotIndexCodec::ReadAllRecords(JoinPath(slotPath, after.activeIndex), records, validBytes).IsOk());
    ASSERT_EQ(records.size(), 3u);
    for (const auto &record : records) {
        ASSERT_EQ(record.type, SlotRecordType::PUT);
    }
    ASSERT_FALSE(FileExist(JoinPath(slotPath, oldIndex)));
    for (const auto &dataFile : oldData) {
        ASSERT_FALSE(FileExist(JoinPath(slotPath, dataFile)));
    }
    (void)RemoveAll(slotPath.substr(0, slotPath.find("/slot_compact")));
}

TEST_F(SlotStoreTest, SlotCompactAbsorbsConcurrentSaveBeforeCutover)
{
    auto slotPath = MakeTempDir() + "/slot_compact_catch_up_save";
    Slot manager(12, slotPath, 1024);
    auto body1 = std::make_shared<std::stringstream>();
    *body1 << "base";
    ASSERT_TRUE(manager.Save("tenant/keyTryAgain", 1, body1).IsOk());

    ASSERT_TRUE(
        inject::Set("slotstore.Slot.Compact.BeforeCommit", "1*call(tenant/keyConflict,2,conflict_payload)").IsOk());
    auto rc = manager.Compact();
    ASSERT_TRUE(inject::Clear("slotstore.Slot.Compact.BeforeCommit").IsOk());
    ASSERT_TRUE(rc.IsOk());

    SlotManifestData manifest;
    ASSERT_TRUE(SlotManifest::Load(slotPath, manifest).IsOk());
    ExpectManifestNormal(manifest);
    ASSERT_FALSE(manifest.gcPending);

    std::vector<std::string> compactIndexes;
    ASSERT_TRUE(Glob(JoinPath(slotPath, "index_compact_*.log"), compactIndexes).IsOk());
    ASSERT_EQ(compactIndexes.size(), 1u);

    auto baseContent = std::make_shared<std::stringstream>();
    ASSERT_TRUE(manager.Get("tenant/keyTryAgain", 1, baseContent).IsOk());
    ASSERT_EQ(ReadAll(baseContent), "base");
    auto conflictContent = std::make_shared<std::stringstream>();
    ASSERT_TRUE(manager.Get("tenant/keyConflict", 2, conflictContent).IsOk());
    ASSERT_EQ(ReadAll(conflictContent), "conflict_payload");
    (void)RemoveAll(slotPath.substr(0, slotPath.find("/slot_compact_catch_up_save")));
}

TEST_F(SlotStoreTest, SlotCompactAbsorbsConcurrentDeleteBeforeCutover)
{
    auto slotPath = MakeTempDir() + "/slot_compact_catch_up_delete";
    Slot manager(13, slotPath, 1024);
    SavePayload(manager, "tenant/keyDeleteDuringCompact", 1, "old");
    SavePayload(manager, "tenant/keyDeleteDuringCompact", 2, "new");

    ASSERT_TRUE(
        inject::Set("slotstore.Slot.Compact.BeforeCommitDelete", "1*call(tenant/keyDeleteDuringCompact,1,false)")
            .IsOk());
    auto rc = manager.Compact();
    ASSERT_TRUE(inject::Clear("slotstore.Slot.Compact.BeforeCommitDelete").IsOk());
    ASSERT_TRUE(rc.IsOk());

    auto oldContent = std::make_shared<std::stringstream>();
    ASSERT_EQ(manager.Get("tenant/keyDeleteDuringCompact", 1, oldContent).GetCode(),
              StatusCode::K_NOT_FOUND_IN_L2CACHE);
    auto newContent = std::make_shared<std::stringstream>();
    ASSERT_TRUE(manager.Get("tenant/keyDeleteDuringCompact", 2, newContent).IsOk());
    ASSERT_EQ(ReadAll(newContent), "new");

    SlotManifestData manifest;
    ASSERT_TRUE(SlotManifest::Load(slotPath, manifest).IsOk());
    ExpectManifestNormal(manifest);
    (void)RemoveAll(slotPath.substr(0, slotPath.find("/slot_compact_catch_up_delete")));
}

TEST_F(SlotStoreTest, SlotClientBackgroundCompactAbsorbsConcurrentMutations)
{
    SlotClient client(baseDir_);
    ASSERT_TRUE(client.Init().IsOk());

    ASSERT_TRUE(inject::Set("slotstore.SlotClient.BackgroundCompact.WaitMs", "100*call(20)").IsOk());
    ASSERT_TRUE(inject::Set("slotstore.Slot.Compact.BeforeCommit", "1*sleep(300)").IsOk());

    const std::string keepKey = "tenant/background_keep";
    const std::string deleteKey = "tenant/background_delete";
    auto keepSlotId = static_cast<uint32_t>(std::hash<std::string>{}(keepKey) % FLAGS_distributed_disk_slot_num);
    auto slotPath = GetSlotPath(baseDir_, keepSlotId, TARGET_WORKER_ADDRESS);

    ASSERT_TRUE(client.Save(keepKey, 1, 0, MakeBody("v1"), 0, WriteMode::WRITE_THROUGH_L2_CACHE).IsOk());
    ASSERT_TRUE(client.Save(deleteKey, 1, 0, MakeBody("old"), 0, WriteMode::WRITE_THROUGH_L2_CACHE).IsOk());

    ASSERT_TRUE(WaitUntil([&]() { return inject::GetExecuteCount("slotstore.Slot.Compact.BeforeCommit") >= 1; }));

    ASSERT_TRUE(client.Save(keepKey, 2, 0, MakeBody("v2"), 0, WriteMode::WRITE_THROUGH_L2_CACHE).IsOk());
    ASSERT_TRUE(client.Delete(deleteKey, UINT64_MAX, true).IsOk());

    ASSERT_TRUE(WaitUntil([&]() {
        SlotManifestData manifest;
        auto rc = SlotManifest::Load(slotPath, manifest);
        return rc.IsOk() && manifest.lastCompactEpochMs > 0 && manifest.state == SlotState::NORMAL
               && manifest.opType == SlotOperationType::NONE;
    }));

    auto contentV1 = std::make_shared<std::stringstream>();
    ASSERT_TRUE(client.Get(keepKey, 1, 0, contentV1).IsOk());
    ASSERT_EQ(ReadAll(contentV1), "v1");
    auto contentV2 = std::make_shared<std::stringstream>();
    ASSERT_TRUE(client.Get(keepKey, 2, 0, contentV2).IsOk());
    ASSERT_EQ(ReadAll(contentV2), "v2");
    auto deletedContent = std::make_shared<std::stringstream>();
    ASSERT_EQ(client.GetWithoutVersion(deleteKey, 0, 0, deletedContent).GetCode(), StatusCode::K_NOT_FOUND_IN_L2CACHE);

    SlotManifestData manifest;
    ASSERT_TRUE(SlotManifest::Load(slotPath, manifest).IsOk());
    ASSERT_GT(manifest.lastCompactEpochMs, 0u);

    ASSERT_TRUE(inject::Clear("slotstore.Slot.Compact.BeforeCommit").IsOk());
    ASSERT_TRUE(inject::Clear("slotstore.SlotClient.BackgroundCompact.WaitMs").IsOk());
}

TEST_F(SlotStoreTest, SlotCompactorBuildArtifactsCleansResidualFilesOnFailure)
{
    auto slotPath = MakeTempDir() + "/slot_compactor_cleanup";
    Slot manager(10, slotPath, 1024);
    auto body = std::make_shared<std::stringstream>();
    *body << "cleanup";
    ASSERT_TRUE(manager.Save("tenant/keyCleanup", 1, body).IsOk());

    SlotManifestData manifest;
    ASSERT_TRUE(SlotManifest::Load(slotPath, manifest).IsOk());
    SlotSnapshot snapshot;
    ASSERT_TRUE(manager.ReplayIndex(snapshot).IsOk());
    ASSERT_TRUE(DeleteFile(JoinPath(slotPath, manifest.activeData.front())).IsOk());

    SlotCompactor compactor(slotPath, 1024);
    SlotCompactBuildResult buildResult;
    auto rc = compactor.BuildArtifacts(manifest, snapshot, 1001, buildResult);
    ASSERT_TRUE(rc.IsError());
    ASSERT_FALSE(FileExist(JoinPath(slotPath, FormatCompactIndexFileName(1001))));
    ASSERT_FALSE(FileExist(JoinPath(slotPath, FormatDataFileName(2))));
    (void)RemoveAll(slotPath.substr(0, slotPath.find("/slot_compactor_cleanup")));
}

TEST_F(SlotStoreTest, SlotCompactorBuildArtifactsAllocatesUniqueCompactIndexFile)
{
    auto slotPath = MakeTempDir() + "/slot_compactor_unique";
    Slot manager(10, slotPath, 1024);
    auto body = std::make_shared<std::stringstream>();
    *body << "unique";
    ASSERT_TRUE(manager.Save("tenant/keyUnique", 1, body).IsOk());

    SlotManifestData manifest;
    ASSERT_TRUE(SlotManifest::Load(slotPath, manifest).IsOk());
    SlotSnapshot snapshot;
    ASSERT_TRUE(manager.ReplayIndex(snapshot).IsOk());

    SlotCompactor compactor(slotPath, 1024);
    SlotCompactBuildResult firstBuild;
    SlotCompactBuildResult secondBuild;
    ASSERT_TRUE(compactor.BuildArtifacts(manifest, snapshot, 999, firstBuild).IsOk());
    ASSERT_TRUE(compactor.BuildArtifacts(manifest, snapshot, 999, secondBuild).IsOk());

    ASSERT_NE(firstBuild.indexFile, secondBuild.indexFile);
    ASSERT_TRUE(FileExist(JoinPath(slotPath, firstBuild.indexFile)));
    ASSERT_TRUE(FileExist(JoinPath(slotPath, secondBuild.indexFile)));

    ASSERT_TRUE(compactor.CleanupArtifacts(firstBuild).IsOk());
    ASSERT_TRUE(compactor.CleanupArtifacts(secondBuild).IsOk());
    (void)RemoveAll(slotPath.substr(0, slotPath.find("/slot_compactor_unique")));
}

TEST_F(SlotStoreTest, SlotRebuildsManifestFromCompactedDirectoryWhenManifestMissing)
{
    auto slotPath = MakeTempDir() + "/slot_manifest_rebuild_after_compact";
    Slot manager(11, slotPath, 1024);
    auto body1 = std::make_shared<std::stringstream>();
    *body1 << "old";
    ASSERT_TRUE(manager.Save("tenant/keyManifestRebuild", 1, body1).IsOk());
    auto body2 = std::make_shared<std::stringstream>();
    *body2 << "new";
    ASSERT_TRUE(manager.Save("tenant/keyManifestRebuild", 2, body2).IsOk());
    ASSERT_TRUE(manager.Compact().IsOk());

    SlotManifestData compacted;
    ASSERT_TRUE(SlotManifest::Load(slotPath, compacted).IsOk());
    ASSERT_TRUE(DeleteFile(JoinPath(slotPath, "manifest")).IsOk());

    auto content = std::make_shared<std::stringstream>();
    ASSERT_TRUE(manager.Get("tenant/keyManifestRebuild", 2, content).IsOk());
    ASSERT_EQ(ReadAll(content), "new");

    SlotManifestData rebuilt;
    ASSERT_TRUE(SlotManifest::Load(slotPath, rebuilt).IsOk());
    ASSERT_EQ(rebuilt.activeIndex, compacted.activeIndex);
    ASSERT_EQ(rebuilt.activeData, compacted.activeData);
    ExpectManifestNormal(rebuilt);
    ASSERT_FALSE(rebuilt.gcPending);

    auto body3 = std::make_shared<std::stringstream>();
    *body3 << "after_rebuild";
    ASSERT_TRUE(manager.Save("tenant/keyManifestRebuild2", 3, body3).IsOk());
    auto newContent = std::make_shared<std::stringstream>();
    ASSERT_TRUE(manager.Get("tenant/keyManifestRebuild2", 3, newContent).IsOk());
    ASSERT_EQ(ReadAll(newContent), "after_rebuild");
    (void)RemoveAll(slotPath.substr(0, slotPath.find("/slot_manifest_rebuild_after_compact")));
}

TEST_F(SlotStoreTest, SlotRepairForwardCompletesSwitching)
{
    auto slotPath = MakeTempDir() + "/slot_switch_forward";
    Slot manager(7, slotPath, 1024);
    auto body = std::make_shared<std::stringstream>();
    *body << "forward";
    ASSERT_TRUE(manager.Save("tenant/keyForward", 1, body).IsOk());

    SlotManifestData manifest;
    ASSERT_TRUE(SlotManifest::Load(slotPath, manifest).IsOk());
    SlotSnapshot snapshot;
    ASSERT_TRUE(manager.ReplayIndex(snapshot).IsOk());
    SlotCompactor compactor(slotPath, 1024);
    SlotCompactBuildResult buildResult;
    ASSERT_TRUE(compactor.BuildArtifacts(manifest, snapshot, 777, buildResult).IsOk());

    SlotManifestData switching = manifest;
    PersistCompactCommittingManifest(slotPath, switching, buildResult.indexFile, buildResult.dataFiles, 777);

    ASSERT_TRUE(manager.Repair().IsOk());

    SlotManifestData repaired;
    ASSERT_TRUE(SlotManifest::Load(slotPath, repaired).IsOk());
    ExpectManifestNormal(repaired);
    ASSERT_TRUE(repaired.pendingIndex.empty());
    ASSERT_TRUE(repaired.pendingData.empty());
    ASSERT_FALSE(repaired.gcPending);
    ASSERT_EQ(repaired.activeIndex, buildResult.indexFile);
    auto content = std::make_shared<std::stringstream>();
    ASSERT_TRUE(manager.Get("tenant/keyForward", 1, content).IsOk());
    ASSERT_EQ(ReadAll(content), "forward");
    ASSERT_FALSE(FileExist(JoinPath(slotPath, manifest.activeIndex)));
    for (const auto &dataFile : manifest.activeData) {
        ASSERT_FALSE(FileExist(JoinPath(slotPath, dataFile)));
    }
    (void)RemoveAll(slotPath.substr(0, slotPath.find("/slot_switch_forward")));
}

TEST_F(SlotStoreTest, SlotRepairRollbackDropsIncompletePendingArtifacts)
{
    auto slotPath = MakeTempDir() + "/slot_switch_rollback";
    Slot manager(8, slotPath, 1024);
    auto body = std::make_shared<std::stringstream>();
    *body << "rollback";
    ASSERT_TRUE(manager.Save("tenant/keyRollback", 1, body).IsOk());

    SlotManifestData manifest;
    ASSERT_TRUE(SlotManifest::Load(slotPath, manifest).IsOk());
    SlotManifestData switching = manifest;
    PersistCompactCommittingManifest(slotPath, switching, "index_compact_missing.log", { "data_000099.bin" }, 999);

    ASSERT_TRUE(manager.Repair().IsOk());

    SlotManifestData repaired;
    ASSERT_TRUE(SlotManifest::Load(slotPath, repaired).IsOk());
    ExpectManifestNormal(repaired);
    ASSERT_EQ(repaired.activeIndex, manifest.activeIndex);
    ASSERT_EQ(repaired.activeData, manifest.activeData);
    ASSERT_TRUE(repaired.pendingIndex.empty());
    ASSERT_TRUE(repaired.pendingData.empty());
    auto content = std::make_shared<std::stringstream>();
    ASSERT_TRUE(manager.Get("tenant/keyRollback", 1, content).IsOk());
    ASSERT_EQ(ReadAll(content), "rollback");
    (void)RemoveAll(slotPath.substr(0, slotPath.find("/slot_switch_rollback")));
}

TEST_F(SlotStoreTest, SlotGetRecoversGcPendingState)
{
    auto slotPath = MakeTempDir() + "/slot_gc_pending";
    Slot manager(9, slotPath, 1024);
    auto body = std::make_shared<std::stringstream>();
    *body << "gc";
    ASSERT_TRUE(manager.Save("tenant/keyGc", 1, body).IsOk());

    SlotManifestData manifest;
    ASSERT_TRUE(SlotManifest::Load(slotPath, manifest).IsOk());
    auto oldIndex = manifest.activeIndex;
    auto oldData = manifest.activeData;
    SlotSnapshot snapshot;
    ASSERT_TRUE(manager.ReplayIndex(snapshot).IsOk());
    SlotCompactor compactor(slotPath, 1024);
    SlotCompactBuildResult buildResult;
    ASSERT_TRUE(compactor.BuildArtifacts(manifest, snapshot, 888, buildResult).IsOk());

    SlotManifestData active = manifest;
    active.activeIndex = buildResult.indexFile;
    active.activeData = buildResult.dataFiles;
    active.obsoleteIndex = oldIndex;
    active.obsoleteData = oldData;
    active.gcPending = true;
    active.lastCompactEpochMs = 888;
    ASSERT_TRUE(SlotManifest::StoreAtomic(slotPath, active).IsOk());

    auto content = std::make_shared<std::stringstream>();
    ASSERT_TRUE(manager.Get("tenant/keyGc", 1, content).IsOk());
    ASSERT_EQ(ReadAll(content), "gc");

    SlotManifestData repaired;
    ASSERT_TRUE(SlotManifest::Load(slotPath, repaired).IsOk());
    ASSERT_FALSE(repaired.gcPending);
    ASSERT_TRUE(repaired.obsoleteIndex.empty());
    ASSERT_TRUE(repaired.obsoleteData.empty());
    ASSERT_FALSE(FileExist(JoinPath(slotPath, oldIndex)));
    for (const auto &dataFile : oldData) {
        ASSERT_FALSE(FileExist(JoinPath(slotPath, dataFile)));
    }
    (void)RemoveAll(slotPath.substr(0, slotPath.find("/slot_gc_pending")));
}

TEST_F(SlotStoreTest, SlotClientMergeRepairsSwitchingTargetBeforeTakeover)
{
    SlotClient client(baseDir_);
    ASSERT_TRUE(client.Init().IsOk());

    auto slotId = 2u;
    auto sourcePath = GetSlotPath(baseDir_, slotId, SOURCE_WORKER_ADDRESS_A);
    auto targetPath = GetSlotPath(baseDir_, slotId, TARGET_WORKER_ADDRESS);
    Slot sourceManager(slotId, sourcePath, 4);
    Slot targetManager(slotId, targetPath, 4);

    SavePayload(targetManager, "tenant/target_before_compact_a", 1, "aaaa");
    SavePayload(targetManager, "tenant/target_before_compact_b", 2, "bbbb");
    SavePayload(sourceManager, "tenant/source_takeover_a", 3, "cccc");
    SavePayload(sourceManager, "tenant/source_takeover_b", 4, "dddd");
    ASSERT_TRUE(sourceManager.Seal("merge_switching").IsOk());

    SlotManifestData targetManifest;
    ASSERT_TRUE(SlotManifest::Load(targetPath, targetManifest).IsOk());
    auto oldTargetIndex = targetManifest.activeIndex;
    auto oldTargetData = targetManifest.activeData;
    SlotSnapshot targetSnapshot;
    ASSERT_TRUE(targetManager.ReplayIndex(targetSnapshot).IsOk());
    SlotCompactor compactor(targetPath, 4);
    SlotCompactBuildResult buildResult;
    ASSERT_TRUE(compactor.BuildArtifacts(targetManifest, targetSnapshot, 1901, buildResult).IsOk());

    SlotManifestData switching = targetManifest;
    PersistCompactCommittingManifest(targetPath, switching, buildResult.indexFile, buildResult.dataFiles, 1901);

    ASSERT_TRUE(client.MergeSlot(SOURCE_WORKER_ADDRESS_A, slotId).IsOk());

    auto contentTargetA = std::make_shared<std::stringstream>();
    ASSERT_TRUE(targetManager.Get("tenant/target_before_compact_a", 1, contentTargetA).IsOk());
    ASSERT_EQ(ReadAll(contentTargetA), "aaaa");
    auto contentTargetB = std::make_shared<std::stringstream>();
    ASSERT_TRUE(targetManager.Get("tenant/target_before_compact_b", 2, contentTargetB).IsOk());
    ASSERT_EQ(ReadAll(contentTargetB), "bbbb");
    auto contentSourceA = std::make_shared<std::stringstream>();
    ASSERT_TRUE(targetManager.Get("tenant/source_takeover_a", 3, contentSourceA).IsOk());
    ASSERT_EQ(ReadAll(contentSourceA), "cccc");
    auto contentSourceB = std::make_shared<std::stringstream>();
    ASSERT_TRUE(targetManager.Get("tenant/source_takeover_b", 4, contentSourceB).IsOk());
    ASSERT_EQ(ReadAll(contentSourceB), "dddd");

    SlotManifestData repairedTarget;
    ASSERT_TRUE(SlotManifest::Load(targetPath, repairedTarget).IsOk());
    ExpectManifestNormal(repairedTarget);
    ASSERT_TRUE(repairedTarget.pendingIndex.empty());
    ASSERT_TRUE(repairedTarget.pendingData.empty());
    ASSERT_FALSE(repairedTarget.gcPending);
    ASSERT_EQ(repairedTarget.activeIndex, buildResult.indexFile);
    ASSERT_FALSE(FileExist(JoinPath(targetPath, oldTargetIndex)));
    for (const auto &dataFile : oldTargetData) {
        ASSERT_FALSE(FileExist(JoinPath(targetPath, dataFile)));
    }

    ASSERT_FALSE(FileExist(sourcePath));
}

TEST_F(SlotStoreTest, SlotClientMergeRollsBackIncompleteSwitchingTargetBeforeTakeover)
{
    SlotClient client(baseDir_);
    ASSERT_TRUE(client.Init().IsOk());

    auto slotId = 0u;
    auto sourcePath = GetSlotPath(baseDir_, slotId, SOURCE_WORKER_ADDRESS_A);
    auto targetPath = GetSlotPath(baseDir_, slotId, TARGET_WORKER_ADDRESS);
    Slot sourceManager(slotId, sourcePath, 1024);
    Slot targetManager(slotId, targetPath, 1024);

    SavePayload(targetManager, "tenant/target_rollback", 1, "stable");
    SavePayload(sourceManager, "tenant/source_after_rollback", 2, "incoming");
    ASSERT_TRUE(sourceManager.Seal("merge_rollback").IsOk());

    SlotManifestData targetManifest;
    ASSERT_TRUE(SlotManifest::Load(targetPath, targetManifest).IsOk());
    auto originalIndex = targetManifest.activeIndex;
    auto originalData = targetManifest.activeData;
    SlotManifestData switching = targetManifest;
    PersistCompactCommittingManifest(targetPath, switching, "index_compact_missing_takeover.log", { "data_009999.bin" },
                                     1200);

    ASSERT_TRUE(client.MergeSlot(SOURCE_WORKER_ADDRESS_A, slotId).IsOk());

    auto stableContent = std::make_shared<std::stringstream>();
    ASSERT_TRUE(targetManager.Get("tenant/target_rollback", 1, stableContent).IsOk());
    ASSERT_EQ(ReadAll(stableContent), "stable");
    auto incomingContent = std::make_shared<std::stringstream>();
    ASSERT_TRUE(targetManager.Get("tenant/source_after_rollback", 2, incomingContent).IsOk());
    ASSERT_EQ(ReadAll(incomingContent), "incoming");

    SlotManifestData targetAfterMerge;
    ASSERT_TRUE(SlotManifest::Load(targetPath, targetAfterMerge).IsOk());
    ExpectManifestNormal(targetAfterMerge);
    ASSERT_TRUE(targetAfterMerge.pendingIndex.empty());
    ASSERT_TRUE(targetAfterMerge.pendingData.empty());
    ASSERT_EQ(targetAfterMerge.obsoleteIndex, "");
    ASSERT_EQ(targetAfterMerge.activeIndex, originalIndex);
    ASSERT_TRUE(std::find(targetAfterMerge.activeData.begin(), targetAfterMerge.activeData.end(), originalData.front())
                != targetAfterMerge.activeData.end());
}

TEST_F(SlotStoreTest, SlotClientPreloadClearsGcPendingBeforeTakeover)
{
    SlotClient client(baseDir_);
    ASSERT_TRUE(client.Init().IsOk());

    auto slotId = 2u;
    auto sourcePath = GetSlotPath(baseDir_, slotId, SOURCE_WORKER_ADDRESS_A);
    auto targetPath = GetSlotPath(baseDir_, slotId, TARGET_WORKER_ADDRESS);
    Slot sourceManager(slotId, sourcePath, 1024);
    Slot targetManager(slotId, targetPath, 1024);

    SavePayload(targetManager, "tenant/target_gc_keep", 1, "keep");
    SavePayload(targetManager, "tenant/target_gc_newer", 2, "newer");
    SavePayload(sourceManager, "tenant/preload_gc_a", 3, "alpha");
    SavePayload(sourceManager, "tenant/preload_gc_b", 4, "beta");
    ASSERT_TRUE(sourceManager.Seal("preload_gc").IsOk());

    SlotManifestData targetManifest;
    ASSERT_TRUE(SlotManifest::Load(targetPath, targetManifest).IsOk());
    auto oldIndex = targetManifest.activeIndex;
    auto oldData = targetManifest.activeData;
    SlotSnapshot targetSnapshot;
    ASSERT_TRUE(targetManager.ReplayIndex(targetSnapshot).IsOk());
    SlotCompactor compactor(targetPath, 1024);
    SlotCompactBuildResult buildResult;
    ASSERT_TRUE(compactor.BuildArtifacts(targetManifest, targetSnapshot, 2901, buildResult).IsOk());

    SlotManifestData active = targetManifest;
    active.activeIndex = buildResult.indexFile;
    active.activeData = buildResult.dataFiles;
    active.obsoleteIndex = oldIndex;
    active.obsoleteData = oldData;
    active.gcPending = true;
    active.lastCompactEpochMs = 2901;
    ASSERT_TRUE(SlotManifest::StoreAtomic(targetPath, active).IsOk());

    ASSERT_TRUE(client.PreloadSlot(SOURCE_WORKER_ADDRESS_A, slotId).IsOk());

    auto keepContent = std::make_shared<std::stringstream>();
    ASSERT_TRUE(targetManager.Get("tenant/target_gc_keep", 1, keepContent).IsOk());
    ASSERT_EQ(ReadAll(keepContent), "keep");
    auto newerContent = std::make_shared<std::stringstream>();
    ASSERT_TRUE(targetManager.Get("tenant/target_gc_newer", 2, newerContent).IsOk());
    ASSERT_EQ(ReadAll(newerContent), "newer");
    auto preloadA = std::make_shared<std::stringstream>();
    ASSERT_TRUE(targetManager.Get("tenant/preload_gc_a", 3, preloadA).IsOk());
    ASSERT_EQ(ReadAll(preloadA), "alpha");
    auto preloadB = std::make_shared<std::stringstream>();
    ASSERT_TRUE(targetManager.Get("tenant/preload_gc_b", 4, preloadB).IsOk());
    ASSERT_EQ(ReadAll(preloadB), "beta");

    SlotManifestData targetAfterPreload;
    ASSERT_TRUE(SlotManifest::Load(targetPath, targetAfterPreload).IsOk());
    ASSERT_FALSE(targetAfterPreload.gcPending);
    ASSERT_TRUE(targetAfterPreload.obsoleteIndex.empty());
    ASSERT_TRUE(targetAfterPreload.obsoleteData.empty());
    ASSERT_FALSE(FileExist(JoinPath(targetPath, oldIndex)));
    for (const auto &dataFile : oldData) {
        ASSERT_FALSE(FileExist(JoinPath(targetPath, dataFile)));
    }
}

TEST_F(SlotStoreTest, SlotCompactTransferIntentPreemptsCompaction)
{
    auto sourcePath = MakeTempDir() + "/compact_preempt_source";
    auto targetPath = MakeTempDir() + "/compact_preempt_target";
    Slot sourceManager(1, sourcePath, 1024);
    Slot targetManager(2, targetPath, 1024);
    SavePayload(targetManager, "tenant/targetBeforePreempt", 1, "target");
    SavePayload(sourceManager, "tenant/sourcePreemptA", 1, "a");
    SavePayload(sourceManager, "tenant/sourcePreemptB", 2, "b");

    ASSERT_TRUE(inject::Set("slotstore.Slot.Compact.BeforeCommitTransfer", "1*call(" + sourcePath + ",false)").IsOk());
    auto rc = targetManager.Compact();
    ASSERT_TRUE(inject::Clear("slotstore.Slot.Compact.BeforeCommitTransfer").IsOk());
    ASSERT_EQ(rc.GetCode(), StatusCode::K_TRY_AGAIN);

    SlotManifestData manifest;
    ASSERT_TRUE(SlotManifest::Load(targetPath, manifest).IsOk());
    ExpectManifestNormal(manifest);

    auto existingContent = std::make_shared<std::stringstream>();
    ASSERT_TRUE(targetManager.Get("tenant/targetBeforePreempt", 1, existingContent).IsOk());
    ASSERT_EQ(ReadAll(existingContent), "target");
    auto sourceA = std::make_shared<std::stringstream>();
    ASSERT_TRUE(targetManager.Get("tenant/sourcePreemptA", 1, sourceA).IsOk());
    ASSERT_EQ(ReadAll(sourceA), "a");
    auto sourceB = std::make_shared<std::stringstream>();
    ASSERT_TRUE(targetManager.Get("tenant/sourcePreemptB", 2, sourceB).IsOk());
    ASSERT_EQ(ReadAll(sourceB), "b");

    std::vector<std::string> compactIndexes;
    ASSERT_TRUE(Glob(JoinPath(targetPath, "index_compact_*.log"), compactIndexes).IsOk());
    ASSERT_TRUE(compactIndexes.empty());
    (void)RemoveAll(sourcePath.substr(0, sourcePath.find("/compact_preempt_source")));
    (void)RemoveAll(targetPath.substr(0, targetPath.find("/compact_preempt_target")));
}

TEST_F(SlotStoreTest, SlotSealHintDoesNotBlockCompact)
{
    auto slotPath = MakeTempDir() + "/slot_sealed_compact";
    Slot manager(11, slotPath, 4);
    SavePayload(manager, "tenant/sealed_compact_a", 1, "aaaa");
    SavePayload(manager, "tenant/sealed_compact_b", 2, "bbbb");
    ASSERT_TRUE(manager.Seal("sealed_compact").IsOk());

    auto rc = manager.Compact();
    ASSERT_TRUE(rc.IsOk());

    auto contentA = std::make_shared<std::stringstream>();
    ASSERT_TRUE(manager.Get("tenant/sealed_compact_a", 1, contentA).IsOk());
    ASSERT_EQ(ReadAll(contentA), "aaaa");
    auto contentB = std::make_shared<std::stringstream>();
    ASSERT_TRUE(manager.Get("tenant/sealed_compact_b", 2, contentB).IsOk());
    ASSERT_EQ(ReadAll(contentB), "bbbb");
    (void)RemoveAll(slotPath.substr(0, slotPath.find("/slot_sealed_compact")));
}

TEST_F(SlotStoreTest, SlotClientMergeIgnoresStaleCompactBuildArtifacts)
{
    SlotClient client(baseDir_);
    ASSERT_TRUE(client.Init().IsOk());

    auto slotId = 2u;
    auto sourcePath = GetSlotPath(baseDir_, slotId, SOURCE_WORKER_ADDRESS_A);
    auto targetPath = GetSlotPath(baseDir_, slotId, TARGET_WORKER_ADDRESS);
    Slot sourceManager(slotId, sourcePath, 4);
    Slot targetManager(slotId, targetPath, 4);

    SavePayload(targetManager, "tenant/target_existing_a", 1, "aaaa");
    SavePayload(targetManager, "tenant/target_existing_b", 2, "bbbb");
    SavePayload(sourceManager, "tenant/source_merge_c", 3, "cccc");
    SavePayload(sourceManager, "tenant/source_merge_d", 4, "dddd");
    ASSERT_TRUE(sourceManager.Seal("merge_stale_build").IsOk());

    SlotManifestData targetManifest;
    ASSERT_TRUE(SlotManifest::Load(targetPath, targetManifest).IsOk());
    SlotSnapshot targetSnapshot;
    ASSERT_TRUE(targetManager.ReplayIndex(targetSnapshot).IsOk());
    SlotCompactor compactor(targetPath, 4);
    SlotCompactBuildResult buildResult;
    ASSERT_TRUE(compactor.BuildArtifacts(targetManifest, targetSnapshot, 3901, buildResult).IsOk());
    ASSERT_TRUE(FileExist(JoinPath(targetPath, buildResult.indexFile)));
    ASSERT_FALSE(buildResult.dataFiles.empty());
    for (const auto &dataFile : buildResult.dataFiles) {
        ASSERT_TRUE(FileExist(JoinPath(targetPath, dataFile)));
    }

    ASSERT_TRUE(client.MergeSlot(SOURCE_WORKER_ADDRESS_A, slotId).IsOk());

    auto existingA = std::make_shared<std::stringstream>();
    ASSERT_TRUE(targetManager.Get("tenant/target_existing_a", 1, existingA).IsOk());
    ASSERT_EQ(ReadAll(existingA), "aaaa");
    auto existingB = std::make_shared<std::stringstream>();
    ASSERT_TRUE(targetManager.Get("tenant/target_existing_b", 2, existingB).IsOk());
    ASSERT_EQ(ReadAll(existingB), "bbbb");
    auto mergedC = std::make_shared<std::stringstream>();
    ASSERT_TRUE(targetManager.Get("tenant/source_merge_c", 3, mergedC).IsOk());
    ASSERT_EQ(ReadAll(mergedC), "cccc");
    auto mergedD = std::make_shared<std::stringstream>();
    ASSERT_TRUE(targetManager.Get("tenant/source_merge_d", 4, mergedD).IsOk());
    ASSERT_EQ(ReadAll(mergedD), "dddd");

    SlotManifestData targetAfterMerge;
    ASSERT_TRUE(SlotManifest::Load(targetPath, targetAfterMerge).IsOk());
    ASSERT_NE(targetAfterMerge.activeIndex, buildResult.indexFile);
    ASSERT_TRUE(
        std::find(targetAfterMerge.activeData.begin(), targetAfterMerge.activeData.end(), buildResult.dataFiles.front())
        == targetAfterMerge.activeData.end());

    ASSERT_TRUE(targetManager.Compact().IsOk());
    auto mergedAfterCompact = std::make_shared<std::stringstream>();
    ASSERT_TRUE(targetManager.Get("tenant/source_merge_c", 3, mergedAfterCompact).IsOk());
    ASSERT_EQ(ReadAll(mergedAfterCompact), "cccc");
}

TEST_F(SlotStoreTest, SlotCompactHandlesLargeScaleDataset)
{
    auto slotPath = MakeTempDir() + "/slot_large_compact";
    Slot manager(21, slotPath, 256 * 1024);
    PopulateLargeScaleDataset(manager, "tenant/large_compact", LARGE_SCALE_KEY_COUNT, LARGE_SCALE_PAYLOAD_BYTES);

    SlotManifestData beforeCompact;
    ASSERT_TRUE(SlotManifest::Load(slotPath, beforeCompact).IsOk());
    ASSERT_GT(beforeCompact.activeData.size(), 1u);

    ASSERT_TRUE(manager.Compact().IsOk());

    SlotManifestData afterCompact;
    ASSERT_TRUE(SlotManifest::Load(slotPath, afterCompact).IsOk());
    ExpectManifestNormal(afterCompact);
    ASSERT_FALSE(afterCompact.gcPending);
    ASSERT_TRUE(afterCompact.pendingIndex.empty());
    ASSERT_TRUE(afterCompact.pendingData.empty());
    ASSERT_GT(afterCompact.activeData.size(), 1u);

    VerifyLargeScaleDataset(manager, "tenant/large_compact", LARGE_SCALE_KEY_COUNT, LARGE_SCALE_PAYLOAD_BYTES);
    (void)RemoveAll(slotPath.substr(0, slotPath.find("/slot_large_compact")));
}

TEST_F(SlotStoreTest, SlotClientMergeHandlesLargeScaleDataset)
{
    SlotClient client(baseDir_);
    ASSERT_TRUE(client.Init().IsOk());

    auto slotId = 2u;
    auto sourcePath = GetSlotPath(baseDir_, slotId, SOURCE_WORKER_ADDRESS_A);
    auto targetPath = GetSlotPath(baseDir_, slotId, TARGET_WORKER_ADDRESS);
    Slot sourceManager(slotId, sourcePath, 1024 * 1024);
    Slot targetManager(slotId, targetPath, 1024 * 1024);

    PopulateLargeScaleDataset(sourceManager, "tenant/large_merge_source", LARGE_SCALE_KEY_COUNT,
                              LARGE_SCALE_PAYLOAD_BYTES);
    PopulateLargeScaleDataset(targetManager, "tenant/large_merge_target", LARGE_SCALE_TARGET_KEY_COUNT,
                              LARGE_SCALE_PAYLOAD_BYTES);

    SlotManifestData sourceBeforeMerge;
    SlotManifestData targetBeforeMerge;
    ASSERT_TRUE(SlotManifest::Load(sourcePath, sourceBeforeMerge).IsOk());
    ASSERT_TRUE(SlotManifest::Load(targetPath, targetBeforeMerge).IsOk());
    ASSERT_GT(sourceBeforeMerge.activeData.size(), 1u);
    ASSERT_GT(targetBeforeMerge.activeData.size(), 1u);

    ASSERT_TRUE(sourceManager.Seal("large_merge").IsOk());
    ASSERT_TRUE(client.MergeSlot(SOURCE_WORKER_ADDRESS_A, slotId).IsOk());

    VerifyLargeScaleDataset(targetManager, "tenant/large_merge_source", LARGE_SCALE_KEY_COUNT,
                            LARGE_SCALE_PAYLOAD_BYTES);
    VerifyLargeScaleDataset(targetManager, "tenant/large_merge_target", LARGE_SCALE_TARGET_KEY_COUNT,
                            LARGE_SCALE_PAYLOAD_BYTES);

    SlotManifestData sourceAfterMerge;
    SlotManifestData targetAfterMerge;
    ASSERT_TRUE(SlotManifest::Load(targetPath, targetAfterMerge).IsOk());
    ASSERT_FALSE(FileExist(sourcePath));
    ExpectManifestNormal(targetAfterMerge);
    ASSERT_GT(targetAfterMerge.activeData.size(), targetBeforeMerge.activeData.size());
}

TEST_F(SlotStoreTest, SlotClientPreloadHandlesLargeScaleDataset)
{
    SlotClient client(baseDir_);
    ASSERT_TRUE(client.Init().IsOk());

    auto slotId = 0u;
    auto sourcePath = GetSlotPath(baseDir_, slotId, SOURCE_WORKER_ADDRESS_A);
    auto targetPath = GetSlotPath(baseDir_, slotId, TARGET_WORKER_ADDRESS);
    Slot sourceManager(slotId, sourcePath, 1024 * 1024);
    Slot targetManager(slotId, targetPath, 1024 * 1024);

    PopulateLargeScaleDataset(sourceManager, "tenant/large_preload_source", LARGE_SCALE_KEY_COUNT,
                              LARGE_SCALE_PAYLOAD_BYTES);
    PopulateLargeScaleDataset(targetManager, "tenant/large_preload_target", LARGE_SCALE_TARGET_KEY_COUNT,
                              LARGE_SCALE_PAYLOAD_BYTES);

    SlotManifestData sourceBeforePreload;
    SlotManifestData targetBeforePreload;
    ASSERT_TRUE(SlotManifest::Load(sourcePath, sourceBeforePreload).IsOk());
    ASSERT_TRUE(SlotManifest::Load(targetPath, targetBeforePreload).IsOk());
    ASSERT_GT(sourceBeforePreload.activeData.size(), 1u);
    ASSERT_GT(targetBeforePreload.activeData.size(), 1u);

    ASSERT_TRUE(sourceManager.Seal("large_preload").IsOk());
    ASSERT_TRUE(client.PreloadSlot(SOURCE_WORKER_ADDRESS_A, slotId).IsOk());

    VerifyLargeScaleDataset(targetManager, "tenant/large_preload_source", LARGE_SCALE_KEY_COUNT,
                            LARGE_SCALE_PAYLOAD_BYTES);
    VerifyLargeScaleDataset(targetManager, "tenant/large_preload_target", LARGE_SCALE_TARGET_KEY_COUNT,
                            LARGE_SCALE_PAYLOAD_BYTES);

    SlotManifestData sourceAfterPreload;
    SlotManifestData targetAfterPreload;
    ASSERT_TRUE(SlotManifest::Load(targetPath, targetAfterPreload).IsOk());
    ASSERT_FALSE(FileExist(sourcePath));
    ExpectManifestNormal(targetAfterPreload);
    ASSERT_GT(targetAfterPreload.activeData.size(), targetBeforePreload.activeData.size());
}

TEST_F(SlotStoreTest, SlotClientPreloadLocalWorkerReadsVisibleData)
{
    SlotClient client(baseDir_);
    ASSERT_TRUE(client.Init().IsOk());

    auto slotId = 1u;
    auto localPath = GetSlotPath(baseDir_, slotId, TARGET_WORKER_ADDRESS);
    Slot localManager(slotId, localPath, 1024);
    SavePayload(localManager, "tenant/local_preload_a", 1, "alpha");
    SavePayload(localManager, "tenant/local_preload_b", 2, "beta");

    std::vector<std::pair<std::string, std::string>> loaded;
    SlotPreloadCallback callback = [&loaded](const SlotPreloadMeta &meta,
                                             const std::shared_ptr<std::stringstream> &content) {
        loaded.emplace_back(meta.objectKey, content->str());
        EXPECT_EQ(meta.size, content->str().size());
        return Status::OK();
    };

    ASSERT_TRUE(client.PreloadSlot(TARGET_WORKER_ADDRESS, slotId, callback).IsOk());
    ASSERT_EQ(loaded.size(), 2u);
    ASSERT_EQ(loaded[0].first, "tenant/local_preload_a");
    ASSERT_EQ(loaded[0].second, "alpha");
    ASSERT_EQ(loaded[1].first, "tenant/local_preload_b");
    ASSERT_EQ(loaded[1].second, "beta");

    ASSERT_TRUE(FileExist(localPath));
    std::vector<std::string> recoveryPaths;
    ASSERT_TRUE(Glob(localPath + ".recovery.*", recoveryPaths).IsOk());
    ASSERT_TRUE(recoveryPaths.empty());

    auto contentA = std::make_shared<std::stringstream>();
    ASSERT_TRUE(localManager.Get("tenant/local_preload_a", 1, contentA).IsOk());
    ASSERT_EQ(ReadAll(contentA), "alpha");
    auto contentB = std::make_shared<std::stringstream>();
    ASSERT_TRUE(localManager.Get("tenant/local_preload_b", 2, contentB).IsOk());
    ASSERT_EQ(ReadAll(contentB), "beta");
}

TEST_F(SlotStoreTest, PreloadStopsCallbackOnOom)
{
    size_t callbackInvoked = 0;
    RunPreloadCallbackStopCase(baseDir_, SOURCE_WORKER_ADDRESS_A, 2u, "tenant/preloadCallbackOom",
                               Status(StatusCode::K_OUT_OF_MEMORY, "stop on oom"), callbackInvoked);
    ASSERT_EQ(callbackInvoked, 1u);
}

TEST_F(SlotStoreTest, PreloadStopsCallbackOnError)
{
    size_t callbackInvoked = 0;
    RunPreloadCallbackStopCase(baseDir_, SOURCE_WORKER_ADDRESS_B, 0u, "tenant/preloadCallbackErr",
                               Status(StatusCode::K_IO_ERROR, "stop on other error"), callbackInvoked);
    ASSERT_EQ(callbackInvoked, 1u);
}

TEST_F(SlotStoreTest, SlotClientDeleteAllVersionsMakesKeyInvisible)
{
    SlotClient client(baseDir_);
    ASSERT_TRUE(client.Init().IsOk());

    auto body1 = std::make_shared<std::stringstream>();
    *body1 << "v1";
    ASSERT_TRUE(client.Save("tenant/keyG", 1, 1000, body1).IsOk());
    auto body2 = std::make_shared<std::stringstream>();
    *body2 << "v2";
    ASSERT_TRUE(client.Save("tenant/keyG", 2, 1000, body2).IsOk());

    ASSERT_TRUE(client.Delete("tenant/keyG", 2, true).IsOk());

    auto content = std::make_shared<std::stringstream>();
    ASSERT_EQ(client.GetWithoutVersion("tenant/keyG", 1000, 0, content).GetCode(), StatusCode::K_NOT_FOUND_IN_L2CACHE);
}

TEST_F(SlotStoreTest, SlotClientCompactSlotWorks)
{
    SlotClient client(baseDir_);
    ASSERT_TRUE(client.Init().IsOk());

    auto body = std::make_shared<std::stringstream>();
    *body << "compact_me";
    ASSERT_TRUE(client.Save("tenant/keyClientCompact", 1, 1000, body).IsOk());

    uint32_t slotId =
        static_cast<uint32_t>(std::hash<std::string>{}("tenant/keyClientCompact") % FLAGS_distributed_disk_slot_num);
    ASSERT_TRUE(client.CompactSlot(slotId).IsOk());

    auto content = std::make_shared<std::stringstream>();
    ASSERT_TRUE(client.Get("tenant/keyClientCompact", 1, 1000, content).IsOk());
    ASSERT_EQ(ReadAll(content), "compact_me");
}

TEST_F(SlotStoreTest, SlotTakeoverPlannerBuildsAndLoadsPlan)
{
    auto sourcePath = MakeTempDir() + "/planner_source";
    auto targetPath = MakeTempDir() + "/planner_target";
    Slot sourceManager(1, sourcePath, 4);
    Slot targetManager(2, targetPath, 1024);
    SavePayload(sourceManager, "tenant/plannerA", 1, "aaaa");
    SavePayload(sourceManager, "tenant/plannerB", 2, "bbbb");
    ASSERT_TRUE(sourceManager.Seal("planner").IsOk());
    ASSERT_TRUE(targetManager.BootstrapManifestIfNeed().IsOk());

    SlotManifestData sourceManifest;
    SlotManifestData targetManifest;
    ASSERT_TRUE(SlotManifest::Load(sourcePath, sourceManifest).IsOk());
    ASSERT_TRUE(SlotManifest::Load(targetPath, targetManifest).IsOk());
    SlotSnapshot sourceSnapshot;
    ASSERT_TRUE(sourceManager.ReplayIndex(sourceSnapshot).IsOk());

    SlotTakeoverPlan plan;
    SlotTakeoverRequest request;
    request.mode = SlotTakeoverMode::MERGE;
    ASSERT_TRUE(SlotTakeoverPlanner::BuildPlan(sourcePath, GetRecoverySlotPath(sourcePath, "txn_planner"),
                                               sourceManifest, sourceSnapshot, targetPath, targetManifest, request,
                                               "txn_planner", plan)
                    .IsOk());
    ASSERT_EQ(plan.txnId, "txn_planner");
    ASSERT_EQ(plan.dataMappings.size(), 2u);
    ASSERT_TRUE(FileExist(JoinPath(targetPath, plan.importIndexFile)));

    ASSERT_TRUE(SlotTakeoverPlanner::DumpPlan(targetPath, plan).IsOk());
    SlotTakeoverPlan loaded;
    ASSERT_TRUE(
        SlotTakeoverPlanner::LoadPlan(JoinPath(targetPath, FormatTakeoverPlanFileName("txn_planner")), loaded).IsOk());
    ASSERT_EQ(loaded.txnId, plan.txnId);
    ASSERT_EQ(loaded.sourceHomeSlotPath, sourcePath);
    ASSERT_EQ(loaded.sourceRecoverySlotPath, GetRecoverySlotPath(sourcePath, "txn_planner"));
    ASSERT_EQ(loaded.targetSlotPath, targetPath);
    ASSERT_EQ(loaded.importIndexFile, plan.importIndexFile);
    ASSERT_EQ(loaded.dataMappings.size(), plan.dataMappings.size());

    SlotTakeoverPlan malformed;
    ASSERT_EQ(SlotTakeoverPlanner::Decode("version=1\nsource_home_slot=/src\n", malformed).GetCode(),
              StatusCode::K_INVALID);
    (void)RemoveAll(sourcePath.substr(0, sourcePath.find("/planner_source")));
    (void)RemoveAll(targetPath.substr(0, targetPath.find("/planner_target")));
}

TEST_F(SlotStoreTest, SlotSealIsIdempotentAndRejectsWrites)
{
    auto slotPath = MakeTempDir() + "/slot_seal";
    Slot manager(10, slotPath, 1024);
    SavePayload(manager, "tenant/keySeal", 1, "sealed");
    ASSERT_TRUE(manager.Seal("unit_test").IsOk());
    ASSERT_TRUE(manager.Seal("unit_test").IsOk());

    SlotManifestData manifest;
    ASSERT_TRUE(SlotManifest::Load(slotPath, manifest).IsOk());
    ExpectManifestNormal(manifest);
    ASSERT_TRUE(manager.Save("tenant/keySeal", 2, MakeBody("new")).IsOk());
    ASSERT_TRUE(manager.Delete("tenant/keySeal", 2, false).IsOk());
    (void)RemoveAll(slotPath.substr(0, slotPath.find("/slot_seal")));
}

TEST_F(SlotStoreTest, MergeAndPreloadMoveFiles)
{
    SlotClient client(baseDir_);
    ASSERT_TRUE(client.Init().IsOk());

    auto mergeSlotId = 2u;
    Slot sourceManager(mergeSlotId, GetSlotPath(baseDir_, mergeSlotId, SOURCE_WORKER_ADDRESS_A), 4);
    Slot targetManager(mergeSlotId, GetSlotPath(baseDir_, mergeSlotId, TARGET_WORKER_ADDRESS), 1024);
    SavePayload(sourceManager, "tenant/mergeA", 1, "aaaa");
    SavePayload(sourceManager, "tenant/mergeB", 2, "bbbb");
    ASSERT_TRUE(sourceManager.Seal("merge").IsOk());
    ASSERT_TRUE(client.MergeSlot(SOURCE_WORKER_ADDRESS_A, mergeSlotId).IsOk());

    auto merged = std::make_shared<std::stringstream>();
    ASSERT_TRUE(targetManager.Get("tenant/mergeA", 1, merged).IsOk());
    ASSERT_EQ(ReadAll(merged), "aaaa");
    auto merged2 = std::make_shared<std::stringstream>();
    ASSERT_TRUE(targetManager.Get("tenant/mergeB", 2, merged2).IsOk());
    ASSERT_EQ(ReadAll(merged2), "bbbb");

    SlotManifestData sourceAfterMerge;
    ASSERT_FALSE(FileExist(GetSlotPath(baseDir_, mergeSlotId, SOURCE_WORKER_ADDRESS_A)));

    auto preloadSlotId = 0u;
    Slot sourceManager2(preloadSlotId, GetSlotPath(baseDir_, preloadSlotId, SOURCE_WORKER_ADDRESS_B), 1024);
    Slot targetManager2(preloadSlotId, GetSlotPath(baseDir_, preloadSlotId, TARGET_WORKER_ADDRESS), 1024);
    SavePayload(sourceManager2, "tenant/preloadA", 3, "preload");
    ASSERT_TRUE(sourceManager2.Seal("preload").IsOk());
    ASSERT_TRUE(client.PreloadSlot(SOURCE_WORKER_ADDRESS_B, preloadSlotId).IsOk());
    auto preloaded = std::make_shared<std::stringstream>();
    ASSERT_TRUE(targetManager2.Get("tenant/preloadA", 3, preloaded).IsOk());
    ASSERT_EQ(ReadAll(preloaded), "preload");
}

TEST_F(SlotStoreTest, TakeoverPlanPrecedesImportManifest)
{
    auto sourcePath = MakeTempDir() + "/takeover_plan_source";
    auto targetPath = MakeTempDir() + "/takeover_plan_target";
    Slot sourceManager(1, sourcePath, 1024);
    Slot targetManager(2, targetPath, 1024);
    SavePayload(sourceManager, "tenant/plan", 1, "payload");
    ASSERT_TRUE(sourceManager.Seal("plan").IsOk());
    (void)BuildTakeoverPlan(sourcePath, targetPath, sourceManager, targetManager, "txn_plan_only");

    std::vector<std::string> planFiles;
    ASSERT_TRUE(Glob(JoinPath(targetPath, "takeover_*.plan"), planFiles).IsOk());
    ASSERT_EQ(planFiles.size(), 1u);
    SlotManifestData targetManifest;
    ASSERT_TRUE(SlotManifest::Load(targetPath, targetManifest).IsOk());
    ExpectManifestNormal(targetManifest);

    ASSERT_TRUE(targetManager.Takeover(sourcePath, false).IsOk());
    auto content = std::make_shared<std::stringstream>();
    ASSERT_TRUE(targetManager.Get("tenant/plan", 1, content).IsOk());
    ASSERT_EQ(ReadAll(content), "payload");
    planFiles.clear();
    ASSERT_TRUE(Glob(JoinPath(targetPath, "takeover_*.plan"), planFiles).IsOk());
    ASSERT_TRUE(planFiles.empty());
    (void)RemoveAll(sourcePath.substr(0, sourcePath.find("/takeover_plan_source")));
    (void)RemoveAll(targetPath.substr(0, targetPath.find("/takeover_plan_target")));
}

TEST_F(SlotStoreTest, ImportRecoveryAfterManifestOnly)
{
    auto sourcePath = MakeTempDir() + "/importing_manifest_source";
    auto targetPath = MakeTempDir() + "/importing_manifest_target";
    Slot sourceManager(1, sourcePath, 1024);
    Slot targetManager(2, targetPath, 1024);
    SavePayload(sourceManager, "tenant/importing0", 1, "recover");
    ASSERT_TRUE(sourceManager.Seal("recover").IsOk());
    auto plan = BuildTakeoverPlan(sourcePath, targetPath, sourceManager, targetManager, "txn_importing_manifest");
    PrepareRecoveryDir(plan);
    PersistImportingManifest(targetPath, plan);

    SlotManifestData importingManifest;
    ASSERT_TRUE(SlotManifest::Load(targetPath, importingManifest).IsOk());
    ASSERT_EQ(importingManifest.state, SlotState::IN_OPERATION);
    ASSERT_EQ(importingManifest.opType, SlotOperationType::TRANSFER);
    ASSERT_EQ(importingManifest.opPhase, SlotOperationPhase::TRANSFER_PREPARED);

    ASSERT_TRUE(targetManager.Repair().IsOk());
    auto content = std::make_shared<std::stringstream>();
    ASSERT_TRUE(targetManager.Get("tenant/importing0", 1, content).IsOk());
    ASSERT_EQ(ReadAll(content), "recover");
    (void)RemoveAll(sourcePath.substr(0, sourcePath.find("/importing_manifest_source")));
    (void)RemoveAll(targetPath.substr(0, targetPath.find("/importing_manifest_target")));
}

TEST_F(SlotStoreTest, ImportRecoveryAfterPartialMove)
{
    auto sourcePath = MakeTempDir() + "/importing_move_source";
    auto targetPath = MakeTempDir() + "/importing_move_target";
    Slot sourceManager(1, sourcePath, 4);
    Slot targetManager(2, targetPath, 1024);
    SavePayload(sourceManager, "tenant/moveA", 1, "aaaa");
    SavePayload(sourceManager, "tenant/moveB", 2, "bbbb");
    ASSERT_TRUE(sourceManager.Seal("recover").IsOk());
    auto plan = BuildTakeoverPlan(sourcePath, targetPath, sourceManager, targetManager, "txn_partial_move");
    PrepareRecoveryDir(plan);
    PersistImportingManifest(targetPath, plan);
    MoveTakeoverDataFiles(plan, 1);

    ASSERT_TRUE(FileExist(JoinPath(targetPath, plan.dataMappings[0].targetDataFile)));
    ASSERT_TRUE(FileExist(JoinPath(plan.sourceRecoverySlotPath, plan.dataMappings[1].sourceDataFile)));

    ASSERT_TRUE(targetManager.Repair().IsOk());
    auto contentA = std::make_shared<std::stringstream>();
    ASSERT_TRUE(targetManager.Get("tenant/moveA", 1, contentA).IsOk());
    ASSERT_EQ(ReadAll(contentA), "aaaa");
    auto contentB = std::make_shared<std::stringstream>();
    ASSERT_TRUE(targetManager.Get("tenant/moveB", 2, contentB).IsOk());
    ASSERT_EQ(ReadAll(contentB), "bbbb");
    (void)RemoveAll(sourcePath.substr(0, sourcePath.find("/importing_move_source")));
    (void)RemoveAll(targetPath.substr(0, targetPath.find("/importing_move_target")));
}

TEST_F(SlotStoreTest, ImportRecoveryClosesOpenBatch)
{
    auto sourcePath = MakeTempDir() + "/import_begin_source";
    auto targetPath = MakeTempDir() + "/import_begin_target";
    Slot sourceManager(1, sourcePath, 1024);
    Slot targetManager(2, targetPath, 1024);
    SavePayload(sourceManager, "tenant/importBegin", 1, "payload");
    ASSERT_TRUE(sourceManager.Seal("recover").IsOk());
    auto plan = BuildTakeoverPlan(sourcePath, targetPath, sourceManager, targetManager, "txn_after_import_begin");
    PrepareRecoveryDir(plan);
    PersistImportingManifest(targetPath, plan);
    MoveTakeoverDataFiles(plan, plan.dataMappings.size());
    SlotManifestData targetManifest;
    ASSERT_TRUE(SlotManifest::Load(targetPath, targetManifest).IsOk());
    AppendImportBatch(targetPath, targetManifest.activeIndex, plan, false, false);

    SlotManifestData manifestBeforeRecovery;
    ASSERT_TRUE(SlotManifest::Load(targetPath, manifestBeforeRecovery).IsOk());
    SlotSnapshot snapshotBeforeRecovery;
    ASSERT_TRUE(SlotSnapshot::Replay(targetPath, manifestBeforeRecovery, snapshotBeforeRecovery).IsOk());
    SlotSnapshotValue invisibleValue;
    ASSERT_EQ(snapshotBeforeRecovery.FindExact("tenant/importBegin", 1, invisibleValue).GetCode(),
              StatusCode::K_NOT_FOUND_IN_L2CACHE);

    ASSERT_TRUE(targetManager.Repair().IsOk());
    auto content = std::make_shared<std::stringstream>();
    ASSERT_TRUE(targetManager.Get("tenant/importBegin", 1, content).IsOk());
    ASSERT_EQ(ReadAll(content), "payload");

    SlotManifestData manifest;
    ASSERT_TRUE(SlotManifest::Load(targetPath, manifest).IsOk());
    std::vector<SlotRecord> records;
    size_t validBytes = 0;
    ASSERT_TRUE(SlotIndexCodec::ReadAllRecords(JoinPath(targetPath, manifest.activeIndex), records, validBytes).IsOk());
    size_t importEndCount = 0;
    for (const auto &record : records) {
        if (record.type == SlotRecordType::IMPORT_END) {
            ++importEndCount;
        }
    }
    ASSERT_EQ(importEndCount, 1u);
    ASSERT_TRUE(targetManager.Repair().IsOk());
    (void)RemoveAll(sourcePath.substr(0, sourcePath.find("/import_begin_source")));
    (void)RemoveAll(targetPath.substr(0, targetPath.find("/import_begin_target")));
}

TEST_F(SlotStoreTest, ImportRecoveryBeforeEndMarker)
{
    auto sourcePath = MakeTempDir() + "/before_import_end_source";
    auto targetPath = MakeTempDir() + "/before_import_end_target";
    Slot sourceManager(1, sourcePath, 1024);
    Slot targetManager(2, targetPath, 1024);
    SavePayload(sourceManager, "tenant/beforeImportEnd", 1, "payload2");
    ASSERT_TRUE(sourceManager.Seal("recover").IsOk());
    auto plan = BuildTakeoverPlan(sourcePath, targetPath, sourceManager, targetManager, "txn_before_import_end");
    PrepareRecoveryDir(plan);
    PersistImportingManifest(targetPath, plan);
    MoveTakeoverDataFiles(plan, plan.dataMappings.size());
    SlotManifestData targetManifest;
    ASSERT_TRUE(SlotManifest::Load(targetPath, targetManifest).IsOk());
    AppendImportBatch(targetPath, targetManifest.activeIndex, plan, true, false);

    SlotManifestData manifestBeforeRecovery;
    ASSERT_TRUE(SlotManifest::Load(targetPath, manifestBeforeRecovery).IsOk());
    SlotSnapshot snapshotBeforeRecovery;
    ASSERT_TRUE(SlotSnapshot::Replay(targetPath, manifestBeforeRecovery, snapshotBeforeRecovery).IsOk());
    SlotSnapshotValue invisibleValue;
    ASSERT_EQ(snapshotBeforeRecovery.FindExact("tenant/beforeImportEnd", 1, invisibleValue).GetCode(),
              StatusCode::K_NOT_FOUND_IN_L2CACHE);

    ASSERT_TRUE(targetManager.Repair().IsOk());
    auto content = std::make_shared<std::stringstream>();
    ASSERT_TRUE(targetManager.Get("tenant/beforeImportEnd", 1, content).IsOk());
    ASSERT_EQ(ReadAll(content), "payload2");
    (void)RemoveAll(sourcePath.substr(0, sourcePath.find("/before_import_end_source")));
    (void)RemoveAll(targetPath.substr(0, targetPath.find("/before_import_end_target")));
}

TEST_F(SlotStoreTest, ImportRecoveryBeforeSourceCleanup)
{
    auto sourcePath = MakeTempDir() + "/before_source_finalize_source";
    auto targetPath = MakeTempDir() + "/before_source_finalize_target";
    Slot sourceManager(1, sourcePath, 1024);
    Slot targetManager(2, targetPath, 1024);
    SavePayload(sourceManager, "tenant/sourceFinalize", 1, "payload3");
    ASSERT_TRUE(sourceManager.Seal("recover").IsOk());
    auto plan = BuildTakeoverPlan(sourcePath, targetPath, sourceManager, targetManager, "txn_before_source_finalize");
    PrepareRecoveryDir(plan);
    PersistImportingManifest(targetPath, plan);
    MoveTakeoverDataFiles(plan, plan.dataMappings.size());
    SlotManifestData targetManifest;
    ASSERT_TRUE(SlotManifest::Load(targetPath, targetManifest).IsOk());
    AppendImportBatch(targetPath, targetManifest.activeIndex, plan, true, true);

    SlotManifestData sourceManifest;
    ASSERT_TRUE(SlotManifest::Load(plan.sourceRecoverySlotPath, sourceManifest).IsOk());
    ASSERT_FALSE(sourceManifest.activeData.empty());

    ASSERT_TRUE(targetManager.Repair().IsOk());
    ASSERT_FALSE(FileExist(sourcePath));

    auto content = std::make_shared<std::stringstream>();
    ASSERT_TRUE(targetManager.Get("tenant/sourceFinalize", 1, content).IsOk());
    ASSERT_EQ(ReadAll(content), "payload3");
    (void)RemoveAll(sourcePath.substr(0, sourcePath.find("/before_source_finalize_source")));
    (void)RemoveAll(targetPath.substr(0, targetPath.find("/before_source_finalize_target")));
}

TEST_F(SlotStoreTest, TransferFencingResetsMissingSource)
{
    auto targetPath = MakeTempDir() + "/fencing_reset_target";
    auto missingSourceRoot = MakeTempDir();
    Slot targetManager(2, targetPath, 1024);
    ASSERT_TRUE(targetManager.BootstrapManifestIfNeed().IsOk());

    SlotManifestData manifest;
    ASSERT_TRUE(SlotManifest::Load(targetPath, manifest).IsOk());
    manifest.state = SlotState::IN_OPERATION;
    manifest.opType = SlotOperationType::TRANSFER;
    manifest.opPhase = SlotOperationPhase::TRANSFER_FENCING;
    manifest.role = SlotOperationRole::TARGET;
    manifest.txnId = "txn_fencing_reset";
    manifest.ownerEpoch = 9;
    manifest.peerSlotPath = missingSourceRoot + "/missing_source_slot";
    manifest.recoverySlotPath = GetRecoverySlotPath(manifest.peerSlotPath, manifest.txnId);
    ASSERT_TRUE(SlotManifest::StoreAtomic(targetPath, manifest).IsOk());

    ASSERT_TRUE(targetManager.Repair().IsOk());
    SlotManifestData repaired;
    ASSERT_TRUE(SlotManifest::Load(targetPath, repaired).IsOk());
    ExpectManifestNormal(repaired);
    ASSERT_EQ(repaired.ownerEpoch, 9u);

    (void)RemoveAll(missingSourceRoot);
    (void)RemoveAll(targetPath.substr(0, targetPath.find("/fencing_reset_target")));
}

TEST_F(SlotStoreTest, ImportRecoveryWithoutRecoveryDir)
{
    auto sourcePath = MakeTempDir() + "/prepared_recovery_gone_source";
    auto targetPath = MakeTempDir() + "/prepared_recovery_gone_target";
    Slot sourceManager(1, sourcePath, 4);
    Slot targetManager(2, targetPath, 1024);
    SavePayload(sourceManager, "tenant/recoveryGoneA", 1, "aaaa");
    SavePayload(sourceManager, "tenant/recoveryGoneB", 2, "bbbb");
    ASSERT_TRUE(sourceManager.Seal("recover").IsOk());
    auto plan = BuildTakeoverPlan(sourcePath, targetPath, sourceManager, targetManager, "txn_recovery_gone");
    PrepareRecoveryDir(plan);
    PersistImportingManifest(targetPath, plan);
    MoveTakeoverDataFiles(plan, plan.dataMappings.size());
    ASSERT_TRUE(RemoveAll(plan.sourceRecoverySlotPath).IsOk());

    ASSERT_TRUE(targetManager.Repair().IsOk());
    SlotManifestData repaired;
    ASSERT_TRUE(SlotManifest::Load(targetPath, repaired).IsOk());
    ExpectManifestNormal(repaired);

    auto contentA = std::make_shared<std::stringstream>();
    ASSERT_TRUE(targetManager.Get("tenant/recoveryGoneA", 1, contentA).IsOk());
    ASSERT_EQ(ReadAll(contentA), "aaaa");
    auto contentB = std::make_shared<std::stringstream>();
    ASSERT_TRUE(targetManager.Get("tenant/recoveryGoneB", 2, contentB).IsOk());
    ASSERT_EQ(ReadAll(contentB), "bbbb");

    (void)RemoveAll(sourcePath.substr(0, sourcePath.find("/prepared_recovery_gone_source")));
    (void)RemoveAll(targetPath.substr(0, targetPath.find("/prepared_recovery_gone_target")));
}

TEST_F(SlotStoreTest, ImportRecoveryRebuildsPlan)
{
    auto sourcePath = MakeTempDir() + "/prepared_rebuild_source";
    auto targetPath = MakeTempDir() + "/prepared_rebuild_target";
    Slot sourceManager(1, sourcePath, 1024);
    Slot targetManager(2, targetPath, 1024);
    SavePayload(sourceManager, "tenant/rebuildPlan", 1, "payload");
    ASSERT_TRUE(sourceManager.Seal("recover").IsOk());
    auto plan = BuildTakeoverPlan(sourcePath, targetPath, sourceManager, targetManager, "txn_rebuild_plan");
    PrepareRecoveryDir(plan);
    PersistImportingManifest(targetPath, plan);
    ASSERT_TRUE(DeleteFile(JoinPath(targetPath, plan.importIndexFile)).IsOk());
    ASSERT_TRUE(DeleteFile(JoinPath(targetPath, FormatTakeoverPlanFileName(plan.txnId))).IsOk());

    ASSERT_TRUE(targetManager.Repair().IsOk());
    SlotManifestData repaired;
    ASSERT_TRUE(SlotManifest::Load(targetPath, repaired).IsOk());
    ExpectManifestNormal(repaired);

    auto content = std::make_shared<std::stringstream>();
    ASSERT_TRUE(targetManager.Get("tenant/rebuildPlan", 1, content).IsOk());
    ASSERT_EQ(ReadAll(content), "payload");

    (void)RemoveAll(sourcePath.substr(0, sourcePath.find("/prepared_rebuild_source")));
    (void)RemoveAll(targetPath.substr(0, targetPath.find("/prepared_rebuild_target")));
}

TEST_F(SlotStoreTest, ImportRecoveryResetsWithoutEvidence)
{
    auto sourcePath = MakeTempDir() + "/prepared_abort_source";
    auto targetPath = MakeTempDir() + "/prepared_abort_target";
    Slot sourceManager(1, sourcePath, 1024);
    Slot targetManager(2, targetPath, 1024);
    SavePayload(sourceManager, "tenant/abortPrepared", 1, "payload_abort");
    ASSERT_TRUE(sourceManager.Seal("recover").IsOk());
    auto plan = BuildTakeoverPlan(sourcePath, targetPath, sourceManager, targetManager, "txn_abort_prepared");
    PrepareRecoveryDir(plan);
    PersistImportingManifest(targetPath, plan);
    MoveTakeoverDataFiles(plan, 1);

    ASSERT_TRUE(RemoveAll(plan.sourceRecoverySlotPath).IsOk());
    ASSERT_TRUE(DeleteFile(JoinPath(targetPath, plan.importIndexFile)).IsOk());
    ASSERT_TRUE(DeleteFile(JoinPath(targetPath, FormatTakeoverPlanFileName(plan.txnId))).IsOk());
    ASSERT_TRUE(FileExist(JoinPath(targetPath, plan.dataMappings[0].targetDataFile)));

    ASSERT_TRUE(targetManager.Repair().IsOk());
    SlotManifestData repaired;
    ASSERT_TRUE(SlotManifest::Load(targetPath, repaired).IsOk());
    ExpectManifestNormal(repaired);
    ASSERT_FALSE(FileExist(JoinPath(targetPath, plan.dataMappings[0].targetDataFile)));

    auto content = std::make_shared<std::stringstream>();
    ASSERT_EQ(targetManager.Get("tenant/abortPrepared", 1, content).GetCode(), StatusCode::K_NOT_FOUND_IN_L2CACHE);

    (void)RemoveAll(sourcePath.substr(0, sourcePath.find("/prepared_abort_source")));
    (void)RemoveAll(targetPath.substr(0, targetPath.find("/prepared_abort_target")));
}
}  // namespace ut
}  // namespace datasystem
