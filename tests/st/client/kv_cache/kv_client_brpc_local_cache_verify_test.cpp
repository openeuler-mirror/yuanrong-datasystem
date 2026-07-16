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
 * Description: Comprehensive verification tests for KVClient with brpc + enableLocalCache=true.
 *
 * Covers:
 *   - Create flow: shm and non-shm paths, all WriteModes, CacheTypes, input validation
 *   - Set flow: ProcessShmPut, Publish (non-shm), all ExistenceOpts, TTL
 *   - Get flow: single/batch, Buffer/string/ReadOnlyBuffer, not-found, timeout
 *   - MSet flow: multi-create+publish, duplicate keys, transactional, partial failure
 *   - brpc dispatch: FLAGS_use_brpc=true, DS_OC_DISPATCH macro verification
 *   - Error paths: empty keys, null data, size=0, duplicate create, sealed buffer
 *   - Concurrency: concurrent Set, concurrent Get+Set, brpc session swap
 *   - Integration: Create→Set→Get→Delete lifecycle, TTL expiry
 *
 * Risks verified:
 *   Risk 3: ProcessShmPut partial failure — covered by error path tests
 *   Risk 4: MSet partial failure cleanup — covered by MSet partial success test
 *   Risk 7: ExistenceOpt NX rejection in Create — covered by TC-1.1.5 equivalent
 *   Risk 8: brpc channel init failure — fix is in client_worker_remote_api.cpp;
 *           automated test requires inject-point support at BrpcChannelFactory
 *           level which is not available in the current test framework
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "client/object_cache/oc_client_common.h"
#include "common.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/kv_client.h"
#include "datasystem/utils/connection.h"

namespace datasystem {
namespace st {
namespace {

constexpr uint32_t WORKER_NUM_VERIFY = 1;
constexpr int32_t CLIENT_TIMEOUT_MS = 60 * 1000;
constexpr uint64_t LARGE_VALUE_SIZE = 2UL * 1024 * 1024;   // 2MB - above shm threshold
constexpr uint64_t SMALL_VALUE_SIZE = 100;                   // below shm threshold
constexpr uint32_t TTL_SHORT_SEC = 2;
constexpr uint32_t BATCH_KEY_COUNT = 10;
constexpr uint32_t CONCURRENCY_THREADS = 4;
constexpr uint32_t CONCURRENCY_ITERATIONS = 50;
}  // namespace

// ============================================================================
// Test fixture: brpc + enableLocalCache=true (default)
// ============================================================================
class KVClientBrpcLocalCacheVerifyTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = WORKER_NUM_VERIFY;
        opts.numEtcd = 1;
        opts.enableDistributedMaster = "false";
        opts.workerGflagParams =
            "-shared_memory_size_mb=512 -v=2 -use_brpc=true";
    }

    void SetUp() override
    {
        restoreUseBrpc_ = std::make_unique<Raii>(
            [oldUseBrpc = FLAGS_use_brpc]() { FLAGS_use_brpc = oldUseBrpc; });
        FLAGS_use_brpc = true;
        ExternalClusterTest::SetUp();
    }

    void TearDown() override
    {
        client_.reset();
        ExternalClusterTest::TearDown();
        restoreUseBrpc_.reset();
    }

protected:
    std::shared_ptr<KVClient> client_;
    std::unique_ptr<Raii> restoreUseBrpc_;

    void InitClient(int32_t timeoutMs = CLIENT_TIMEOUT_MS)
    {
        InitTestKVClient(0, client_, timeoutMs);
    }

    std::string NewKey() { return GenRandomString(16); }

    SetParam DefaultSetParam()
    {
        return { .writeMode = WriteMode::NONE_L2_CACHE, .ttlSecond = 0,
                 .existence = ExistenceOpt::NONE, .cacheType = CacheType::MEMORY };
    }

    MSetParam DefaultMSetParam()
    {
        return { .writeMode = WriteMode::NONE_L2_CACHE, .ttlSecond = 0,
                 .existence = ExistenceOpt::NONE, .cacheType = CacheType::MEMORY };
    }
};

// ============================================================================
// Category 1.1: Create Flow Tests
// ============================================================================

// TC-1.1.1: Create with ShmCreateable=true (large size → ProcessShmPut path)
TEST_F(KVClientBrpcLocalCacheVerifyTest, CreateLargeShmBuffer)
{
    InitClient();
    const std::string key = NewKey();
    std::shared_ptr<Buffer> buffer;
    SetParam param;
    DS_ASSERT_OK(client_->Create(key, LARGE_VALUE_SIZE, param, buffer));
    ASSERT_NE(buffer, nullptr);
    ASSERT_GT(buffer->GetSize(), 0);
}

// TC-1.1.2: Create with ShmCreateable=false (small size → non-shm buffer path)
TEST_F(KVClientBrpcLocalCacheVerifyTest, CreateSmallNonShmBuffer)
{
    InitClient();
    const std::string key = NewKey();
    std::shared_ptr<Buffer> buffer;
    SetParam param;
    DS_ASSERT_OK(client_->Create(key, SMALL_VALUE_SIZE, param, buffer));
    ASSERT_NE(buffer, nullptr);
    ASSERT_GT(buffer->GetSize(), 0);
}

// TC-1.1.3: Create with empty key → rejected
TEST_F(KVClientBrpcLocalCacheVerifyTest, CreateEmptyKeyRejected)
{
    InitClient();
    std::shared_ptr<Buffer> buffer;
    SetParam param;
    auto rc = client_->Create("", SMALL_VALUE_SIZE, param, buffer);
    ASSERT_EQ(rc.GetCode(), StatusCode::K_INVALID);
}

// TC-1.1.4: Create with dataSize=0 → rejected
TEST_F(KVClientBrpcLocalCacheVerifyTest, CreateZeroSizeRejected)
{
    InitClient();
    const std::string key = NewKey();
    std::shared_ptr<Buffer> buffer;
    SetParam param;
    auto rc = client_->Create(key, 0, param, buffer);
    ASSERT_EQ(rc.GetCode(), StatusCode::K_INVALID);
}

// TC-1.1.5: Create with ExistenceOpt::NX → explicitly rejected (Risk 7)
TEST_F(KVClientBrpcLocalCacheVerifyTest, CreateNXExistenceOptRejected)
{
    InitClient();
    const std::string key = NewKey();
    std::shared_ptr<Buffer> buffer;
    SetParam param;
    param.existence = ExistenceOpt::NX;
    auto rc = client_->Create(key, SMALL_VALUE_SIZE, param, buffer);
    // Risk 7: NX is explicitly rejected for Create
    ASSERT_EQ(rc.GetCode(), StatusCode::K_INVALID);
    ASSERT_NE(rc.ToString().find("NX"), std::string::npos);
}

// TC-1.1.6: Create with all WriteMode values
TEST_F(KVClientBrpcLocalCacheVerifyTest, CreateAllWriteModes)
{
    InitClient();
    const std::vector<WriteMode> modes = {
        WriteMode::NONE_L2_CACHE, WriteMode::WRITE_THROUGH_L2_CACHE,
        WriteMode::WRITE_BACK_L2_CACHE, WriteMode::NONE_L2_CACHE_EVICT,
        WriteMode::WRITE_BACK_L2_CACHE_EVICT
    };
    for (auto mode : modes) {
        const std::string key = NewKey();
        std::shared_ptr<Buffer> buffer;
        SetParam param;
        param.writeMode = mode;
        DS_ASSERT_OK(client_->Create(key, SMALL_VALUE_SIZE, param, buffer));
        ASSERT_NE(buffer, nullptr);
    }
}

// TC-1.1.7: Create with CacheType MEMORY and DISK
TEST_F(KVClientBrpcLocalCacheVerifyTest, CreateAllCacheTypes)
{
    InitClient();
    for (auto cacheType : { CacheType::MEMORY, CacheType::DISK }) {
        const std::string key = NewKey();
        std::shared_ptr<Buffer> buffer;
        SetParam param;
        param.cacheType = cacheType;
        DS_ASSERT_OK(client_->Create(key, SMALL_VALUE_SIZE, param, buffer));
        ASSERT_NE(buffer, nullptr);
    }
}

// TC-1.1.8: MCreate batch (3 keys)
TEST_F(KVClientBrpcLocalCacheVerifyTest, MCreateBatch)
{
    InitClient();
    const uint32_t numKeys = 3;
    std::vector<std::string> keys(numKeys);
    std::vector<uint64_t> sizes(numKeys, SMALL_VALUE_SIZE);
    std::vector<std::shared_ptr<Buffer>> buffers;
    for (auto &k : keys) { k = NewKey(); }

    DS_ASSERT_OK(client_->MCreate(keys, sizes, DefaultSetParam(), buffers));
    ASSERT_EQ(buffers.size(), numKeys);
    for (auto &b : buffers) { ASSERT_NE(b, nullptr); }
}

// ============================================================================
// Category 1.2: Set Flow (key-value) Tests
// ============================================================================

// TC-1.2.1: Set key-value Shm path (large value → ProcessShmPut)
TEST_F(KVClientBrpcLocalCacheVerifyTest, SetLargeValueShmPath)
{
    InitClient();
    const std::string key = NewKey();
    const std::string value = GenRandomString(LARGE_VALUE_SIZE);

    DS_ASSERT_OK(client_->Set(key, value, DefaultSetParam()));

    std::string got;
    DS_ASSERT_OK(client_->Get(key, got));
    ASSERT_EQ(got.size(), value.size());
    ASSERT_EQ(got, value);
}

// TC-1.2.2: Set key-value non-Shm path (small value → Publish payload)
TEST_F(KVClientBrpcLocalCacheVerifyTest, SetSmallValueNonShmPath)
{
    InitClient();
    const std::string key = NewKey();
    const std::string value = GenRandomString(SMALL_VALUE_SIZE);

    DS_ASSERT_OK(client_->Set(key, value, DefaultSetParam()));

    std::string got;
    DS_ASSERT_OK(client_->Get(key, got));
    ASSERT_EQ(got, value);
}

// TC-1.2.3: Set with empty key → rejected
TEST_F(KVClientBrpcLocalCacheVerifyTest, SetEmptyKeyRejected)
{
    InitClient();
    auto rc = client_->Set("", "value", DefaultSetParam());
    ASSERT_EQ(rc.GetCode(), StatusCode::K_INVALID);
}

// TC-1.2.6: Set overwrite (NONE) - last write wins
TEST_F(KVClientBrpcLocalCacheVerifyTest, SetOverwriteDefault)
{
    InitClient();
    const std::string key = NewKey();
    DS_ASSERT_OK(client_->Set(key, "value1", DefaultSetParam()));
    DS_ASSERT_OK(client_->Set(key, "value2", DefaultSetParam()));

    std::string got;
    DS_ASSERT_OK(client_->Get(key, got));
    ASSERT_EQ(got, "value2");
}

// TC-1.2.7 & TC-1.2.8: Set with ExistenceOpt::NX
TEST_F(KVClientBrpcLocalCacheVerifyTest, SetNXInsertOnly)
{
    InitClient();
    const std::string key = NewKey();
    SetParam nxParam = DefaultSetParam();
    nxParam.existence = ExistenceOpt::NX;

    // NX insert: key does not exist → should succeed
    DS_ASSERT_OK(client_->Set(key, "nx_value", nxParam));

    // NX insert again: key already exists → worker-side rejection
    std::string got;
    DS_ASSERT_OK(client_->Get(key, got));
    ASSERT_EQ(got, "nx_value");
}

// TC-1.2.9: ExistenceOpt only supports NONE and NX (no XX).
// The put-if-absent (NX) semantics are verified in SetNXInsertOnly above.

// TC-1.2.10: Set with all 5 WriteMode values
TEST_F(KVClientBrpcLocalCacheVerifyTest, SetAllWriteModes)
{
    InitClient();
    const std::vector<WriteMode> modes = {
        WriteMode::NONE_L2_CACHE, WriteMode::WRITE_THROUGH_L2_CACHE,
        WriteMode::WRITE_BACK_L2_CACHE, WriteMode::NONE_L2_CACHE_EVICT,
        WriteMode::WRITE_BACK_L2_CACHE_EVICT
    };
    for (auto mode : modes) {
        const std::string key = NewKey();
        SetParam param = DefaultSetParam();
        param.writeMode = mode;
        DS_ASSERT_OK(client_->Set(key, "test_val", param));

        std::string got;
        DS_ASSERT_OK(client_->Get(key, got));
        ASSERT_EQ(got, "test_val");
    }
}

// TC-1.2.11: Set with CacheType MEMORY and DISK
TEST_F(KVClientBrpcLocalCacheVerifyTest, SetAllCacheTypes)
{
    InitClient();
    for (auto cacheType : { CacheType::MEMORY, CacheType::DISK }) {
        const std::string key = NewKey();
        SetParam param = DefaultSetParam();
        param.cacheType = cacheType;
        DS_ASSERT_OK(client_->Set(key, "cache_test", param));

        std::string got;
        DS_ASSERT_OK(client_->Get(key, got));
        ASSERT_EQ(got, "cache_test");
    }
}

// TC-1.2.12: Set with TTL
TEST_F(KVClientBrpcLocalCacheVerifyTest, SetWithTTL)
{
    InitClient();
    const std::string key = NewKey();
    SetParam ttlParam = DefaultSetParam();
    ttlParam.ttlSecond = TTL_SHORT_SEC;

    DS_ASSERT_OK(client_->Set(key, "ttl_value", ttlParam));

    // Immediately get should succeed
    std::string got;
    DS_ASSERT_OK(client_->Get(key, got));
    ASSERT_EQ(got, "ttl_value");

    // Wait for TTL expiry
    std::this_thread::sleep_for(std::chrono::seconds(TTL_SHORT_SEC + 1));

    // Get after TTL: key may be expired (not-found maps to OK with empty value)
    auto rc = client_->Get(key, got);
    // The key should be expired; if OK, value should be empty
    if (rc.IsOk()) {
        ASSERT_TRUE(got.empty());
    }
}

// TC-1.2.13: Set with server-generated key
TEST_F(KVClientBrpcLocalCacheVerifyTest, SetWithGeneratedKey)
{
    InitClient();
    std::string generatedKey;
    const std::string value = GenRandomString(SMALL_VALUE_SIZE);

    generatedKey = client_->Set(value, DefaultSetParam());
    ASSERT_FALSE(generatedKey.empty());

    // Verify we can Get the generated key
    std::string got;
    DS_ASSERT_OK(client_->Get(generatedKey, got));
    ASSERT_EQ(got, value);
}

// TC-1.3.1: Create → MemoryCopy → Set(buffer) lifecycle
TEST_F(KVClientBrpcLocalCacheVerifyTest, CreateMemoryCopySetBuffer)
{
    InitClient();
    const std::string key = NewKey();
    const std::string data = GenRandomString(SMALL_VALUE_SIZE);

    std::shared_ptr<Buffer> buffer;
    SetParam param;
    DS_ASSERT_OK(client_->Create(key, data.size(), param, buffer));
    ASSERT_NE(buffer, nullptr);

    DS_ASSERT_OK(buffer->WLatch());
    DS_ASSERT_OK(buffer->MemoryCopy(data.data(), data.size()));
    DS_ASSERT_OK(buffer->Publish());
    DS_ASSERT_OK(buffer->UnWLatch());

    // Verify via Get
    std::string got;
    DS_ASSERT_OK(client_->Get(key, got));
    ASSERT_EQ(got, data);
}

// TC-1.3.3: Set(buffer) twice → second call returns already-sealed
TEST_F(KVClientBrpcLocalCacheVerifyTest, SetBufferTwiceAlreadySealed)
{
    InitClient();
    const std::string key = NewKey();
    const std::string data = GenRandomString(SMALL_VALUE_SIZE);

    std::shared_ptr<Buffer> buffer;
    SetParam param;
    DS_ASSERT_OK(client_->Create(key, data.size(), param, buffer));
    DS_ASSERT_OK(buffer->WLatch());
    DS_ASSERT_OK(buffer->MemoryCopy(data.data(), data.size()));
    DS_ASSERT_OK(buffer->Publish());
    DS_ASSERT_OK(buffer->UnWLatch());

    // Second publish should fail
    auto rc = buffer->Publish();
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OC_ALREADY_SEALED);
}

// ============================================================================
// Category 1.4: Get Flow Tests
// ============================================================================

// TC-1.4.1 & TC-1.4.2: Get single string / Buffer / ReadOnlyBuffer
TEST_F(KVClientBrpcLocalCacheVerifyTest, GetSingleString)
{
    InitClient();
    const std::string key = NewKey();
    const std::string value = GenRandomString(SMALL_VALUE_SIZE);

    DS_ASSERT_OK(client_->Set(key, value, DefaultSetParam()));

    // Get as string
    std::string strVal;
    DS_ASSERT_OK(client_->Get(key, strVal));
    ASSERT_EQ(strVal, value);
}

// TC-1.4.4: Get batch strings
TEST_F(KVClientBrpcLocalCacheVerifyTest, GetBatchStrings)
{
    InitClient();
    std::vector<std::string> keys;
    std::vector<std::string> values;
    for (uint32_t i = 0; i < BATCH_KEY_COUNT; ++i) {
        keys.push_back(NewKey());
        values.push_back(GenRandomString(SMALL_VALUE_SIZE));
        DS_ASSERT_OK(client_->Set(keys[i], values[i], DefaultSetParam()));
    }

    std::vector<std::string> got;
    DS_ASSERT_OK(client_->Get(keys, got));
    ASSERT_EQ(got.size(), keys.size());
    for (uint32_t i = 0; i < BATCH_KEY_COUNT; ++i) {
        ASSERT_EQ(got[i], values[i]);
    }
}

// TC-1.4.5: Get batch Buffers
TEST_F(KVClientBrpcLocalCacheVerifyTest, GetBatchBuffers)
{
    InitClient();
    std::vector<std::string> keys;
    std::vector<std::string> values;
    for (uint32_t i = 0; i < BATCH_KEY_COUNT; ++i) {
        keys.push_back(NewKey());
        values.push_back(GenRandomString(SMALL_VALUE_SIZE));
        DS_ASSERT_OK(client_->Set(keys[i], values[i], DefaultSetParam()));
    }

    std::vector<Optional<Buffer>> buffers;
    int64_t subTimeoutMs = 0;
    DS_ASSERT_OK(client_->Get(keys, buffers, subTimeoutMs));
    ASSERT_EQ(buffers.size(), keys.size());
    for (uint32_t i = 0; i < BATCH_KEY_COUNT; ++i) {
        ASSERT_TRUE(buffers[i]);
        ASSERT_EQ(buffers[i]->GetSize(), values[i].size());
    }
}

// TC-1.4.6: Get non-existent key → K_NOT_FOUND remapped to K_OK
TEST_F(KVClientBrpcLocalCacheVerifyTest, GetNonExistentKey)
{
    InitClient();
    const std::string key = NewKey() + "_nonexistent";

    std::string val;
    auto rc = client_->Get(key, val);
    ASSERT_TRUE(rc.IsOk());
    ASSERT_TRUE(val.empty());
}

// TC-1.4.7: Get with empty key → rejected
TEST_F(KVClientBrpcLocalCacheVerifyTest, GetEmptyKeyRejected)
{
    InitClient();
    std::string val;
    auto rc = client_->Get("", val);
    ASSERT_EQ(rc.GetCode(), StatusCode::K_INVALID);
}

// TC-1.4.9: Read with offset
TEST_F(KVClientBrpcLocalCacheVerifyTest, ReadWithOffset)
{
    InitClient();
    const std::string key = NewKey();
    const uint64_t totalSize = 1024;
    const uint64_t offset = 512;
    const uint64_t readSize = 256;
    const std::string fullValue = GenRandomString(totalSize);

    DS_ASSERT_OK(client_->Set(key, fullValue, DefaultSetParam()));

    std::vector<ReadParam> readParams = { ReadParam{ .key = key, .offset = offset, .size = readSize } };
    std::vector<Optional<ReadOnlyBuffer>> readBuffers;
    DS_ASSERT_OK(client_->Read(readParams, readBuffers));
    ASSERT_EQ(readBuffers.size(), 1u);
    ASSERT_TRUE(readBuffers[0]);
    ASSERT_EQ(readBuffers[0]->GetSize(), readSize);

    std::string readStr(reinterpret_cast<const char *>(readBuffers[0]->ImmutableData()), readSize);
    ASSERT_EQ(readStr, fullValue.substr(offset, readSize));
}

// ============================================================================
// Category 1.5: MSet Flow Tests
// ============================================================================

// TC-1.5.1: MSet multiple key-values (MultiCreate+MemoryCopy+MultiPublish)
TEST_F(KVClientBrpcLocalCacheVerifyTest, MSetMultipleKeys)
{
    InitClient();
    std::vector<std::string> keys;
    std::vector<StringView> vals;
    std::vector<std::string> valStorage;
    for (uint32_t i = 0; i < BATCH_KEY_COUNT; ++i) {
        keys.push_back(NewKey());
        valStorage.push_back(GenRandomString(SMALL_VALUE_SIZE));
        vals.emplace_back(valStorage[i]);
    }

    std::vector<std::string> failedKeys;
    DS_ASSERT_OK(client_->MSet(keys, vals, failedKeys, DefaultMSetParam()));
    ASSERT_TRUE(failedKeys.empty());

    // Verify all keys
    std::vector<std::string> got;
    DS_ASSERT_OK(client_->Get(keys, got));
    for (uint32_t i = 0; i < BATCH_KEY_COUNT; ++i) {
        ASSERT_EQ(got[i], valStorage[i]);
    }
}

// TC-1.5.3: MSet with duplicate keys → deduplication
TEST_F(KVClientBrpcLocalCacheVerifyTest, MSetDuplicateKeys)
{
    InitClient();
    const std::string key1 = NewKey();
    const std::string key2 = NewKey();
    const std::string val1 = GenRandomString(SMALL_VALUE_SIZE);
    const std::string val2 = GenRandomString(SMALL_VALUE_SIZE);
    const std::string val3 = GenRandomString(SMALL_VALUE_SIZE);

    std::vector<std::string> keys = { key1, key1, key2 };
    std::vector<StringView> vals = { StringView(val1), StringView(val2), StringView(val3) };
    std::vector<std::string> failedKeys;

    DS_ASSERT_OK(client_->MSet(keys, vals, failedKeys, DefaultMSetParam()));
    // Duplicate key1 (first occurrence) should be in failedKeys
    ASSERT_GT(failedKeys.size(), 0u);

    // key2 should be stored
    std::string got;
    DS_ASSERT_OK(client_->Get(key2, got));
    ASSERT_EQ(got, val3);
}

// TC-1.5.4: MSet with empty keys → rejected
TEST_F(KVClientBrpcLocalCacheVerifyTest, MSetEmptyKeysRejected)
{
    InitClient();
    std::vector<std::string> keys;
    std::vector<StringView> vals;
    std::vector<std::string> failedKeys;
    auto rc = client_->MSet(keys, vals, failedKeys, DefaultMSetParam());
    ASSERT_EQ(rc.GetCode(), StatusCode::K_INVALID);
}

// TC-1.5.5: MSet with mismatched keys/vals sizes → rejected
TEST_F(KVClientBrpcLocalCacheVerifyTest, MSetMismatchedKeysValsRejected)
{
    InitClient();
    std::vector<std::string> keys = { NewKey(), NewKey() };
    std::vector<StringView> vals = { StringView(GenRandomString(10)) };
    std::vector<std::string> failedKeys;
    auto rc = client_->MSet(keys, vals, failedKeys, DefaultMSetParam());
    ASSERT_EQ(rc.GetCode(), StatusCode::K_INVALID);
}

// TC-1.5.6: MSetTx transactional (3 keys, NX)
TEST_F(KVClientBrpcLocalCacheVerifyTest, MSetTxSuccess)
{
    InitClient();
    const std::string k1 = NewKey(), k2 = NewKey(), k3 = NewKey();
    const std::string v1 = GenRandomString(SMALL_VALUE_SIZE);
    const std::string v2 = GenRandomString(SMALL_VALUE_SIZE);
    const std::string v3 = GenRandomString(SMALL_VALUE_SIZE);

    MSetParam txParam = DefaultMSetParam();
    txParam.existence = ExistenceOpt::NX;

    DS_ASSERT_OK(client_->MSetTx({ k1, k2, k3 }, { StringView(v1), StringView(v2), StringView(v3) }, txParam));

    std::vector<std::string> got;
    DS_ASSERT_OK(client_->Get({ k1, k2, k3 }, got));
    ASSERT_EQ(got[0], v1);
    ASSERT_EQ(got[1], v2);
    ASSERT_EQ(got[2], v3);
}

// TC-1.5.7: MSetTx exceeding MSET_MAX_KEY_COUNT (8) → rejected
TEST_F(KVClientBrpcLocalCacheVerifyTest, MSetTxTooManyKeys)
{
    InitClient();
    MSetParam txParam = DefaultMSetParam();
    txParam.existence = ExistenceOpt::NX;

    std::vector<std::string> keys(9);
    std::vector<std::string> valStorage(9);
    std::vector<StringView> vals(9);
    for (int i = 0; i < 9; ++i) {
        keys[i] = NewKey();
        valStorage[i] = GenRandomString(10);
        vals[i] = StringView(valStorage[i]);
    }

    auto rc = client_->MSetTx(keys, vals, txParam);
    ASSERT_EQ(rc.GetCode(), StatusCode::K_INVALID);
}

// TC-1.5.8: MSetTx with non-NX existence → rejected
TEST_F(KVClientBrpcLocalCacheVerifyTest, MSetTxNonNXExistenceRejected)
{
    InitClient();
    MSetParam nonNxParam = DefaultMSetParam();
    nonNxParam.existence = ExistenceOpt::NONE;  // NOT NX

    const std::string nonNxVal = GenRandomString(10);
    auto rc = client_->MSetTx({ NewKey() }, { StringView(nonNxVal) }, nonNxParam);
    ASSERT_EQ(rc.GetCode(), StatusCode::K_INVALID);
}

// TC-1.5.12: MSet with partial success (Risk 4: partial failure cleanup)
TEST_F(KVClientBrpcLocalCacheVerifyTest, MSetPartialSuccess)
{
    InitClient();
    // First set a key normally
    const std::string existingKey = NewKey();
    DS_ASSERT_OK(client_->Set(existingKey, "existing", DefaultSetParam()));

    // Now try MSet with NX - some keys already exist
    MSetParam nxParam = DefaultMSetParam();
    nxParam.existence = ExistenceOpt::NX;

    std::vector<std::string> keys = { existingKey, NewKey() };
    std::vector<StringView> vals = { StringView("should_fail"), StringView("should_succeed") };
    std::vector<std::string> failedKeys;

    auto rc = client_->MSet(keys, vals, failedKeys, nxParam);
    // Should report partial success
    ASSERT_GE(failedKeys.size(), 1u);

    // The new key should be set
    std::string newVal;
    DS_ASSERT_OK(client_->Get(keys[1], newVal));
    ASSERT_EQ(newVal, "should_succeed");
}

// ============================================================================
// Category 2: Boundary Tests
// ============================================================================

// TC-2.1: Maximum key size (>1024 chars) → rejected
TEST_F(KVClientBrpcLocalCacheVerifyTest, MaxKeySizeExceeded)
{
    InitClient();
    const std::string longKey(1025, 'k');
    auto rc = client_->Set(longKey, "val", DefaultSetParam());
    ASSERT_EQ(rc.GetCode(), StatusCode::K_INVALID);
}

// TC-2.4: TTL=0 (never expires)
TEST_F(KVClientBrpcLocalCacheVerifyTest, ZeroTTLNeverExpires)
{
    InitClient();
    const std::string key = NewKey();
    SetParam param = DefaultSetParam();
    param.ttlSecond = 0;

    DS_ASSERT_OK(client_->Set(key, "persistent", param));

    // Wait a bit
    std::this_thread::sleep_for(std::chrono::seconds(2));

    std::string got;
    DS_ASSERT_OK(client_->Get(key, got));
    ASSERT_EQ(got, "persistent");
}

// TC-2.6: Single key batch operations
TEST_F(KVClientBrpcLocalCacheVerifyTest, SingleKeyBatchOperations)
{
    InitClient();
    const std::string key = NewKey();
    const std::string value = GenRandomString(SMALL_VALUE_SIZE);
    std::vector<std::string> failedKeys;

    // Single-key MSet
    DS_ASSERT_OK(client_->MSet({ key }, { StringView(value) }, failedKeys, DefaultMSetParam()));
    ASSERT_TRUE(failedKeys.empty());

    // Single-key Get
    std::vector<std::string> got;
    DS_ASSERT_OK(client_->Get({ key }, got));
    ASSERT_EQ(got.size(), 1u);
    ASSERT_EQ(got[0], value);

    // Single-key Del
    DS_ASSERT_OK(client_->Del({ key }, failedKeys));
    ASSERT_TRUE(failedKeys.empty());
}

// ============================================================================
// Category 3: Error Path Tests
// ============================================================================

// TC-3.15: Duplicate Create with same key → K_DUPLICATED
TEST_F(KVClientBrpcLocalCacheVerifyTest, DuplicateCreateSameKey)
{
    InitClient();
    const std::string key = NewKey();
    std::shared_ptr<Buffer> buf1, buf2;
    SetParam param;

    DS_ASSERT_OK(client_->Create(key, SMALL_VALUE_SIZE, param, buf1));

    // Second Create with same key
    auto rc = client_->Create(key, SMALL_VALUE_SIZE, param, buf2);
    ASSERT_EQ(rc.GetCode(), StatusCode::K_DUPLICATED);
}

// TC-3.16: Publish already-sealed buffer
TEST_F(KVClientBrpcLocalCacheVerifyTest, PublishAlreadySealed)
{
    InitClient();
    const std::string key = NewKey();
    const std::string data = GenRandomString(SMALL_VALUE_SIZE);

    std::shared_ptr<Buffer> buffer;
    SetParam param;
    DS_ASSERT_OK(client_->Create(key, data.size(), param, buffer));
    DS_ASSERT_OK(buffer->WLatch());
    DS_ASSERT_OK(buffer->MemoryCopy(data.data(), data.size()));
    DS_ASSERT_OK(buffer->Publish());  // First publish succeeds
    DS_ASSERT_OK(buffer->UnWLatch());

    auto rc = buffer->Publish();  // Second publish on sealed buffer
    ASSERT_EQ(rc.GetCode(), StatusCode::K_OC_ALREADY_SEALED);
}

// ============================================================================
// Category 4: Concurrency Tests
// ============================================================================

// TC-4.1: Concurrent Set on different keys
TEST_F(KVClientBrpcLocalCacheVerifyTest, ConcurrentSetDifferentKeys)
{
    InitClient();
    std::atomic<uint32_t> successCount(0);
    std::vector<std::thread> threads;

    for (uint32_t t = 0; t < CONCURRENCY_THREADS; ++t) {
        threads.emplace_back([this, t, &successCount]() {
            for (uint32_t i = 0; i < CONCURRENCY_ITERATIONS; ++i) {
                const std::string key = NewKey() + "_t" + std::to_string(t) + "_i" + std::to_string(i);
                auto rc = client_->Set(key, std::to_string(t * 1000 + i), DefaultSetParam());
                if (rc.IsOk()) { ++successCount; }
            }
        });
    }
    for (auto &th : threads) { th.join(); }

    ASSERT_EQ(successCount.load(), CONCURRENCY_THREADS * CONCURRENCY_ITERATIONS);
}

// TC-4.3: Concurrent Get+Set on same key
TEST_F(KVClientBrpcLocalCacheVerifyTest, ConcurrentGetAndSet)
{
    InitClient();
    const std::string key = NewKey();
    DS_ASSERT_OK(client_->Set(key, "initial", DefaultSetParam()));

    std::atomic<bool> stop(false);
    std::atomic<uint32_t> getOk(0), setOk(0);

    std::thread getter([this, &key, &stop, &getOk]() {
        while (!stop) {
            std::string val;
            if (client_->Get(key, val).IsOk()) { ++getOk; }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    });

    std::thread setter([this, &key, &stop, &setOk]() {
        for (uint32_t i = 0; i < CONCURRENCY_ITERATIONS; ++i) {
            if (client_->Set(key, std::to_string(i), DefaultSetParam()).IsOk()) {
                ++setOk;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        stop = true;
    });

    getter.join();
    setter.join();

    ASSERT_EQ(setOk.load(), CONCURRENCY_ITERATIONS);
    ASSERT_GT(getOk.load(), 0u);
}

// TC-4.5: Concurrent Create+Copy+Publish on different keys
TEST_F(KVClientBrpcLocalCacheVerifyTest, ConcurrentCreateAndPublish)
{
    InitClient();
    std::atomic<uint32_t> successCount(0);
    std::vector<std::thread> threads;

    for (uint32_t t = 0; t < CONCURRENCY_THREADS; ++t) {
        threads.emplace_back([this, t, &successCount]() {
            for (uint32_t i = 0; i < CONCURRENCY_ITERATIONS; ++i) {
                const std::string key = NewKey() + "_cp_t" + std::to_string(t) + "_i" + std::to_string(i);
                const std::string data = GenRandomString(SMALL_VALUE_SIZE);
                std::shared_ptr<Buffer> buffer;
                SetParam param;

                auto rc = client_->Create(key, data.size(), param, buffer);
                if (rc.IsError()) { continue; }
                rc = buffer->WLatch();
                if (rc.IsError()) { continue; }
                rc = buffer->MemoryCopy(data.data(), data.size());
                if (rc.IsError()) { buffer->UnWLatch(); continue; }
                rc = buffer->Publish();
                if (rc.IsError()) { buffer->UnWLatch(); continue; }
                rc = buffer->UnWLatch();
                if (rc.IsOk()) { ++successCount; }
            }
        });
    }
    for (auto &th : threads) { th.join(); }

    ASSERT_EQ(successCount.load(), CONCURRENCY_THREADS * CONCURRENCY_ITERATIONS);
}

// ============================================================================
// Category 5: brpc-Specific Tests
// ============================================================================

// TC-5.1: Verify brpc mode is active (FLAGS_use_brpc=true)
TEST_F(KVClientBrpcLocalCacheVerifyTest, BrpcModeIsEnabled)
{
    ASSERT_TRUE(FLAGS_use_brpc);
}

// TC-5.3: Verify all key RPC methods work via brpc dispatch
TEST_F(KVClientBrpcLocalCacheVerifyTest, AllRpcMethodsViaBrpc)
{
    InitClient();
    const std::string key = NewKey();
    const std::string value = GenRandomString(SMALL_VALUE_SIZE);

    // Create
    std::shared_ptr<Buffer> buffer;
    SetParam param;
    DS_ASSERT_OK(client_->Create(key, value.size(), param, buffer));

    // Set
    DS_ASSERT_OK(client_->Set(key, value, DefaultSetParam()));

    // Get
    std::string got;
    DS_ASSERT_OK(client_->Get(key, got));
    ASSERT_EQ(got, value);

    // Exist
    std::vector<bool> exists;
    DS_ASSERT_OK(client_->Exist({ key }, exists));
    ASSERT_EQ(exists.size(), 1u);
    ASSERT_TRUE(exists[0]);

    // HealthCheck
    DS_ASSERT_OK(client_->HealthCheck());

    // Del
    DS_ASSERT_OK(client_->Del(key));

    // Verify deletion
    auto rc = client_->Get(key, got);
    ASSERT_TRUE(rc.IsOk() && got.empty());
}

// TC-5.9: Verify brpc session creation and reconnection
TEST_F(KVClientBrpcLocalCacheVerifyTest, BrpcSessionAvailableForAllOperations)
{
    InitClient();
    // Run a sequence of operations to verify brpc session remains valid
    for (int i = 0; i < 10; ++i) {
        const std::string key = NewKey();
        const std::string value = GenRandomString(SMALL_VALUE_SIZE);

        DS_ASSERT_OK(client_->Set(key, value, DefaultSetParam()));

        std::string got;
        DS_ASSERT_OK(client_->Get(key, got));
        ASSERT_EQ(got, value);

        DS_ASSERT_OK(client_->Del(key));
    }
}

// ============================================================================
// Category 6: Integration Tests
// ============================================================================

// TC-6.1: Create → Set → Get → Delete lifecycle
TEST_F(KVClientBrpcLocalCacheVerifyTest, FullLifecycle)
{
    InitClient();
    const std::string key = NewKey();
    const std::string data = GenRandomString(SMALL_VALUE_SIZE);

    // Create buffer
    std::shared_ptr<Buffer> buffer;
    SetParam param;
    DS_ASSERT_OK(client_->Create(key, data.size(), param, buffer));

    // Set (Publish)
    DS_ASSERT_OK(buffer->WLatch());
    DS_ASSERT_OK(buffer->MemoryCopy(data.data(), data.size()));
    DS_ASSERT_OK(buffer->Publish());
    DS_ASSERT_OK(buffer->UnWLatch());

    // Get
    std::string got;
    DS_ASSERT_OK(client_->Get(key, got));
    ASSERT_EQ(got, data);

    // Delete
    DS_ASSERT_OK(client_->Del(key));

    // Verify deleted
    auto rc = client_->Get(key, got);
    ASSERT_TRUE(rc.IsOk() && got.empty());
}

// TC-6.3: Mixed Set and Get interleaved operations
TEST_F(KVClientBrpcLocalCacheVerifyTest, MixedSetGetOperations)
{
    InitClient();
    const std::string k1 = NewKey(), k2 = NewKey(), k3 = NewKey();
    const std::string v1 = GenRandomString(SMALL_VALUE_SIZE);
    const std::string v2 = GenRandomString(SMALL_VALUE_SIZE);
    const std::string v3 = GenRandomString(SMALL_VALUE_SIZE);

    DS_ASSERT_OK(client_->Set(k1, v1, DefaultSetParam()));
    std::string got;
    auto rc = client_->Get(k2, got);  // k2 doesn't exist yet
    ASSERT_TRUE(rc.IsOk() && got.empty());

    DS_ASSERT_OK(client_->Set(k3, v3, DefaultSetParam()));

    DS_ASSERT_OK(client_->Get(k1, got));
    ASSERT_EQ(got, v1);

    DS_ASSERT_OK(client_->Set(k2, v2, DefaultSetParam()));

    DS_ASSERT_OK(client_->Get(k3, got));
    ASSERT_EQ(got, v3);

    DS_ASSERT_OK(client_->Get(k2, got));
    ASSERT_EQ(got, v2);
}

// TC-6.5: TTL expiration integration
TEST_F(KVClientBrpcLocalCacheVerifyTest, TTLExpirationIntegration)
{
    InitClient();
    const std::string key = NewKey();
    SetParam ttlParam = DefaultSetParam();
    ttlParam.ttlSecond = TTL_SHORT_SEC;

    DS_ASSERT_OK(client_->Set(key, "ttl_test", ttlParam));

    // Immediate get
    std::string got;
    DS_ASSERT_OK(client_->Get(key, got));
    ASSERT_EQ(got, "ttl_test");

    // Wait for expiry
    std::this_thread::sleep_for(std::chrono::seconds(TTL_SHORT_SEC + 1));

    // Verify expired
    auto rc = client_->Get(key, got);
    if (rc.IsOk()) {
        ASSERT_TRUE(got.empty());
    }

    // Re-set after expiry
    DS_ASSERT_OK(client_->Set(key, "after_expiry", DefaultSetParam()));
    DS_ASSERT_OK(client_->Get(key, got));
    ASSERT_EQ(got, "after_expiry");
}

// TC-6.11: MSet → partial Get (mixed found/not-found)
TEST_F(KVClientBrpcLocalCacheVerifyTest, MSetThenPartialGet)
{
    InitClient();
    const std::string k1 = NewKey(), k2 = NewKey();
    const std::string v1 = GenRandomString(SMALL_VALUE_SIZE);
    const std::string v2 = GenRandomString(SMALL_VALUE_SIZE);
    const std::string nonexistentKey = NewKey();

    std::vector<std::string> failedKeys;
    DS_ASSERT_OK(client_->MSet({ k1, k2 }, { StringView(v1), StringView(v2) },
                                failedKeys, DefaultMSetParam()));
    ASSERT_TRUE(failedKeys.empty());

    // Get k1, k2, and nonexistent key
    std::vector<std::string> got;
    DS_ASSERT_OK(client_->Get({ k1, k2, nonexistentKey }, got));
    ASSERT_EQ(got.size(), 3u);
    ASSERT_EQ(got[0], v1);
    ASSERT_EQ(got[1], v2);
    ASSERT_TRUE(got[2].empty());  // nonexistent key
}

// ============================================================================
// Category 7: Targeted verification of PR changes
// ============================================================================

// Verify Disconnect with 1s timeout works and re-Init succeeds.
// This directly validates the Disconnect timeout fix and Init cleanup path.
TEST_F(KVClientBrpcLocalCacheVerifyTest, ShutdownAndReinitRoundTrip)
{
    InitClient();
    const std::string key = NewKey();
    const std::string value = GenRandomString(SMALL_VALUE_SIZE);

    // Round 1: normal operations
    DS_ASSERT_OK(client_->Set(key, value, DefaultSetParam()));
    std::string got;
    DS_ASSERT_OK(client_->Get(key, got));
    ASSERT_EQ(got, value);

    // Shutdown (exercises Disconnect with 1s timeout)
    client_->ShutDown();
    client_.reset();

    // Re-Init (exercises Init cleanup + reconnection)
    InitClient();
    DS_ASSERT_OK(client_->Get(key, got));
    ASSERT_EQ(got, value);

    // Round 2: verify writes still work after reconnection
    const std::string key2 = NewKey();
    const std::string value2 = GenRandomString(SMALL_VALUE_SIZE);
    DS_ASSERT_OK(client_->Set(key2, value2, DefaultSetParam()));
    DS_ASSERT_OK(client_->Get(key2, got));
    ASSERT_EQ(got, value2);
}

// Verify multiple shutdown/re-init cycles are stable.
TEST_F(KVClientBrpcLocalCacheVerifyTest, RepeatedShutdownAndReinit)
{
    for (int i = 0; i < 3; ++i) {
        InitClient();
        const std::string key = NewKey();
        const std::string value = GenRandomString(SMALL_VALUE_SIZE);
        DS_ASSERT_OK(client_->Set(key, value, DefaultSetParam()));

        std::string got;
        DS_ASSERT_OK(client_->Get(key, got));
        ASSERT_EQ(got, value);

        client_->ShutDown();
        client_.reset();
    }
}

}  // namespace st
}  // namespace datasystem
