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
 * Description: publish semantics test.
 */
#include "gtest/gtest.h"

#include "datasystem/common/log/log.h"
#include "datasystem/common/constants.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/utils/status.h"
#include "oc_client_common.h"

namespace datasystem {
namespace st {
class MockEvictionAllocatorTest : public CommonTest {
public:
    struct MemoryParams {
        uint64_t needSize;
        void *pointer;
        int fd;
        ptrdiff_t offset;
        uint64_t mmapSize;
    };
};

TEST_F(MockEvictionAllocatorTest, DISABLED_TestEvictMallocFree)
{
    auto *allocator = datasystem::memory::Allocator::Instance();
    uint64_t maxSize = 50u * 1024l * 1024ul;
    ASSERT_EQ(allocator->Init(maxSize, 100, 100), Status::OK());

    size_t shmSz = 25165824;
    size_t nonShmSz = 114688;

    // Buffer 1 Alloc 100KB.
    MemoryParams params{ .needSize = nonShmSz, .pointer = nullptr, .fd = 0, .offset = 0, .mmapSize = 0 };
    DS_ASSERT_OK(allocator->AllocateMemory(DEFAULT_TENANT_ID, params.needSize, false, params.pointer, params.fd,
                                           params.offset, params.mmapSize));
    // Buffer 2 Alloc 20MB.
    MemoryParams params2{ .needSize = shmSz, .pointer = nullptr, .fd = 0, .offset = 0, .mmapSize = 0 };
    DS_ASSERT_OK(allocator->AllocateMemory(DEFAULT_TENANT_ID, params2.needSize, false, params2.pointer, params2.fd,
                                           params2.offset, params2.mmapSize));
    // Buffer 1 Free.
    DS_ASSERT_OK(allocator->FreeMemory(params.pointer));
    // Buffer 2 Free.
    DS_ASSERT_OK(allocator->FreeMemory(params2.pointer));

    //  Buffer 3 Alloc 20MB.
    MemoryParams params3{ .needSize = shmSz, .pointer = nullptr, .fd = 0, .offset = 0, .mmapSize = 0 };
    DS_ASSERT_OK(allocator->AllocateMemory(DEFAULT_TENANT_ID, params3.needSize, false, params3.pointer, params3.fd,
                                           params3.offset, params3.mmapSize));
    // Buffer 3 Free.
    DS_ASSERT_OK(allocator->FreeMemory(params3.pointer));

    // Buffer 4 Alloc 20MB. Not Free.
    MemoryParams params4{ .needSize = shmSz, .pointer = nullptr, .fd = 0, .offset = 0, .mmapSize = 0 };
    DS_ASSERT_OK(allocator->AllocateMemory(DEFAULT_TENANT_ID, params4.needSize, false, params4.pointer, params4.fd,
                                           params4.offset, params4.mmapSize));

    // Buffer 5 Alloc.
    MemoryParams params5{ .needSize = shmSz, .pointer = nullptr, .fd = 0, .offset = 0, .mmapSize = 0 };
    DS_ASSERT_OK(allocator->AllocateMemory(DEFAULT_TENANT_ID, params5.needSize, false, params5.pointer, params5.fd,
                                           params5.offset, params5.mmapSize));
    allocator->Shutdown();
}

class EvictionMemoryTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 3;
        opts.numOBS = 1;
        opts.numEtcd = 1;
        opts.workerGflagParams = " -v=1 -shared_memory_size_mb=100";
        FLAGS_v = 1;
    }

    void GetAndVerify(const std::shared_ptr<ObjectClient> &client, const std::string &objKey,
                      std::vector<uint8_t> &bytes2) const
    {
        std::vector<Optional<Buffer>> buffers;
        ASSERT_EQ(client->Get({ objKey }, timeOut, buffers), Status::OK());
        ASSERT_TRUE(NotExistsNone(buffers));
        ASSERT_EQ(buffers.size(), 1u);
        auto &buf = *buffers[0];
        std::string expected;
        ArrToStr(bytes2.data(), bytes2.size(), expected);
        ASSERT_EQ((size_t)buf.GetSize(), bytes2.size());
        ASSERT_BUF_EQ(buf, expected);
    }

protected:
    std::string objKey0 = "objKey0";
    std::string objKey1 = "objKey1";
    // Attention:  size_t shmSz = 20 * 1024ul * 1024ul; is not ok because of jemalloc.
    size_t shmSz = 20 * 1000ul * 1000ul;
    size_t nonShmSz = 100'000;
    CreateParam createParam = { .consistencyType = ConsistencyType::CAUSAL };
    int64_t timeOut = 1'000;
};

TEST_F(EvictionMemoryTest, EvictionWithRedis)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    std::shared_ptr<ObjectClient> client3;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
    InitTestClient(2, client3);
    RandomData randomData;
    auto bytes1 = randomData.RandomBytes(nonShmSz);
    auto bytes2 = randomData.RandomBytes(shmSz);
    auto bytes3 = randomData.RandomBytes(shmSz);
    auto bytes4 = randomData.RandomBytes(shmSz);

    std::vector<std::string> failedIds;
    client1->GIncreaseRef({ objKey0 }, failedIds);
    client1->Put(objKey0, bytes1.data(), bytes1.size(), createParam);  // obj0: 100KB.
    client1->Put(objKey0, bytes2.data(), bytes2.size(), createParam);  // obj0. 20MB.
    client1->GIncreaseRef({ objKey1 }, failedIds);
    client1->Put(objKey1, bytes3.data(), bytes3.size(), createParam);  // obj1. 20MB.
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(client1->Create(objKey1, bytes4.size(), createParam, buffer));  // obj1. 20MB. (buffer).
    {
        ASSERT_EQ(buffer->WLatch(), Status::OK());
        datasystem::Raii unlatch([&buffer]() { buffer->UnWLatch(); });
        ASSERT_EQ(buffer->MemoryCopy(bytes4.data(), bytes4.size()), Status::OK());
        ASSERT_EQ(buffer->Publish(), Status::OK());
    }

    // Verify obj0.
    GetAndVerify(client1, objKey0, bytes2);
    client2->GIncreaseRef({ objKey0 }, failedIds);
    GetAndVerify(client2, objKey0, bytes2);
    client3->GIncreaseRef({ objKey0 }, failedIds);
    GetAndVerify(client3, objKey0, bytes2);

    // Verify obj1.
    GetAndVerify(client1, objKey1, bytes4);
    client2->GIncreaseRef({ objKey1 }, failedIds);
    GetAndVerify(client2, objKey1, bytes4);
    client3->GIncreaseRef({ objKey1 }, failedIds);
    GetAndVerify(client3, objKey1, bytes4);

    DS_ASSERT_OK(client1->GDecreaseRef({ objKey0, objKey1 }, failedIds));
    ASSERT_TRUE(failedIds.empty());
    DS_ASSERT_OK(client2->GDecreaseRef({ objKey0, objKey1 }, failedIds));
    ASSERT_TRUE(failedIds.empty());
    DS_ASSERT_OK(client3->GDecreaseRef({ objKey0, objKey1 }, failedIds));
    ASSERT_TRUE(failedIds.empty());
}

class OCClientPublishTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 3;
        opts.numEtcd = 1;
        opts.workerGflagParams = " -v=1 ";
        FLAGS_v = 1;
    }

    template <typename F>
    void PublishOrSeal(Buffer &buffer, size_t createSz, F f, bool assert = true)
    {
        RandomData randomData;
        auto bytes = randomData.RandomBytes(createSz);
        ASSERT_EQ(buffer.WLatch(), Status::OK());
        datasystem::Raii unlatch([&buffer]() { buffer.UnWLatch(); });
        ASSERT_EQ(buffer.MemoryCopy(bytes.data(), createSz), Status::OK());
        if (assert) {
            ASSERT_EQ(f(), Status::OK());
        } else {
            f();
        }
    }

    void CreateAndPublish(const std::shared_ptr<ObjectClient> &client, int createSz = -1)
    {
        if (createSz == -1) {
            createSz = sz;
        }
        std::shared_ptr<Buffer> buffer;
        DS_ASSERT_OK(client->Create(objKey, createSz, createParam, buffer));
        std::vector<std::string> failedIds;
        DS_ASSERT_OK(client->GIncreaseRef({ objKey }, failedIds));
        PublishOrSeal(*buffer, createSz, [&buffer]() -> Status { return buffer->Publish(); });
    }

    static uint64_t ToHashInt(const std::vector<uint8_t> &bytes)
    {
        std::hash<std::string> hash;
        auto str = std::string{ bytes.begin(), bytes.end() };
        return hash(str);
    }

    void CreateAndPublish(const std::shared_ptr<ObjectClient> &client, size_t createSz, std::vector<uint8_t> &outBytes)
    {
        std::shared_ptr<Buffer> buf;
        DS_ASSERT_OK(client->Create(objKey, createSz, createParam, buf));
        RandomData randomData;
        auto bytes = randomData.RandomBytes(sz);
        ASSERT_EQ(buf->WLatch(), Status::OK());
        datasystem::Raii unlatch([&buf]() { buf->UnWLatch(); });
        buf->MemoryCopy(bytes.data(), sz);

        outBytes = std::move(bytes);
        std::vector<std::string> failedIds;
        DS_ASSERT_OK(client->GIncreaseRef({ objKey }, failedIds));
        ASSERT_TRUE(failedIds.empty());
        DS_ASSERT_OK(buf->Publish());
    }

    void GetAndPublish(const std::shared_ptr<ObjectClient> &client, std::vector<uint8_t> &outBytes)
    {
        std::vector<Optional<Buffer>> buffers;
        Status status = Status(StatusCode::K_RUNTIME_ERROR, "");

        // May fail due to object expired.
        do {
            status = client->Get({ objKey }, timeOut, buffers);
        } while (status.IsError() || ExistsNone(buffers));
        ASSERT_TRUE(NotExistsNone(buffers));
        auto &buf = buffers[0];
        if (!buf) {
            LOG(ERROR) << status.ToString();
            LOG(ERROR) << buffers.size();
        }
        ASSERT_EQ(bool(buf), true);
        RandomData randomData;
        auto bytes = randomData.RandomBytes(sz);
        ASSERT_EQ(buf->WLatch(), Status::OK());
        datasystem::Raii unlatch([&buf]() { buf->UnWLatch(); });
        buf->MemoryCopy(bytes.data(), sz);

        outBytes = std::move(bytes);
        std::vector<std::string> failedIds;
        DS_ASSERT_OK(client->GIncreaseRef({ objKey }, failedIds));
        ASSERT_TRUE(failedIds.empty());
        DS_ASSERT_OK(buf->Publish());
    }

    uint64_t GetAndHashBytes(const std::shared_ptr<ObjectClient> &client, Status &status)
    {
        std::vector<Optional<Buffer>> buffers;
        // May fail due to object expired.
        status = client->Get({ objKey }, timeOut, buffers);
        if (status.IsOk() && !buffers.empty() && buffers[0]) {
            auto &buf = buffers[0];
            auto latchStatus = buf->RLatch();
            if (latchStatus.IsError()) {
                return 0;
            }
            datasystem::Raii unlatch([&buf]() { buf->UnRLatch(); });
            std::string str((char *)buf->ImmutableData(), buf->GetSize());
            std::hash<std::string> hash;
            return hash(str);
        } else {
            LOG(ERROR) << status.ToString();
        }
        return 0;
    }

    void GetAndVerify(const std::shared_ptr<ObjectClient> &client, std::vector<uint8_t> &bytes2)
    {
        std::vector<Optional<Buffer>> buffers;
        ASSERT_EQ(client->Get({ objKey }, timeOut, buffers), Status::OK());
        ASSERT_TRUE(NotExistsNone(buffers));
        ASSERT_EQ(buffers.size(), 1u);
        auto &buf = *buffers[0];
        std::string expected;
        ArrToStr(bytes2.data(), bytes2.size(), expected);
        ASSERT_EQ((size_t)buf.GetSize(), bytes2.size());
        ASSERT_BUF_EQ(buf, expected);
    }

    static void PrintBytes(const std::vector<std::vector<uint8_t>> &bytesLst)
    {
        for (auto &byteArr : bytesLst) {
            std::string str;
            std::hash<std::string> hash;
            size_t threshold = 100;
            ArrToStr((void *)byteArr.data(), byteArr.size(), str);
            LOG(INFO) << "md5 of bytes: " << hash(str) << " , SubStr:" << str.substr(0, threshold);
        }
    }

protected:
    std::string objKey = "objKey";
    size_t sz = 10'000'000;
    size_t nonShmSz = 100;
    CreateParam createParam = { .consistencyType = ConsistencyType::CAUSAL };
    int64_t timeOut = 1'000;
};

TEST_F(OCClientPublishTest, TestCacheInvalidPublish)
{
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1);
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(1, client2);
    std::shared_ptr<ObjectClient> client3;
    InitTestClient(2, client3);

    // 1st: Create shm object and decrease shm reference to 0.
    std::vector<uint8_t> data(sz, 0);
    DS_ASSERT_OK(client1->Put(objKey, data.data(), sz, createParam));

    // 2nd: Another client triggers cache invalidation.
    {
        std::vector<Optional<Buffer>> buffers;
        DS_ASSERT_OK(client2->Get({ objKey }, timeOut, buffers));
        ASSERT_TRUE(NotExistsNone(buffers));
        auto &buf = buffers[0];
        PublishOrSeal(buf.value(), sz, [&buf]() -> Status { return buf->Publish(); });
    }

    // 3rd: Expect publish success after invalidation.
    DS_ASSERT_OK(client1->Put(objKey, data.data(), sz, createParam));

    // 4th: Get and verify correctness.
    std::hash<std::string> myHash;
    uint64_t groundTruth = myHash(std::string((char *)data.data(), data.size()));
    Status status;
    ASSERT_EQ(GetAndHashBytes(client1, status), groundTruth);
    ASSERT_EQ(status, Status::OK());
    ASSERT_EQ(GetAndHashBytes(client2, status), groundTruth);
    ASSERT_EQ(status, Status::OK());
    ASSERT_EQ(GetAndHashBytes(client3, status), groundTruth);
    ASSERT_EQ(status, Status::OK());
}

TEST_F(OCClientPublishTest, LEVEL1_TestEventualConsistentConcurrentPub)
{
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1);
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(1, client2);
    std::vector<std::shared_ptr<ObjectClient>> clients = { client1, client2 };

    size_t numOfClients = 2;
    ThreadPool pool(numOfClients);
    int numOfRounds = 10;
    for (int round = 0; round < numOfRounds; round++) {
        // 1st: Concurrent create-and-publish and trigger cache invalidation.
        std::vector<std::future<std::vector<uint8_t>>> futures;
        futures.emplace_back(pool.Submit([this, client1]() {
            std::vector<uint8_t> outHash;
            CreateAndPublish(client1, sz, outHash);
            return outHash;
        }));
        futures.emplace_back(pool.Submit([this, client2]() {
            std::vector<uint8_t> outHash;
            CreateAndPublish(client2, sz, outHash);
            return outHash;
        }));

        std::vector<std::vector<uint8_t>> bytesArr;
        for (auto &fut : futures) {
            bytesArr.emplace_back(fut.get());
        }

        // 2nd: Verify they are the same.
        std::vector<uint64_t> getHashCodes;
        for (const auto &client : clients) {
            Status status;
            getHashCodes.emplace_back(GetAndHashBytes(client, status));
            ASSERT_EQ(status, Status::OK());
        }
        // 3rd: Output Put HashCode.
        std::vector<uint64_t> putHashCodes;
        for (const auto &bytes : bytesArr) {
            putHashCodes.emplace_back(ToHashInt(bytes));
        }
        LOG(INFO) << "put hash values: " << VectorToString(putHashCodes);
        // Verify Get HashCode.
        uint64_t frontVal = getHashCodes.front();
        LOG(INFO) << "get hash values: " << VectorToString(getHashCodes);
        for (auto v : getHashCodes) {
            ASSERT_EQ(v, frontVal);
        }

        // 4th: Delete.
        std::vector<std::string> failedIds;
        DS_ASSERT_OK(client1->GDecreaseRef({ objKey }, failedIds));
        ASSERT_EQ(failedIds.size(), size_t(0));
        DS_ASSERT_OK(client2->GDecreaseRef({ objKey }, failedIds));
        ASSERT_EQ(failedIds.size(), size_t(0));
    }
}

// Test Case Ensure Notification in Concurrent Create.
TEST_F(OCClientPublishTest, LEVEL1_TestEventualConsistent)
{
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1);
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(1, client2);
    std::shared_ptr<ObjectClient> client3;
    InitTestClient(2, client3);
    std::vector<std::shared_ptr<ObjectClient>> clients = { client1, client2, client3 };

    int numOfRounds = 10;
    size_t numOfClients = clients.size();
    ThreadPool pool(numOfClients);
    for (int round = 0; round < numOfRounds; round++) {
        // 1st: Create shm object and decrease shm reference to 0.
        std::vector<uint8_t> data(sz, 0);
        DS_ASSERT_OK(client1->Put(objKey, data.data(), sz, createParam));
        std::string dataStr(data.begin(), data.end());

        // 2nd: Sync cache invalidation (causal consistency) and create again.
        std::vector<std::future<std::vector<uint8_t>>> futures;
        futures.emplace_back(pool.Submit([this, client1]() {
            std::vector<uint8_t> outHash;
            GetAndPublish(client1, outHash);
            return outHash;
        }));
        futures.emplace_back(pool.Submit([this, client2]() {
            std::vector<uint8_t> outHash;
            GetAndPublish(client2, outHash);
            return outHash;
        }));
        futures.emplace_back(pool.Submit([this, client3]() {
            std::vector<uint8_t> outHash;
            GetAndPublish(client3, outHash);
            return outHash;
        }));

        std::vector<std::vector<uint8_t>> bytesArr;
        bytesArr.emplace_back(std::move(data));
        for (auto &fut : futures) {
            bytesArr.emplace_back(fut.get());
        }

        // 3rd: Verify they are the same.
        std::vector<uint64_t> getHashCodes;
        for (const auto &client : clients) {
            Status status;
            getHashCodes.emplace_back(GetAndHashBytes(client, status));
            ASSERT_EQ(status, Status::OK());
        }
        // Output Put HashCode.
        std::vector<uint64_t> putHashCodes;
        for (const auto &bytes : bytesArr) {
            putHashCodes.emplace_back(ToHashInt(bytes));
        }
        LOG(INFO) << "put hash values: " << VectorToString(putHashCodes);
        // Verify Get HashCode.
        uint64_t frontVal = getHashCodes.front();
        LOG(INFO) << "get hash values: " << VectorToString(getHashCodes);
        for (auto v : getHashCodes) {
            ASSERT_EQ(v, frontVal);
        }

        // 4th: Delete.
        std::vector<std::string> failedIds;
        DS_ASSERT_OK(client1->GDecreaseRef({ objKey }, failedIds));
        ASSERT_EQ(failedIds.size(), size_t(0));
        DS_ASSERT_OK(client2->GDecreaseRef({ objKey }, failedIds));
        ASSERT_EQ(failedIds.size(), size_t(0));
        DS_ASSERT_OK(client3->GDecreaseRef({ objKey }, failedIds));
        ASSERT_EQ(failedIds.size(), size_t(0));
    }
}

TEST_F(OCClientPublishTest, TestPutGetPubAndDel)
{
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1);

    int numOfRounds = 10;
    for (int i = 0; i < numOfRounds; i++) {
        std::vector<uint8_t> data(sz, 0);
        std::vector<std::string> failedIds;
        DS_ASSERT_OK(client1->GIncreaseRef({ objKey }, failedIds));
        ASSERT_EQ(failedIds.size(), size_t(0));
        DS_ASSERT_OK(client1->Put(objKey, data.data(), sz, createParam));

        std::vector<uint8_t> outHash;
        GetAndPublish(client1, outHash);
        DS_ASSERT_OK(client1->GDecreaseRef({ objKey }, failedIds));
        ASSERT_EQ(failedIds.size(), size_t(0));
    }
}

TEST_F(OCClientPublishTest, TestPutAndDel)
{
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1);

    int numOfRounds = 10;
    for (int i = 0; i < numOfRounds; i++) {
        std::vector<uint8_t> data(sz, 0);
        std::vector<std::string> failedIds;
        DS_ASSERT_OK(client1->GIncreaseRef({ objKey }, failedIds));
        ASSERT_EQ(failedIds.size(), size_t(0));
        DS_ASSERT_OK(client1->Put(objKey, data.data(), sz, createParam));

        DS_ASSERT_OK(client1->GDecreaseRef({ objKey }, failedIds));
        ASSERT_EQ(failedIds.size(), size_t(0));
    }
}

TEST_F(OCClientPublishTest, TestShmCreateCreate)
{
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1);
    std::shared_ptr<Buffer> buffer1;
    std::shared_ptr<Buffer> buffer2;
    ASSERT_EQ(client1->Create(objKey, sz, createParam, buffer1), Status::OK());
    ASSERT_EQ(client1->Create(objKey, sz, createParam, buffer2), Status::OK());

    ASSERT_EQ(buffer1->Publish(), Status::OK());
    ASSERT_EQ(buffer2->Publish(), Status::OK());
}

TEST_F(OCClientPublishTest, TestNonShmCreateCreate)
{
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1);
    std::shared_ptr<Buffer> buffer1;
    std::shared_ptr<Buffer> buffer2;
    ASSERT_EQ(client1->Create(objKey, nonShmSz, createParam, buffer1), Status::OK());
    ASSERT_EQ(client1->Create(objKey, nonShmSz, createParam, buffer2), Status::OK());

    ASSERT_EQ(buffer1->Publish(), Status::OK());
    ASSERT_EQ(buffer2->Publish(), Status::OK());
}

TEST_F(OCClientPublishTest, TestNonShmCreateCreate2)
{
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1);
    {
        std::shared_ptr<Buffer> buffer1;
        ASSERT_EQ(client1->Create(objKey, nonShmSz, createParam, buffer1), Status::OK());
        ASSERT_EQ(buffer1->Publish(), Status::OK());
    }
    std::shared_ptr<Buffer> buffer2;
    ASSERT_EQ(client1->Create(objKey, nonShmSz, createParam, buffer2), Status::OK());
    ASSERT_EQ(buffer2->Publish(), Status::OK());
}

TEST_F(OCClientPublishTest, CrossNodeCreatePub)
{
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1);
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(1, client2);

    CreateAndPublish(client1);
    CreateAndPublish(client2);
}

TEST_F(OCClientPublishTest, CrossNodeCreatePubGet)
{
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1);
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(1, client2);
    std::shared_ptr<ObjectClient> client3;
    InitTestClient(1, client3);

    CreateAndPublish(client1, 1'000'000);
    std::vector<Optional<Buffer>> buffers;
    // Verify Result.
    ASSERT_EQ(client2->Get({ objKey }, timeOut, buffers), Status::OK());
    ASSERT_TRUE(NotExistsNone(buffers));
    CreateAndPublish(client2, 2'000'000);

    // Verify Result.
    ASSERT_EQ(client3->Get({ objKey }, timeOut, buffers), Status::OK());
    ASSERT_TRUE(NotExistsNone(buffers));
    CreateAndPublish(client2, 3'000'000);
}

TEST_F(OCClientPublishTest, VarySzCreateUpdateMetaGet)
{
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1);
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(1, client2);
    int64_t smallSz = 100'000;
    int64_t largeSz = 200'000;

    // Trigger Create & Update.
    std::shared_ptr<Buffer> buffer1;
    std::shared_ptr<Buffer> buffer2;
    RandomData randomData;
    auto bytes2 = randomData.RandomBytes(largeSz);
    {
        ASSERT_EQ(Status::OK(), client1->Create(objKey, smallSz, createParam, buffer1));
        auto bytes = randomData.RandomBytes(smallSz);
        ASSERT_EQ(buffer1->WLatch(), Status::OK());
        datasystem::Raii unlatch([&buffer1]() { buffer1->UnWLatch(); });
        ASSERT_EQ(buffer1->MemoryCopy(bytes.data(), bytes.size()), Status::OK());

        ASSERT_EQ(Status::OK(), client1->Create(objKey, largeSz, createParam, buffer2));
        ASSERT_EQ(buffer2->WLatch(), Status::OK());
        datasystem::Raii unlatch2([&buffer2]() { buffer2->UnWLatch(); });
        ASSERT_EQ(buffer2->MemoryCopy(bytes2.data(), bytes2.size()), Status::OK());

        buffer1->Publish();
        buffer2->Publish();
    }
    GetAndVerify(client1, bytes2);
    GetAndVerify(client2, bytes2);
}

TEST_F(OCClientPublishTest, VarySzCreateCreateMetaGet)
{
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1);
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(1, client2);
    std::shared_ptr<ObjectClient> client3;
    InitTestClient(2, client3);
    int64_t smallSz = 100'000;
    int64_t largeSz = 200'000;

    // Trigger Create & Update.
    std::shared_ptr<Buffer> buffer1;
    std::shared_ptr<Buffer> buffer2;
    RandomData randomData;
    auto bytes2 = randomData.RandomBytes(largeSz);
    {
        ASSERT_EQ(Status::OK(), client1->Create(objKey, smallSz, createParam, buffer1));
        auto bytes = randomData.RandomBytes(smallSz);
        ASSERT_EQ(buffer1->WLatch(), Status::OK());
        datasystem::Raii unlatch([&buffer1]() { buffer1->UnWLatch(); });
        ASSERT_EQ(buffer1->MemoryCopy(bytes.data(), bytes.size()), Status::OK());

        ASSERT_EQ(Status::OK(), client2->Create(objKey, largeSz, createParam, buffer2));
        ASSERT_EQ(buffer2->WLatch(), Status::OK());
        datasystem::Raii unlatch2([&buffer2]() { buffer2->UnWLatch(); });
        ASSERT_EQ(buffer2->MemoryCopy(bytes2.data(), bytes2.size()), Status::OK());

        buffer1->Publish();
        buffer2->Publish();
    }
    GetAndVerify(client1, bytes2);
    GetAndVerify(client2, bytes2);
    GetAndVerify(client3, bytes2);
}

TEST_F(OCClientPublishTest, VarySzMultiPub)
{
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1);
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(1, client2);
    std::string objKey = "obj0";
    int64_t smallSz = 100'000;
    int64_t largeSz = 200'000;
    auto smallBytes = randomData_.RandomBytes(smallSz);
    auto largeBytes = randomData_.RandomBytes(largeSz);
    client1->Put(objKey, smallBytes.data(), smallBytes.size(), createParam, {});
    client1->Put(objKey, largeBytes.data(), largeBytes.size(), createParam, {});
    client1->Put(objKey, smallBytes.data(), smallBytes.size(), createParam, {});

    {
        std::vector<Optional<Buffer>> buffers;
        DS_ASSERT_OK(client2->Get({ objKey }, timeOut, buffers));
        ASSERT_TRUE(NotExistsNone(buffers));
        ASSERT_EQ(buffers.size(), 1u);
        auto &buf = buffers[0];
        ASSERT_TRUE(buf->GetSize() != largeSz);
        std::shared_ptr<Buffer> bufferPtr;
        {
            RandomData randomData;
            auto bytes = randomData.RandomBytes(largeSz);
            ASSERT_EQ(buf->WLatch(), Status::OK());
            datasystem::Raii unlatch([&buf]() { buf->UnWLatch(); });
            ASSERT_FALSE(buf->MemoryCopy(bytes.data(), bytes.size()) == Status::OK());
        }
        ASSERT_EQ(Status::OK(), client2->Create(objKey, largeSz, createParam, bufferPtr));
        PublishOrSeal(*bufferPtr, largeSz, [&bufferPtr]() -> Status { return bufferPtr->Publish(); });
    }

    {
        std::vector<Optional<Buffer>> buffers;
        DS_ASSERT_OK(client1->Get({ objKey }, timeOut, buffers));
        ASSERT_TRUE(NotExistsNone(buffers));
        ASSERT_EQ(buffers.size(), 1u);
        auto &buf = buffers[0];

        ASSERT_TRUE(buf->GetSize() != smallSz);
        ASSERT_EQ(buf->GetSize(), largeSz);
        {
            RandomData randomData;
            auto bytes = randomData.RandomBytes(smallSz);
            ASSERT_EQ(buf->WLatch(), Status::OK());
            datasystem::Raii unlatch([&buf]() { buf->UnWLatch(); });
            ASSERT_EQ(buf->MemoryCopy(bytes.data(), bytes.size()), Status::OK());
        }
        std::shared_ptr<Buffer> bufferPtr;
        ASSERT_EQ(Status::OK(), client2->Create(objKey, smallSz, createParam, bufferPtr));
        PublishOrSeal(*bufferPtr, smallSz, [&bufferPtr]() -> Status { return bufferPtr->Publish(); });
    }
}

TEST_F(OCClientPublishTest, SealForwardingShimSemantics)
{
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1);
    int64_t sz = 100'000;

    // Seal forwards to Publish: Create -> Seal -> Get reads the data.
    std::shared_ptr<Buffer> buffer1;
    RandomData randomData;
    auto bytes1 = randomData.RandomBytes(sz);
    {
        DS_ASSERT_OK(client1->Create(objKey, sz, createParam, buffer1));
        DS_ASSERT_OK(buffer1->WLatch());
        datasystem::Raii unlatch([&buffer1]() { buffer1->UnWLatch(); });
        DS_ASSERT_OK(buffer1->MemoryCopy(bytes1.data(), bytes1.size()));
        DS_ASSERT_OK(buffer1->Seal());
    }
    GetAndVerify(client1, bytes1);

    // Seal no longer pins the object: a later Publish overwrites it.
    std::shared_ptr<Buffer> buffer2;
    auto bytes2 = randomData.RandomBytes(sz);
    {
        DS_ASSERT_OK(client1->Create(objKey, sz, createParam, buffer2));
        DS_ASSERT_OK(buffer2->WLatch());
        datasystem::Raii unlatch([&buffer2]() { buffer2->UnWLatch(); });
        DS_ASSERT_OK(buffer2->MemoryCopy(bytes2.data(), bytes2.size()));
        DS_ASSERT_OK(buffer2->Publish());
    }
    GetAndVerify(client1, bytes2);
}
}  // namespace st
}  // namespace datasystem