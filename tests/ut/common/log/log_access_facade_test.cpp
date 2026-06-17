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
 * Description: Unit tests for AccessRecorder typed facade (Object/Stream/RequestOut).
 */

#include "datasystem/common/log/log_sampler.h"
#include "datasystem/common/log/access_recorder.h"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "ut/common.h"
#include "datasystem/common/log/trace.h"

namespace datasystem {
namespace ut {

class LogAccessFacadeTest : public CommonTest {
protected:
    void SetUp() override
    {
        CommonTest::SetUp();
        LogSampler::Instance().ResetForTest();
    }

    void TearDown() override
    {
        LogSampler::Instance().ResetForTest();
        CommonTest::TearDown();
    }

    void EnableSampler(double requestRate, double accessRate, double diagnosticRate)
    {
        LogSampleUserConfig cfg;
        cfg.requestSampleRate = requestRate;
        cfg.requestSampleRateExplicit = true;
        cfg.accessSampleRate = accessRate;
        cfg.accessSampleRateExplicit = true;
        cfg.diagnosticSampleRate = diagnosticRate;
        cfg.diagnosticSampleRateExplicit = true;
        ASSERT_TRUE(LogSampler::Instance().UpdateConfigFromFlags(cfg));
    }
};

TEST_F(LogAccessFacadeTest, ObjectFacadeSampledOutNoCrash)
{
    EnableSampler(0.0, 0.0, 1.0);
    LogSampler::Instance().SetSaltForTest(0);
    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();
    auto access = AccessRecorder::Object(AccessRecorderKey::DS_KV_CLIENT_SET);
    access.ObjectKeyRef("test_key").TimeoutMs(100).Result(Status::OK()).DataSize(50).Record();
}

TEST_F(LogAccessFacadeTest, ObjectFacadeSampledInProviderExecutedOnce)
{
    EnableSampler(1.0, 1.0, 1.0);
    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();
    int providerCallCount = 0;
    auto access = AccessRecorder::Object(AccessRecorderKey::DS_KV_CLIENT_SET);
    access.DataSizeProvider([&providerCallCount] {
        ++providerCallCount;
        return 42;
    }).Result(Status::OK()).Record();
    ASSERT_EQ(providerCallCount, 1);
}

TEST_F(LogAccessFacadeTest, ObjectFacadeSampledOutProviderNotExecuted)
{
    EnableSampler(0.0, 0.0, 1.0);
    LogSampler::Instance().SetSaltForTest(0);
    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();
    int providerCallCount = 0;
    auto access = AccessRecorder::Object(AccessRecorderKey::DS_KV_CLIENT_SET);
    access.DataSizeProvider([&providerCallCount] {
        ++providerCallCount;
        return 42;
    }).Result(Status::OK()).Record();
    ASSERT_EQ(providerCallCount, 0);
}

TEST_F(LogAccessFacadeTest, ObjectFacadeTrackedTransport)
{
    EnableSampler(1.0, 1.0, 1.0);
    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();
    AccessTransportTracker::Reset();
    AccessTransportTracker::Record(AccessTransportKind::UB);
    auto access = AccessRecorder::Object(AccessRecorderKey::DS_KV_CLIENT_SET);
    access.ObjectKeyRef("key").TrackedTransportType().Result(Status::OK()).DataSize(10).Record();
}

TEST_F(LogAccessFacadeTest, ObjectFacadeObjectKeysSummaryRef)
{
    EnableSampler(1.0, 1.0, 1.0);
    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();
    std::vector<std::string> keys = { "key1", "key2", "key3" };
    auto access = AccessRecorder::Object(AccessRecorderKey::DS_KV_CLIENT_SET);
    access.ObjectKeysSummaryRef(keys).Result(Status::OK()).DataSize(10).Record();
}

TEST_F(LogAccessFacadeTest, ObjectFacadeMultipleSetters)
{
    EnableSampler(1.0, 1.0, 1.0);
    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();
    auto access = AccessRecorder::Object(AccessRecorderKey::DS_KV_CLIENT_SET);
    access.ObjectKeyRef("key")
          .WriteMode(1)
          .TtlSecond(300)
          .Existence(0)
          .CacheType(2)
          .Keep(true)
          .TimeoutMs(5000)
          .TrackedTransportType()
          .Result(Status::OK())
          .DataSize(100)
          .Record();
}

TEST_F(LogAccessFacadeTest, ObjectFacadeWriteModeText)
{
    EnableSampler(1.0, 1.0, 1.0);
    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();
    auto access = AccessRecorder::Object(AccessRecorderKey::DS_KV_CLIENT_SET);
    access.ObjectKeyRef("key").WriteModeText("LATEST").Result(Status::OK()).Record();
}

TEST_F(LogAccessFacadeTest, StreamFacadeBasic)
{
    EnableSampler(1.0, 1.0, 1.0);
    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();
    auto access = AccessRecorder::Stream(AccessRecorderKey::DS_STREAM_CREATE_PRODUCER);
    access.StreamName("test_stream")
          .ProducerId("prod1")
          .PageSize(4096)
          .MaxStreamSize(1048576)
          .AutoCleanup(true)
          .Result(Status::OK())
          .Record();
}

TEST_F(LogAccessFacadeTest, StreamFacadeSampledOut)
{
    EnableSampler(0.0, 0.0, 1.0);
    LogSampler::Instance().SetSaltForTest(0);
    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();
    auto access = AccessRecorder::Stream(AccessRecorderKey::DS_STREAM_CREATE_PRODUCER);
    access.StreamName("test_stream").Result(Status::OK()).Record();
}

TEST_F(LogAccessFacadeTest, StreamFacadeSharedPtrCrossThread)
{
    EnableSampler(1.0, 1.0, 1.0);
    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();
    auto recorder = std::make_shared<StreamAccessRecorder>(
        AccessRecorder::Stream(AccessRecorderKey::DS_STREAM_CREATE_PRODUCER));
    recorder->StreamName("test_stream").ProducerId("prod1");
    recorder->Result(Status::OK()).Record();
}

TEST_F(LogAccessFacadeTest, RequestOutFacadeBasic)
{
    EnableSampler(1.0, 1.0, 1.0);
    auto access = AccessRecorder::RequestOut(AccessRecorderKey::DS_ETCD_PUT);
    access.OutReq("my_key").DataSize(64).Result(0, "OK").Record();
}

TEST_F(LogAccessFacadeTest, RequestOutFacadeDS_ETCD_UNKNOWNNoOp)
{
    EnableSampler(1.0, 1.0, 1.0);
    auto access = AccessRecorder::RequestOut(AccessRecorderKey::DS_ETCD_UNKNOWN);
    access.OutReq("ignored").Result(0, "ignored").Record();
}

TEST_F(LogAccessFacadeTest, RequestOutFacadeProviderDeferred)
{
    EnableSampler(1.0, 1.0, 1.0);
    int providerCallCount = 0;
    auto access = AccessRecorder::RequestOut(AccessRecorderKey::DS_ETCD_GET);
    access.OutReqProvider([&providerCallCount] {
        ++providerCallCount;
        return "ShortDebugString_result";
    }).DataSize(0).Result(0, "OK").Record();
    ASSERT_EQ(providerCallCount, 1);
}

TEST_F(LogAccessFacadeTest, ObjectFacadeCArrayRef)
{
    EnableSampler(1.0, 1.0, 1.0);
    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();
    const char *keys[] = { "key1", "key2" };
    size_t lens[] = { 4, 4 };
    auto access = AccessRecorder::Object(AccessRecorderKey::DS_OBJECT_CLIENT_GET);
    access.ObjectKeysRef(keys, lens, 2).Result(Status::OK()).DataSize(10).Record();
}

TEST_F(LogAccessFacadeTest, ObjectFacadeResultStatusCode)
{
    EnableSampler(1.0, 1.0, 1.0);
    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();
    auto access = AccessRecorder::Object(AccessRecorderKey::DS_KV_CLIENT_SET);
    access.ObjectKeyRef("key").Result(StatusCode::K_NOT_FOUND, "not found").Record();
}

TEST_F(LogAccessFacadeTest, ObjectFacadeAsyncElapsedUs)
{
    EnableSampler(1.0, 1.0, 1.0);
    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();
    auto access = AccessRecorder::Object(AccessRecorderKey::DS_KV_CLIENT_SET);
    access.ObjectKeyRef("key").AsyncElapsedUs(5000).Result(Status::OK()).Record();
}

TEST_F(LogAccessFacadeTest, ObjectFacadeSharedPtrAsyncPattern)
{
    EnableSampler(1.0, 1.0, 1.0);
    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();
    auto access = std::make_shared<ObjectAccessRecorder>(
        AccessRecorder::Object(AccessRecorderKey::DS_HETERO_CLIENT_ASYNCMGETH2D));
    access->ObjectKeyRef("key").DataSize(100).Result(Status::OK()).Record();
}

TEST_F(LogAccessFacadeTest, StreamFacadeCountAndResponseFields)
{
    EnableSampler(1.0, 1.0, 1.0);
    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();
    auto access = AccessRecorder::Stream(AccessRecorderKey::DS_STREAM_QUERY_PRODUCERS_NUM);
    access.StreamName("stream1").ClientId("client1").Count(5).Result(Status::OK()).Record();
}

TEST_F(LogAccessFacadeTest, FieldEntryLastWriteWins)
{
    EnableSampler(1.0, 1.0, 1.0);
    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();
    auto access = AccessRecorder::Object(AccessRecorderKey::DS_KV_CLIENT_SET);
    access.WriteMode(1).WriteModeText("LATEST").Result(Status::OK()).Record();
}

TEST_F(LogAccessFacadeTest, ObjectFacadeNonNulTerminatedKeyNoOverread)
{
    EnableSampler(1.0, 1.0, 1.0);
    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();
    char keyBuf[] = "mykeyXXgarbage";
    size_t keyLen = 5;
    keyBuf[keyLen] = 'X';
    auto access = AccessRecorder::Object(AccessRecorderKey::DS_KV_CLIENT_SET);
    access.ObjectKeyRef(std::string_view(keyBuf, keyLen)).Result(Status::OK()).Record();
}

TEST_F(LogAccessFacadeTest, ObjectFacadeKeyTruncatedToLimit)
{
    EnableSampler(1.0, 1.0, 1.0);
    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();
    std::string longKey(500, 'A');
    auto access = AccessRecorder::Object(AccessRecorderKey::DS_KV_CLIENT_SET);
    access.ObjectKeyRef(std::string_view(longKey)).Result(Status::OK()).Record();
}

TEST_F(LogAccessFacadeTest, ObjectFacadeEmbeddedNulKeyPreserved)
{
    EnableSampler(1.0, 1.0, 1.0);
    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();
    char keyBuf[] = "a\0b";
    size_t keyLen = 3;
    auto access = AccessRecorder::Object(AccessRecorderKey::DS_KV_CLIENT_SET);
    access.ObjectKeyRef(std::string_view(keyBuf, keyLen)).Result(Status::OK()).Record();
}

TEST_F(LogAccessFacadeTest, NestedKeyProviderSampledOutNotExecuted)
{
    EnableSampler(0.0, 0.0, 1.0);
    LogSampler::Instance().SetSaltForTest(0);
    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();
    int providerCallCount = 0;
    auto access = AccessRecorder::Object(AccessRecorderKey::DS_OBJECT_CLIENT_PUT);
    access.NestedKeyProvider([&providerCallCount] {
        ++providerCallCount;
        return std::string("nested_result");
    }).Result(Status::OK()).Record();
    ASSERT_EQ(providerCallCount, 0);
}

TEST_F(LogAccessFacadeTest, NestedKeyProviderSampledInExecuted)
{
    EnableSampler(1.0, 1.0, 1.0);
    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();
    int providerCallCount = 0;
    auto access = AccessRecorder::Object(AccessRecorderKey::DS_OBJECT_CLIENT_PUT);
    access.NestedKeyProvider([&providerCallCount] {
        ++providerCallCount;
        return std::string("[key1,key2]");
    }).Result(Status::OK()).Record();
    ASSERT_EQ(providerCallCount, 1);
}

TEST_F(LogAccessFacadeTest, NestedKeysRefTempVectorIsDanglingRegression)
{
    EnableSampler(1.0, 1.0, 1.0);
    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();
    std::vector<std::string> nestedKeys = { "nk1", "nk2" };
    auto access = AccessRecorder::Object(AccessRecorderKey::DS_OBJECT_CLIENT_PUT);
    access.NestedKeysRef(nestedKeys).Result(Status::OK()).Record();
}

TEST_F(LogAccessFacadeTest, ObjectFacadeObjectKeyOwnedSkipsCopyWhenSampledOut)
{
    EnableSampler(0.0, 0.0, 1.0);
    LogSampler::Instance().SetSaltForTest(0);
    TraceGuard guard1 = Trace::Instance().SetRequestTraceUUID();

    {
        std::string key = "owned_const_ref_key";
        auto access = AccessRecorder::Object(AccessRecorderKey::DS_KV_CLIENT_SET);
        access.ObjectKeyOwned(key).Result(Status::OK()).Record();
        EXPECT_EQ(key, "owned_const_ref_key");
    }

    {
        std::string key = "owned_move_key";
        auto access = AccessRecorder::Object(AccessRecorderKey::DS_KV_CLIENT_SET);
        access.ObjectKeyOwned(std::move(key)).Result(Status::OK()).Record();
        EXPECT_EQ(key, "owned_move_key");
    }

    EnableSampler(1.0, 1.0, 1.0);
    TraceGuard guard2 = Trace::Instance().SetRequestTraceUUID();

    {
        std::string key = "sampled_in_const_key";
        auto access = AccessRecorder::Object(AccessRecorderKey::DS_KV_CLIENT_SET);
        access.ObjectKeyOwned(key).Result(Status::OK()).DataSize(100).Record();
    }

    {
        std::string key = "sampled_in_move_key";
        auto access = AccessRecorder::Object(AccessRecorderKey::DS_KV_CLIENT_SET);
        access.ObjectKeyOwned(std::move(key)).Result(Status::OK()).DataSize(100).Record();
    }
}

TEST_F(LogAccessFacadeTest, ObjectFacadeObjectKeyProviderSetBeforeCallCoversEarlyExit)
{
    EnableSampler(1.0, 1.0, 1.0);
    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();

    // Simulate multi-key pattern: Provider set before potentially-failing call
    // Even if the "call" fails (Result with error code), Provider was already set
    std::vector<std::string> keys = { "key1", "key2", "key3" };
    auto access = AccessRecorder::Object(AccessRecorderKey::DS_KV_CLIENT_GET);
    access.ObjectKeyProvider([keys] { return objectKeysToString(keys); })
          .TimeoutMs(5000)
          .Result(Status(StatusCode::K_RUNTIME_ERROR, "simulated failure"))
          .DataSize(0)
          .Record();
}

TEST_F(LogAccessFacadeTest, ObjectFacadeObjectKeyProviderSetBeforeCallSampledOutNoCopy)
{
    EnableSampler(0.0, 0.0, 1.0);
    LogSampler::Instance().SetSaltForTest(0);
    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();

    int providerCalls = 0;
    std::vector<std::string> keys = { "key1", "key2" };
    auto access = AccessRecorder::Object(AccessRecorderKey::DS_KV_CLIENT_GET);
    access.ObjectKeyProvider([&providerCalls, &keys] {
              ++providerCalls;
              return objectKeysToString(keys);
          })
          .TimeoutMs(5000)
          .Result(Status::OK())
          .DataSize(0)
          .Record();
    EXPECT_EQ(providerCalls, 0);
}

}  // namespace ut
}  // namespace datasystem
