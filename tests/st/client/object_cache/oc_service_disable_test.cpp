/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: Test oc service disable.
 */
#include "common.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/object_client.h"
#include "datasystem/kv_client.h"
#include "datasystem/stream_client.h"

#include <chrono>
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/utils/connection.h"
#include "datasystem/utils/status.h"
#include "oc_client_common.h"

namespace datasystem {
namespace st {
namespace {
constexpr int STREAM_SERVICE_READY_TIMEOUT_MS = 5'000;
constexpr int STREAM_SERVICE_READY_RETRY_INTERVAL_MS = 100;
}  // namespace

void OcOp(const std::shared_ptr<ObjectClient> &client, bool success)
{
    std::string objKey = "ikun";
    std::string data = RandomData().GetRandomString(1024 * 1024ul);
    if (success) {
        DS_ASSERT_OK(client->Put(objKey, (uint8_t *)data.data(), data.size(), CreateParam{}));
    } else {
        ASSERT_EQ(client->Put(objKey, (uint8_t *)data.data(), data.size(), CreateParam{}).GetCode(),
                  StatusCode::K_RUNTIME_ERROR);
    }
}

void ScOp(const std::shared_ptr<StreamClient> &client, bool success)
{
    std::shared_ptr<Consumer> consumer;
    SubscriptionConfig config("sub1", SubscriptionType::STREAM);
    if (success) {
        DS_ASSERT_OK(client->Subscribe("test1", config, consumer));
    } else {
        ASSERT_EQ(client->Subscribe("test1", config, consumer).GetCode(), StatusCode::K_RUNTIME_ERROR);
    }
}

Status SubscribeWhenReady(const std::shared_ptr<StreamClient> &client)
{
    Timer timer;
    Status lastStatus;
    do {
        std::shared_ptr<Consumer> consumer;
        SubscriptionConfig config("sub1", SubscriptionType::STREAM);
        lastStatus = client->Subscribe("test1", config, consumer);
        if (lastStatus.IsOk()) {
            return lastStatus;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(STREAM_SERVICE_READY_RETRY_INTERVAL_MS));
    } while (timer.ElapsedMilliSecond() < STREAM_SERVICE_READY_TIMEOUT_MS);
    return lastStatus;
}

void KvOp(const std::shared_ptr<KVClient> &client, bool success)
{
    std::string key = "ikun_again";
    std::string val = RandomData().GetRandomString(1024 * 1024ul);
    if (success) {
        DS_ASSERT_OK(client->Set(key, val));
    } else {
        ASSERT_EQ(client->Set(key, val).GetCode(), StatusCode::K_RUNTIME_ERROR);
    }
}

class OcServiceDisableTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 1;
        opts.numRpcThreads = 0;
        opts.numEtcd = 1;
        opts.workerGflagParams = "-sc_stream_socket_num=1 -sc_regular_socket_num=1";
    }
};

TEST_F(OcServiceDisableTest, TestInit)
{
    LOG(INFO) << "Test oc client init when oc service disable.";
    ConnectOptions opts;
    InitConnectOpt(0, opts);
    auto ocClient = std::make_shared<ObjectClient>(opts);
    DS_ASSERT_OK(ocClient->Init());
    OcOp(ocClient, false);
    auto kVClient = std::make_shared<KVClient>(opts);
    DS_ASSERT_OK(kVClient->Init());
    KvOp(kVClient, false);
    auto scClient = std::make_shared<StreamClient>(opts);
    DS_ASSERT_OK(scClient->Init());
    DS_ASSERT_OK(SubscribeWhenReady(scClient));
}

class ScServiceDisableTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 1;
        opts.numEtcd = 1;
    }
};

TEST_F(ScServiceDisableTest, TestInit)
{
    LOG(INFO) << "Test sc client init when sc service disable.";
    ConnectOptions opts;
    InitConnectOpt(0, opts);
    auto ocClient = std::make_shared<ObjectClient>(opts);
    DS_ASSERT_OK(ocClient->Init());
    OcOp(ocClient, true);
    auto kVClient = std::make_shared<KVClient>(opts);
    DS_ASSERT_OK(kVClient->Init());
    KvOp(kVClient, true);
    auto scClient = std::make_shared<StreamClient>(opts);
    DS_ASSERT_OK(scClient->Init());
    ScOp(scClient, false);
}

class CommonServiceTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 1;
        opts.numEtcd = 1;
        opts.workerGflagParams = "-sc_stream_socket_num=1 -sc_regular_socket_num=1";
    }
};

TEST_F(CommonServiceTest, TestInit)
{
    LOG(INFO) << "Test sc client init when sc service disable.";
    ConnectOptions opts;
    InitConnectOpt(0, opts);
    auto ocClient = std::make_shared<ObjectClient>(opts);
    DS_ASSERT_OK(ocClient->Init());
    OcOp(ocClient, true);
    auto kVClient = std::make_shared<KVClient>(opts);
    DS_ASSERT_OK(kVClient->Init());
    KvOp(kVClient, true);
    auto scClient = std::make_shared<StreamClient>(opts);
    DS_ASSERT_OK(scClient->Init());
    ScOp(scClient, true);
}

class CommonServiceDisableTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 1;
        opts.numRpcThreads = 0;
        opts.numEtcd = 1;
        opts.waitWorkerReady = false;
    }
};

TEST_F(CommonServiceDisableTest, TestInit)
{
    LOG(INFO) << "Test sc client init when sc service disable.";
    ConnectOptions opts;
    InitConnectOpt(0, opts);
    auto ocClient = std::make_shared<ObjectClient>(opts);
    DS_ASSERT_OK(ocClient->Init());
    OcOp(ocClient, false);
    auto kVClient = std::make_shared<KVClient>(opts);
    DS_ASSERT_OK(kVClient->Init());
    KvOp(kVClient, false);
    auto scClient = std::make_shared<StreamClient>(opts);
    DS_ASSERT_OK(scClient->Init());
    ScOp(scClient, false);
}
}  // namespace st
}  // namespace datasystem
