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
 * Description: Test KVClient with WRITE_BACK_L2_CACHE_EVICT mode.
 */

#include "datasystem/kv_client.h"

#include <vector>

#include <gtest/gtest.h>

#include "client/object_cache/oc_client_common.h"
#include "common.h"
#include "datasystem/common/log/log.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/utils/connection.h"
#include "datasystem/utils/status.h"
#include "datasystem/utils/string_view.h"

namespace datasystem {
namespace st {

class KVClientWriteBackL2CacheEvictTest : public OCClientCommon {
public:
  void SetClusterSetupOptions(ExternalClusterOptions &opts) override {
    opts.enableSpill = false;
    opts.workerGflagParams = "-shared_memory_size_mb=12 -log_monitor=true -v=1";
    opts.numEtcd = 1;
    opts.numWorkers = 1;
    opts.numOBS = 1;
    opts.enableDistributedMaster = "false";
    for (size_t i = 0; i < opts.numWorkers; i++) {
      std::string dir =
          GetTestCaseDataDir() + "/worker" + std::to_string(i) + "/shared_disk";
      opts.workerSpecifyGflagParams[i] = FormatString(
          "-shared_disk_directory=%s -shared_disk_size_mb=12", dir);
    }
  }

  void SetUp() override {
    ExternalClusterTest::SetUp();
    InitTestKVClientWithTenant(0, client_);
  }

  void InitConnectOptW(uint32_t workerIndex, ConnectOptions &connectOptions,
                       int32_t timeoutMs = 60000) {
    HostPort workerAddress;
    ASSERT_TRUE(workerIndex < cluster_->GetWorkerNum());
    DS_ASSERT_OK(cluster_->GetWorkerAddr(workerIndex, workerAddress));
    connectOptions = {.host = workerAddress.Host(),
                      .port = workerAddress.Port(),
                      .connectTimeoutMs = timeoutMs};
    connectOptions.accessKey = "QTWAOYTTINDUT2QVKYUC";
    connectOptions.secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
  }

  void InitTestKVClientWithTenant(uint32_t workerIndex,
                                  std::shared_ptr<KVClient> &client) {
    ConnectOptions connectOptions;
    InitConnectOptW(workerIndex, connectOptions);
    client = std::make_shared<KVClient>(connectOptions);
    DS_ASSERT_OK(client->Init());
  }

protected:
  std::shared_ptr<KVClient> client_;
};

TEST_F(KVClientWriteBackL2CacheEvictTest,
       TestSetWithWriteBackL2CacheEvictMode) {
  LOG(INFO) << "Test Set with WRITE_BACK_L2_CACHE_EVICT mode";
  std::string key = "test_set_write_back_l2_cache_evict";
  std::string value = "test_value";

  SetParam param;
  param.writeMode = WriteMode::WRITE_BACK_L2_CACHE_EVICT;

  Status status = client_->Set(key, StringView(value), param);
  DS_ASSERT_OK(status);

  std::string result;
  status = client_->Get(key, result);
  DS_ASSERT_OK(status);
  ASSERT_EQ(result, value);

  status = client_->Del(key);
  DS_ASSERT_OK(status);
}

TEST_F(KVClientWriteBackL2CacheEvictTest,
       TestMSetWithWriteBackL2CacheEvictMode) {
  LOG(INFO) << "Test MSet with WRITE_BACK_L2_CACHE_EVICT mode";
  std::vector<std::string> keys = {"test_mset_1", "test_mset_2"};
  std::vector<StringView> values = {StringView("value1"), StringView("value2")};
  std::vector<std::string> failedKeys;

  MSetParam param;
  param.writeMode = WriteMode::WRITE_BACK_L2_CACHE_EVICT;
  param.existence = ExistenceOpt::NONE;

  Status status = client_->MSet(keys, values, failedKeys, param);
  DS_ASSERT_OK(status);
  ASSERT_TRUE(failedKeys.empty());

  for (size_t i = 0; i < keys.size(); ++i) {
    std::string result;
    status = client_->Get(keys[i], result);
    DS_ASSERT_OK(status);
    ASSERT_EQ(result, std::string(values[i].data(), values[i].size()));
  }

  status = client_->Del(keys, failedKeys);
  DS_ASSERT_OK(status);
  ASSERT_TRUE(failedKeys.empty());
}

TEST_F(KVClientWriteBackL2CacheEvictTest,
       TestCreateWithWriteBackL2CacheEvictMode) {
  LOG(INFO) << "Test Create with WRITE_BACK_L2_CACHE_EVICT mode";
  std::string key = "test_create_write_back_l2_cache_evict";
  uint64_t size = 1024;

  SetParam param;
  param.writeMode = WriteMode::WRITE_BACK_L2_CACHE_EVICT;

  std::shared_ptr<Buffer> buffer;
  Status status = client_->Create(key, size, param, buffer);
  DS_ASSERT_OK(status);
  ASSERT_NE(buffer, nullptr);

  status = client_->Set(buffer);
  DS_ASSERT_OK(status);

  std::string result;
  status = client_->Get(key, result);
  DS_ASSERT_OK(status);

  status = client_->Del(key);
  DS_ASSERT_OK(status);
}

TEST_F(KVClientWriteBackL2CacheEvictTest,
       TestMCreateWithWriteBackL2CacheEvictMode) {
  LOG(INFO) << "Test MCreate with WRITE_BACK_L2_CACHE_EVICT mode";
  std::vector<std::string> keys = {"test_mcreate_1", "test_mcreate_2"};
  std::vector<uint64_t> sizes = {512, 1024};

  SetParam param;
  param.writeMode = WriteMode::WRITE_BACK_L2_CACHE_EVICT;

  std::vector<std::shared_ptr<Buffer>> buffers;
  Status status = client_->MCreate(keys, sizes, param, buffers);
  DS_ASSERT_OK(status);
  ASSERT_EQ(buffers.size(), keys.size());

  status = client_->MSet(buffers);
  DS_ASSERT_OK(status);

  for (const auto &key : keys) {
    std::string result;
    status = client_->Get(key, result);
    DS_ASSERT_OK(status);
  }

  std::vector<std::string> failedKeys;
  status = client_->Del(keys, failedKeys);
  DS_ASSERT_OK(status);
  ASSERT_TRUE(failedKeys.empty());
}

TEST_F(KVClientWriteBackL2CacheEvictTest, TestEvictBeforeWriteToL2Cache)
{
    LOG(INFO) << "Test Evict and Delete before write to L2 cache";
    const size_t kDataSize = 4 * 1024 * 1024;
    const size_t kObjectCount = 3;
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.async_send.before_send", "pause"));
    SetParam param;
    param.writeMode = WriteMode::WRITE_BACK_L2_CACHE_EVICT;
    std::vector<std::string> keys;
    for (size_t i = 0; i < kObjectCount; ++i) {
        std::string key = "test_evict_del_" + std::to_string(i);
        std::string value(kDataSize, 'A' + i);
        Status status = client_->Set(key, StringView(value), param);
        DS_ASSERT_OK(status);
        keys.push_back(key);
    }
    cluster_->ClearInjectAction(WORKER, 0, "worker.async_send.before_send");
    std::string val;
    ASSERT_EQ(client_->Get(keys[0], val).GetCode(), K_NOT_FOUND);
}

} // namespace st
} // namespace datasystem
