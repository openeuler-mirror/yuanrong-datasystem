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

#include <gtest/gtest.h>
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdint>
#include <initializer_list>
#include <iterator>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common.h"
#include "client/object_cache/oc_client_common.h"
#include "client/kv_cache/kv_client_scale_common.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/kv_client.h"
#include "datasystem/utils/status.h"

DS_DECLARE_string(etcd_address);
DS_DECLARE_string(log_dir);

namespace datasystem {
namespace st {

constexpr size_t DEFAULT_WORKER_NUM = 8;
const size_t MASTER_NUM = 2;
const std::string AZ1 = "AZ1";
const std::string AZ2 = "AZ2";

class KVClientCrossAZDelTest : virtual public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.waitWorkerReady = false;
        // worker 0, 2 use the same master address, which is worker 0 address.
        // worker 1, 3 use the same master address, which is worker 1 address.
        opts.crossAZMap = { { 0, 0 }, { 1, 1 }, { 2, 0 }, { 3, 1 } };
        opts.numEtcd = 1;
        opts.numWorkers = workerNum_;
        opts.numMasters = MASTER_NUM;
        opts.enableDistributedMaster = "true";
        opts.numOBS = 1;
        std::string OBSGflag =
            FormatString(
                "-other_cluster_names=AZ1,AZ2,AZ3 "
                "-v=2 "
                "-cross_cluster_get_meta_from_worker=%s -inject_actions=TryGetObjectFromRemote.NoRetry:call() ",
                crossAzGetMetaFromWorker_)
            + appendCmd_;

        opts.workerGflagParams = OBSGflag;
        for (size_t i = 0; i < workerNum_; i++) {
            std::string param = "-cluster_name=";
            if (i % MASTER_NUM == 0) {
                param.append(AZ1);
            } else {
                param.append(AZ2);
            }
            opts.workerSpecifyGflagParams[i] += param;
        }
    }

    void InitTestEtcdInstance()
    {
        std::string etcdAddress;
        for (size_t i = 0; i < cluster_->GetEtcdNum(); ++i) {
            std::pair<HostPort, HostPort> addrs;
            cluster_->GetEtcdAddrs(i, addrs);
            if (!etcdAddress.empty()) {
                etcdAddress += ",";
            }
            etcdAddress += addrs.first.ToString();
        }
        FLAGS_etcd_address = etcdAddress;
        LOG(INFO) << "The etcd address is:" << FLAGS_etcd_address << std::endl;
        etcdStore_ = std::make_unique<EtcdStore>(etcdAddress);
        if ((etcdStore_ != nullptr) && (etcdStore_->Init().IsOk())) {
            std::string az1RingStr = "/" + AZ1 + std::string(ETCD_RING_PREFIX);
            std::string az2RingStr = "/" + AZ2 + std::string(ETCD_RING_PREFIX);
            etcdStore_->DropTable(az1RingStr);
            etcdStore_->DropTable(az2RingStr);
            (void)etcdStore_->CreateTable(az1RingStr, az1RingStr);
            (void)etcdStore_->CreateTable(az2RingStr, az2RingStr);
        }
    }

    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        DS_ASSERT_OK(cluster_->StartOBS());
        externalCluster_ = dynamic_cast<ExternalCluster *>(cluster_.get());
        InitTestEtcdInstance();
        DS_ASSERT_OK(cluster_->StartWorkers());
        for (size_t i = 0; i < workerNum_; i++) {
            DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, i));
        }
    }

    void TearDown() override
    {
        etcdStore_.reset();
        ExternalClusterTest::TearDown();
    }

protected:
    ExternalCluster *externalCluster_ = nullptr;
    std::unique_ptr<EtcdStore> etcdStore_;
    const int32_t timeoutMs_ = 10'000;
    std::string crossAzGetMetaFromWorker_ = "true";
    std::string appendCmd_ = "";
    size_t workerNum_ = DEFAULT_WORKER_NUM;
};

TEST_F(KVClientCrossAZDelTest, Del)
{
    std::shared_ptr<KVClient> az1c1, az1c2, az2c1;
    InitTestKVClient(0, az1c1, timeoutMs_);
    InitTestKVClient(1, az2c1, timeoutMs_);
    InitTestKVClient(1, az1c2, timeoutMs_);
    auto num = 200;
    auto val = "c";
    std::vector<std::string> keys, failedKeys;
    for (auto i = 0; i < num; i++) {
        auto key = FormatString("az1_client1_%s", i);
        DS_ASSERT_OK(az1c1->Set(key, val));
        keys.emplace_back(key);
    }
    for (auto i = 0; i < num; i++) {
        auto key = FormatString("az1_client2_%s", i);
        DS_ASSERT_OK(az1c2->Set(key, val));
        keys.emplace_back(key);
    }
    for (auto i = 0; i < num; i++) {
        auto key = FormatString("az2_client1_%s", i);
        DS_ASSERT_OK(az2c1->Set(key, val));
        keys.emplace_back(key);
    }
    for (auto key : keys) {
        std::string val;
        DS_ASSERT_OK(az1c1->Get(key, val));
    }
    DS_ASSERT_OK(az1c1->Del(keys, failedKeys));
}

}  // namespace st
}  // namespace datasystem
