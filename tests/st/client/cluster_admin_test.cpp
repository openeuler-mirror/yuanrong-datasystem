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
 * Description: Integration tests for cluster_admin_client (dscli delete cluster).
 */

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "common.h"
#include "client/object_cache/oc_client_common.h"
#include "datasystem/client/cluster_admin/cluster_admin_client.h"
#include "datasystem/client/cluster_query/cluster_query_client.h"
#include "datasystem/common/util/status_helper.h"

DS_DECLARE_string(cluster_name);
DS_DECLARE_string(etcd_address);

namespace datasystem {
namespace st {

class ClusterAdminDeleteTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 2;
        opts.numWorkers = 2;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams = "-v=1 -node_timeout_s=15";
    }

    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        DS_ASSERT_OK(cluster_->StartWorkers());
        for (size_t i = 0; i < 2; i++) {
            DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, i));
        }
        FLAGS_etcd_address = cluster_->GetEtcdAddrs();
    }

    void TearDown() override
    {
        ClusterTest::TearDown();
    }

};

TEST_F(ClusterAdminDeleteTest, DeleteRemovesPerAddressKeysAndTopologyMember)
{
    HostPort worker0;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, worker0));
    auto *externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
    ASSERT_NE(externalCluster, nullptr);
    DS_ASSERT_OK(externalCluster->ShutdownNode(WORKER, 0));
    sleep(20);

    client::cluster_admin::ClusterAdminOptions options;
    options.clusterName = GetTestClusterName();
    options.etcdAddress = cluster_->GetEtcdAddrs();
    options.force = true;
    client::cluster_admin::ClusterAdminClient adminClient(std::move(options));
    DS_ASSERT_OK(adminClient.Init());

    std::vector<std::string> addresses = { worker0.ToString() };
    std::vector<client::cluster_admin::DeleteClusterMemberResult> results;
    DS_ASSERT_OK(adminClient.DeleteClusterMembers(addresses, results));
    ASSERT_EQ(results.size(), 1);
    EXPECT_EQ(results[0].address, worker0.ToString());
    EXPECT_TRUE(results[0].error.empty());
}

TEST_F(ClusterAdminDeleteTest, DeleteNonExistentKeyIsNonFatal)
{
    HostPort worker0;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, worker0));
    auto *externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
    ASSERT_NE(externalCluster, nullptr);
    DS_ASSERT_OK(externalCluster->ShutdownNode(WORKER, 0));
    sleep(20);

    client::cluster_admin::ClusterAdminOptions options;
    options.clusterName = GetTestClusterName();
    options.etcdAddress = cluster_->GetEtcdAddrs();
    options.force = true;
    client::cluster_admin::ClusterAdminClient adminClient(std::move(options));
    DS_ASSERT_OK(adminClient.Init());

    std::vector<std::string> addresses = { worker0.ToString() };
    std::vector<client::cluster_admin::DeleteClusterMemberResult> results;
    DS_ASSERT_OK(adminClient.DeleteClusterMembers(addresses, results));
    ASSERT_EQ(results.size(), 1);
    DS_ASSERT_OK(adminClient.DeleteClusterMembers(addresses, results));
    ASSERT_EQ(results.size(), 1);
    EXPECT_FALSE(results[0].membershipDeleted);
    EXPECT_TRUE(results[0].error.empty());
}

TEST_F(ClusterAdminDeleteTest, OnlineWorkerRejectedWithoutForce)
{
    HostPort worker0;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, worker0));

    client::cluster_admin::ClusterAdminOptions options;
    options.clusterName = GetTestClusterName();
    options.etcdAddress = cluster_->GetEtcdAddrs();
    client::cluster_admin::ClusterAdminClient adminClient(std::move(options));
    DS_ASSERT_OK(adminClient.Init());

    std::vector<std::string> addresses = { worker0.ToString() };
    std::vector<client::cluster_admin::DeleteClusterMemberResult> results;
    auto rc = adminClient.DeleteClusterMembers(addresses, results);
    ASSERT_EQ(results.size(), 1);
    EXPECT_FALSE(results[0].membershipDeleted);
    EXPECT_FALSE(results[0].topologyMemberRemoved);
    EXPECT_FALSE(results[0].error.empty());
    EXPECT_NE(results[0].error.find("online"), std::string::npos);
}

}  // namespace st
}  // namespace datasystem
