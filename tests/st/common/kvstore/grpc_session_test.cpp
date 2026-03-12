/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: Test grpc session.
 */

#include <string>
#include "common.h"
#include "datasystem/common/kvstore/etcd/grpc_session.h"
#include "etcd/api/etcdserverpb/rpc.grpc.pb.h"

namespace datasystem {
namespace st {
class GrpcSessionTest : public ExternalClusterTest {
public:
    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        std::pair<HostPort, HostPort> addrs;
        DS_ASSERT_OK(cluster_->GetEtcdAddrs(etcdNum_ - 1, addrs));
        etcdAddr_ = addrs.first.ToString();
    }

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = etcdNum_;
        opts.numMasters = 0;
        opts.numWorkers = 0;
    }

protected:
    std::string etcdAddr_;
    const size_t etcdNum_ = 1;
};

TEST_F(GrpcSessionTest, DISABLED_TestCreateSessionAfterEtcdCrash)
{
    auto externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
    DS_ASSERT_OK(externalCluster->ShutdownEtcds());

    std::unique_ptr<GrpcSession<etcdserverpb::KV>> rpcSession;
    DS_ASSERT_OK(GrpcSession<etcdserverpb::KV>::CreateSession(etcdAddr_, rpcSession));

    std::string key = "key";
    std::string val = "val";
    int timeoutMs = 1'000;
    etcdserverpb::PutRequest req;
    req.set_key(key);
    req.set_value(val);
    etcdserverpb::PutResponse rsp;
    DS_ASSERT_NOT_OK(
        rpcSession->SendRpc("Put::etcd_kv_Put", req, rsp, &etcdserverpb::KV::Stub::Put, "", val.size(), timeoutMs));
    DS_ASSERT_OK(externalCluster->StartEtcdCluster());
    DS_ASSERT_OK(
        rpcSession->SendRpc("Put::etcd_kv_Put", req, rsp, &etcdserverpb::KV::Stub::Put, "", val.size(), timeoutMs));
}

TEST_F(GrpcSessionTest, TestPutClusterTableWithoutLeaseId)
{
    std::unique_ptr<GrpcSession<etcdserverpb::KV>> kvSession;
    DS_ASSERT_OK(GrpcSession<etcdserverpb::KV>::CreateSession(etcdAddr_, kvSession));

    std::string val = "val";
    int timeoutMs = 1'000;
    etcdserverpb::PutRequest putReq;
    putReq.set_key(ETCD_CLUSTER_TABLE);
    putReq.set_value(val);
    etcdserverpb::PutResponse putRsp;
    DS_ASSERT_NOT_OK(kvSession->SendRpc("Put::etcd_kv_Put", putReq, putRsp, &etcdserverpb::KV::Stub::Put, "",
                                        val.size(), timeoutMs));

    std::unique_ptr<GrpcSession<etcdserverpb::Lease>> leaseSession;
    DS_ASSERT_OK(GrpcSession<etcdserverpb::Lease>::CreateSession(etcdAddr_, leaseSession));
    etcdserverpb::LeaseGrantRequest leaseReq;
    etcdserverpb::LeaseGrantResponse leaseRsp;
    int ttlInSec = 60;
    leaseReq.set_ttl(ttlInSec);
    leaseReq.set_id(0);
    DS_ASSERT_OK(leaseSession->AsyncSendRpc("LeaseGrant", leaseReq, leaseRsp,
                                            &etcdserverpb::Lease::Stub::AsyncLeaseGrant, "", timeoutMs));
    auto leaseId = leaseRsp.id();

    putReq.set_lease(leaseId);
    DS_ASSERT_OK(kvSession->SendRpc("Put::etcd_kv_Put", putReq, putRsp, &etcdserverpb::KV::Stub::Put, "", val.size(),
                                    timeoutMs));

    auto invalidLeaseId = 1234;
    putReq.set_lease(invalidLeaseId);
    DS_ASSERT_NOT_OK(kvSession->SendRpc("Put::etcd_kv_Put", putReq, putRsp, &etcdserverpb::KV::Stub::Put, "",
                                        val.size(), timeoutMs));
}
}  // namespace st
}  // namespace datasystem
