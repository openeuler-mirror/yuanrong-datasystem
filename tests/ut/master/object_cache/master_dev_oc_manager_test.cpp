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
 * Description: Test master device object manager class.
 */
#include <string>

#include "common.h"
#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/master/object_cache/device/master_dev_oc_manager.h"
#include "datasystem/master/object_cache/device/master_dev_oc_directory.h"
#include "datasystem/protos/p2p_subscribe.pb.h"
#include "gtest/gtest.h"

using namespace datasystem::master;
namespace datasystem {
namespace ut {

constexpr int ABNORMAL_EXIT_CODE = -2;
class MasterDevOcManagerTest : public CommonTest {
public:
    RandomData randomData_;
};

int WaitForChildFork(pid_t pid)
{
    if (pid == 0) {
        return 0;
    }
    int statLoc;
    if (waitpid(pid, &statLoc, 0) < 0) {
        LOG(ERROR) << FormatString("waitpid: %d error!", pid);
        return -1;
    }
    if (WIFEXITED(statLoc)) {
        const int exitStatus = WEXITSTATUS(statLoc);
        if (exitStatus != 0) {
            LOG(ERROR) << FormatString("Non-zero exit status %d from test!", exitStatus);
        }
        return exitStatus;
    } else {
        LOG(ERROR) << FormatString("Non-normal exit %d from child!, status: %d", pid, statLoc);
        return ABNORMAL_EXIT_CODE;
    }
}
TEST_F(MasterDevOcManagerTest, DISABLED_TestInvalidParameterInPut)
{
    MasterDevOcManager manager{};
    manager.Init();
    PutP2PMetaReqPb req;
    auto subReq = req.add_dev_obj_meta();
    subReq->set_object_key(GetStringUuid());
    auto clientId = GetStringUuid();
    auto deviceId = randomData_.GetRandomUint32();
    auto loc = subReq->add_locations();
    loc->set_client_id(clientId);
    loc->set_device_id(deviceId);
    PutP2PMetaRspPb resp;
    DS_ASSERT_OK(manager.PutP2PMetaImpl(req, resp));
    DS_ASSERT_NOT_OK(manager.PutP2PMetaImpl(req, resp));
}

void AddGraph(MasterDevOcManager &manager, const std::string srcClientId, const std::string dstClientId,
              const int srcDeviceId, const int dstDeveiceId)
{
    SendRootInfoReqPb sendrootreq;
    SendRootInfoRspPb sendrootresp;
    sendrootreq.set_src_client_id(srcClientId);
    sendrootreq.set_dst_client_id(dstClientId);
    sendrootreq.set_src_device_id(srcDeviceId);
    sendrootreq.set_dst_device_id(dstDeveiceId);
    (void)manager.SendRootInfoImpl(sendrootreq, sendrootresp);
}

void AddLocs(MasterDevOcManager &manager, const std::string objectKey, const std::string srcClientId,
             const std::string dstClientId, const int srcDeviceId, const int dstDeveiceId)
{
    AckRecvFinishReqPb req;
    req.set_object_key(objectKey);
    req.set_src_client_id(srcClientId);
    req.set_dst_client_id(dstClientId);
    req.set_src_device_id(srcDeviceId);
    req.set_dst_device_id(dstDeveiceId);
    req.set_cache_location(true);
    AckRecvFinishRspPb resp;
    (void)manager.AckRecvFinish(req, resp);
}

void RegObjectKey(MasterDevOcManager &manager, const std::string objectKey, const std::string clientId,
                 const int deviceId)
{
    PutP2PMetaReqPb req;
    PutP2PMetaRspPb resp;
    auto objMeta = req.add_dev_obj_meta();  // DeviceObjectMetaPb
    // req/obj_meta->obj_key
    objMeta->set_object_key(objectKey);
    LifetimeParamPb lifetime = REFERENCE;
    // req/obj_meta->lifetime
    objMeta->set_lifetime(lifetime);
    // req/obj_meta/locations/loc
    auto loc = objMeta->add_locations();
    loc->set_device_id(deviceId);
    loc->set_client_id(clientId);
    // req/obj_meta/datainfos/
    auto dataInfo = objMeta->add_data_infos();
    dataInfo->set_count(1);
    dataInfo->set_data_type(1);

    (void)manager.PutP2PMetaImpl(req, resp);
}

void GetSelect(MasterDevOcManager &manager, const std::string objectKey, const std::string clientId, const int deviceId)
{
    GetP2PMetaReqPb subReq;
    GetP2PMetaRspPb resp;
    std::shared_ptr<ServerUnaryWriterReader<GetP2PMetaRspPb, GetP2PMetaReqPb>> serverApi;
    auto objMeta = subReq.add_dev_obj_meta();
    // req/obj_meta->obj_key
    objMeta->set_object_key(objectKey);
    LifetimeParamPb lifetime = REFERENCE;
    // req/obj_meta->lifetime
    objMeta->set_lifetime(lifetime);
    // req/obj_meta/locations/loc
    auto loc = objMeta->add_locations();
    loc->set_device_id(deviceId);
    loc->set_client_id(clientId);
    // req/obj_meta/datainfos/
    auto dataInfo = objMeta->add_data_infos();
    dataInfo->set_count(1);
    dataInfo->set_data_type(1);

    (void)manager.ProcessGetP2PMetaRequest(subReq, serverApi);
}

TEST_F(MasterDevOcManagerTest, DISABLED_TestHcclSelect_ExitingHccl)
{
    datasystem::inject::Set("MasterDevOcManager.PutP2PMeta.CheckLocSelect", "return");
    MasterDevOcManager manager{};
    manager.Init();
    std::string clientA = "a", clientB = "b", clientC = "c";
    int clientADeviceId = 0, clientBDeviceId = 1, clientCDeviceId = 2;

    // register object key
    // client b npu 1 - reg obj 7959
    auto tempObjectKey = "7959";
    RegObjectKey(manager, tempObjectKey, clientB, clientBDeviceId);

    // add locs
    // client b npu 1 -> client c npu 2
    AddLocs(manager, tempObjectKey, clientB, clientC, clientBDeviceId, clientCDeviceId);

    // add graph_
    // client b npu 1 -> client a npu 0
    AddGraph(manager, clientB, clientA, clientBDeviceId, clientADeviceId);

    // check get loc
    // client a npu 0 -> (client b / client c)
    GetSelect(manager, tempObjectKey, clientA, clientADeviceId);
}

TEST_F(MasterDevOcManagerTest, DISABLED_TestHcclSelect_NewLoc)
{
    datasystem::inject::Set("MasterDevOcManager.PutP2PMeta.CheckLocSelect", "return");
    MasterDevOcManager manager{};
    manager.Init();
    std::string clientA = "a", clientB = "b", clientC = "c";
    int clientADeviceId = 0, clientBDeviceId = 1, clientCDeviceId = 2;

    // register object key
    // client b npu 1 - reg obj 7959
    auto tempObjectKey = "7959";
    RegObjectKey(manager, tempObjectKey, clientB, clientBDeviceId);

    // add locs
    // client b npu 1 -> client c npu 2
    AddLocs(manager, tempObjectKey, clientB, clientC, clientBDeviceId, clientCDeviceId);

    // check get loc
    // client a npu 0 -> (client b / client c)
    GetSelect(manager, tempObjectKey, clientA, clientADeviceId);
}

void SetDeviceObjectMetaPb(DeviceObjectMetaPb &subreq, std::string objectKey, std::string clientId, int deviceId)
{
    auto loc = subreq.add_locations();
    loc->set_client_id(clientId);
    loc->set_device_id(deviceId);
    subreq.set_object_key(objectKey);
}

void SetGetP2PMetaReqPb(GetP2PMetaReqPb &req, std::string objectKey, std::string clientId, int deviceId)
{
    auto deviceMeta = req.add_dev_obj_meta();
    SetDeviceObjectMetaPb(*deviceMeta, objectKey, clientId, deviceId);
}

void SetPutP2PmetaReq(PutP2PMetaReqPb &req, std::string objectKey, std::string clientId, int deviceId)
{
    auto subreq = req.add_dev_obj_meta();
    subreq->set_object_key(objectKey);
    auto loc = subreq->add_locations();
    loc->set_client_id(clientId);
    loc->set_device_id(deviceId);
}

static pid_t ForkForTest(std::function<void()> func)
{
    pid_t child = fork();
    if (child == 0) {
        // avoid zmq problem when fork.
        std::thread thread(func);
        thread.join();
        exit(0);
    }
    return child;
}

TEST_F(MasterDevOcManagerTest, DISABLED_TestPutGetMultThreadSync)
{
    MasterDevOcManager manager{};
    manager.Init();
    std::string objectKey = "005168";
    datasystem::inject::Set("MasterDevOcManager.ProcessGetP2PMetaRequest.CheckLocSelect", "return");
    datasystem::inject::Set("MasterDevOcManager.PutP2PMetaImpl.TestPutGetMultThreadSync", "return");
    datasystem::inject::Set("MasterDevOcManager.ReturnGetP2PMetaRequest.TestPutGetMultThreadSync", "return");
    // get pre
    GetP2PMetaReqPb getP2PMetaReq;
    SetGetP2PMetaReqPb(getP2PMetaReq, objectKey, "clientA", 1);
    std::shared_ptr<ServerUnaryWriterReader<GetP2PMetaRspPb, GetP2PMetaReqPb>> serverApi;

    // put pre
    PutP2PMetaReqPb putp2pMetaReq;
    SetPutP2PmetaReq(putp2pMetaReq, objectKey, "clientB", 0);
    PutP2PMetaRspPb putp2pmetarsp;

    // get req first
    pid_t childA = ForkForTest([&]() {
        LOG(ERROR) << "GET start";
        manager.ProcessGetP2PMetaRequest(getP2PMetaReq, serverApi);
        LOG(ERROR) << "GET down";
        exit(0);
    });

    pid_t childB = ForkForTest([&]() {
        LOG(ERROR) << "Put start";
        manager.PutP2PMetaImpl(putp2pMetaReq, putp2pmetarsp);
        LOG(ERROR) << "Put down";
        exit(0);
    });
    ASSERT_EQ(0, WaitForChildFork(childA));
    ASSERT_EQ(0, WaitForChildFork(childB));
}
}  // namespace ut
}  // namespace datasystem