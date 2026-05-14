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

#include "datasystem/worker/worker_cli.h"

#include <iostream>
#include <memory>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/hash_ring.pb.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/hash_ring/hash_ring_pb_utils.h"

DS_DEFINE_string(get_hash_ring, "", "Get hashring from etcd and save to file with json format.");
DS_DEFINE_string(set_hash_ring, "", "Read hashring from file with json fromat and update to etcd.");

DS_DECLARE_string(etcd_address);
DS_DECLARE_string(etcd_username);
DS_DECLARE_string(etcd_password);
DS_DECLARE_string(metastore_address);
DS_DECLARE_int32(minloglevel);
DS_DECLARE_uint32(etcd_token_refresh_interval_s);

namespace datasystem {
namespace cli {
namespace {
Status GetEtcdStore(std::unique_ptr<EtcdStore> &etcdStore)
{
    // Use metastore_address if specified, otherwise use etcd_address
    std::string backendAddress;
    // Check if both etcd_address and metastore_address are specified
    if (!FLAGS_metastore_address.empty() && !FLAGS_etcd_address.empty()) {
        RETURN_STATUS(K_RUNTIME_ERROR, "Only one of etcd_address or metastore_address can be specified, not both");
    }
    if (!FLAGS_metastore_address.empty()) {
        backendAddress = FLAGS_metastore_address;
    } else if (FLAGS_etcd_address.empty()) {
        RETURN_STATUS(K_RUNTIME_ERROR, "argument etcd_address is empty.");
    } else {
        backendAddress = FLAGS_etcd_address;
    }
    if (FLAGS_enable_etcd_auth) {
        CHECK_FAIL_RETURN_STATUS(!FLAGS_etcd_ca.empty(), K_INVALID, "etcd_ca is required when enable etcd auth");
        CHECK_FAIL_RETURN_STATUS(!FLAGS_etcd_cert.empty(), K_INVALID, "etcd_cert is required when enable etcd auth");
        CHECK_FAIL_RETURN_STATUS(!FLAGS_etcd_key.empty(), K_INVALID, "etcd_key is required when enable etcd auth");
    }
    etcdStore = std::make_unique<EtcdStore>(backendAddress);
    RETURN_IF_NOT_OK(etcdStore->Init());
    RETURN_IF_NOT_OK(
        etcdStore->Authenticate(FLAGS_etcd_username, FLAGS_etcd_password, FLAGS_etcd_token_refresh_interval_s));
    return etcdStore->CreateTable(ETCD_RING_PREFIX, ETCD_RING_PREFIX);
}
}  // namespace

Status SaveHashRingToFile(const std::string &filename)
{
    std::unique_ptr<EtcdStore> etcdStore;
    RETURN_IF_NOT_OK(GetEtcdStore(etcdStore));
    std::string hashRingStr;
    RETURN_IF_NOT_OK(etcdStore->Get(ETCD_RING_PREFIX, "", hashRingStr));
    HashRingPb hashring;
    if (!hashring.ParseFromString(hashRingStr)) {
        RETURN_STATUS(K_RUNTIME_ERROR, FormatString("Parse to HashRingPb failed"));
    }
    return worker::WriteToJsonFile(filename, hashring);
}

Status UpdateHashRingFromFile(const std::string &filename)
{
    std::unique_ptr<EtcdStore> etcdStore;
    RETURN_IF_NOT_OK(GetEtcdStore(etcdStore));
    HashRingPb hashRing;
    RETURN_IF_NOT_OK(worker::ReadFromJsonFile(filename, hashRing));
    std::string hashRingStr;
    if (!hashRing.SerializeToString(&hashRingStr)) {
        RETURN_STATUS(K_RUNTIME_ERROR, FormatString("HashRingPb SerializeToString failed"));
    }
    return etcdStore->Put(ETCD_RING_PREFIX, "", hashRingStr);
}

bool HandleCli()
{
    if (FLAGS_get_hash_ring.empty() && FLAGS_set_hash_ring.empty()) {
        return false;
    }
    Status rc;
    std::stringstream ss;
    if (!FLAGS_get_hash_ring.empty()) {
        ss << "Get hashring from etcd and save to file " << FLAGS_get_hash_ring;
        rc = SaveHashRingToFile(FLAGS_get_hash_ring);
    }
    if (!FLAGS_set_hash_ring.empty()) {
        ss << "Read hashring from file " << FLAGS_set_hash_ring << " and save to etcd";
        rc = UpdateHashRingFromFile(FLAGS_set_hash_ring);
    }

    if (rc.IsOk()) {
        ss << " success.\n";
        std::cout << ss.str();
    } else {
        ss << " failed with: " << rc.GetMsg() << "\n";
        std::cerr << ss.str();
    }
    FLAGS_minloglevel = 1;
    return true;
}

}  // namespace cli
}  // namespace datasystem
