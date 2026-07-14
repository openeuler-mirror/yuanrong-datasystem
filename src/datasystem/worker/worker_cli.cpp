/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "datasystem/worker/worker_cli.h"

#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>

#include <google/protobuf/util/json_util.h>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/cluster/repository/topology_key_helper.h"
#include "datasystem/protos/cluster_topology.pb.h"
#include "datasystem/utils/status.h"

DS_DEFINE_string(get_cluster_topology, "", "Get topology from etcd and save to file with json format.");
DS_DEFINE_string(set_cluster_topology, "", "Read topology from a JSON file and update ETCD.");

DS_DECLARE_string(etcd_address);
DS_DECLARE_string(etcd_username);
DS_DECLARE_string(etcd_password);
DS_DECLARE_string(metastore_address);
DS_DECLARE_int32(minloglevel);
DS_DECLARE_uint32(etcd_token_refresh_interval_s);
DS_DECLARE_string(cluster_name);

namespace datasystem {
namespace cli {
namespace {
namespace cluster_schema = ::datasystem;

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
    std::unique_ptr<cluster::TopologyKeyHelper> keys;
    RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::Create(FLAGS_cluster_name, keys));
    return etcdStore->CreateTableWithExactPrefix(keys->TopologyTable(), keys->TopologyTable());
}

Status WriteTopologyJsonFile(const std::string &filename, const cluster_schema::ClusterTopologyPb &topology)
{
    std::string json;
    google::protobuf::util::JsonPrintOptions options;
    options.add_whitespace = true;
    auto protobufRc = google::protobuf::util::MessageToJsonString(topology, &json, options);
    CHECK_FAIL_RETURN_STATUS(protobufRc.ok(), K_RUNTIME_ERROR, protobufRc.ToString());
    std::ofstream out(filename);
    CHECK_FAIL_RETURN_STATUS(out.is_open(), K_RUNTIME_ERROR, "open cluster topology json file failed");
    out << json;
    return Status::OK();
}

Status ReadTopologyJsonFile(const std::string &filename, cluster_schema::ClusterTopologyPb &topology)
{
    std::ifstream in(filename);
    CHECK_FAIL_RETURN_STATUS(in.is_open(), K_RUNTIME_ERROR, "open cluster topology json file failed");
    std::stringstream buffer;
    buffer << in.rdbuf();
    auto protobufRc = google::protobuf::util::JsonStringToMessage(buffer.str(), &topology);
    CHECK_FAIL_RETURN_STATUS(protobufRc.ok(), K_RUNTIME_ERROR, protobufRc.ToString());
    return Status::OK();
}
}  // namespace

Status SaveClusterTopologyToFile(const std::string &filename)
{
    std::unique_ptr<EtcdStore> etcdStore;
    RETURN_IF_NOT_OK(GetEtcdStore(etcdStore));
    std::unique_ptr<cluster::TopologyKeyHelper> keys;
    RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::Create(FLAGS_cluster_name, keys));
    std::string topologyBytes;
    RETURN_IF_NOT_OK(etcdStore->Get(keys->TopologyTable(), "", topologyBytes));
    cluster_schema::ClusterTopologyPb topology;
    if (!topology.ParseFromString(topologyBytes)) {
        RETURN_STATUS(K_RUNTIME_ERROR, "Parse ClusterTopologyPb failed");
    }
    return WriteTopologyJsonFile(filename, topology);
}

Status UpdateClusterTopologyFromFile(const std::string &filename)
{
    std::unique_ptr<EtcdStore> etcdStore;
    RETURN_IF_NOT_OK(GetEtcdStore(etcdStore));
    std::unique_ptr<cluster::TopologyKeyHelper> keys;
    RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::Create(FLAGS_cluster_name, keys));
    cluster_schema::ClusterTopologyPb topology;
    RETURN_IF_NOT_OK(ReadTopologyJsonFile(filename, topology));
    std::string topologyBytes;
    if (!topology.SerializeToString(&topologyBytes)) {
        RETURN_STATUS(K_RUNTIME_ERROR, "Serialize ClusterTopologyPb failed");
    }
    return etcdStore->Put(keys->TopologyTable(), "", topologyBytes);
}

bool HandleCli()
{
    if (FLAGS_get_cluster_topology.empty() && FLAGS_set_cluster_topology.empty()) {
        return false;
    }
    Status rc;
    std::stringstream ss;
    if (!FLAGS_get_cluster_topology.empty()) {
        ss << "Get topology from etcd and save to file " << FLAGS_get_cluster_topology;
        rc = SaveClusterTopologyToFile(FLAGS_get_cluster_topology);
    }
    if (!FLAGS_set_cluster_topology.empty()) {
        ss << "Read topology from file " << FLAGS_set_cluster_topology << " and save to etcd";
        rc = UpdateClusterTopologyFromFile(FLAGS_set_cluster_topology);
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
