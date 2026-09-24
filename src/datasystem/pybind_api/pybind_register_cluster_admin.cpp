/*
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
 * Description: pybind registration for ClusterAdminClient.
 */
#include "datasystem/pybind_api/pybind_register.h"

#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "datasystem/client/cluster_admin/cluster_admin_client.h"

namespace datasystem {
namespace {

using client::cluster_admin::ClusterAdminClient;
using client::cluster_admin::ClusterAdminOptions;
using client::cluster_admin::DeleteClusterMemberResult;
using AdminReturn = std::tuple<std::string, std::string, py::list>;

std::string StatusName(const Status &status)
{
    return status.IsOk() ? "OK" : Status::StatusCodeName(status.GetCode());
}

py::dict ResultToDict(const DeleteClusterMemberResult &result)
{
    py::dict item;
    item["address"] = result.address;
    item["membership_deleted"] = result.membershipDeleted;
    item["notify_deleted"] = result.notifyDeleted;
    item["probe_deleted"] = result.probeDeleted;
    item["ub_health_deleted"] = result.ubHealthDeleted;
    item["topology_member_removed"] = result.topologyMemberRemoved;
    item["topology_version"] = result.topologyVersion;
    item["error"] = result.error;
    return item;
}

AdminReturn DeleteClusterMembers(ClusterAdminOptions options, const std::vector<std::string> &addresses)
{
    ClusterAdminClient client(std::move(options));
    std::vector<DeleteClusterMemberResult> results;
    Status status;
    {
        py::gil_scoped_release release;
        status = client.Init();
        if (status.IsOk()) {
            status = client.DeleteClusterMembers(addresses, results);
        }
    }
    py::list resultList;
    for (const auto &result : results) {
        resultList.append(ResultToDict(result));
    }
    return { StatusName(status), status.GetMsg(), std::move(resultList) };
}

}  // namespace

PybindDefineRegisterer g_pybind_define_f_ClusterAdminOptions(
    "ClusterAdminOptions", PRIORITY_LOW, [](py::module *module) {
        py::class_<ClusterAdminOptions>(*module, "ClusterAdminOptions")
            .def(py::init<>())
            .def_readwrite("cluster_name", &ClusterAdminOptions::clusterName)
            .def_readwrite("etcd_address", &ClusterAdminOptions::etcdAddress)
            .def_readwrite("coordinator_address", &ClusterAdminOptions::coordinatorAddress);
    });

PybindDefineRegisterer g_pybind_define_f_ClusterAdmin(
    "ClusterAdmin", PRIORITY_MID, [](py::module *module) {
        module->def("delete_cluster_members", &DeleteClusterMembers);
    });

}  // namespace datasystem
