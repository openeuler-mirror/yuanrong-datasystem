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
#include "datasystem/pybind_api/pybind_register.h"

#include <limits>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "datasystem/client/cluster_query/cluster_query_client.h"

namespace datasystem {
namespace {

namespace query = client::cluster_query;
using QueryReturn = std::tuple<std::string, std::string, py::dict>;

const char *HealthName(query::ClusterNodeHealth health)
{
    switch (health) {
        case query::ClusterNodeHealth::HEALTHY:
            return "HEALTHY";
        case query::ClusterNodeHealth::DRAINING:
            return "DRAINING";
        case query::ClusterNodeHealth::STARTING:
            return "STARTING";
        case query::ClusterNodeHealth::RECOVERING:
            return "RECOVERING";
        case query::ClusterNodeHealth::UNHEALTHY:
            return "UNHEALTHY";
    }
    return "UNHEALTHY";
}

const char *MemberStateName(cluster::MemberState state)
{
    switch (state) {
        case cluster::MemberState::INITIAL:
            return "INITIAL";
        case cluster::MemberState::JOINING:
            return "JOINING";
        case cluster::MemberState::ACTIVE:
            return "ACTIVE";
        case cluster::MemberState::PRE_LEAVING:
            return "PRE_LEAVING";
        case cluster::MemberState::LEAVING:
            return "LEAVING";
        case cluster::MemberState::FAILED:
            return "FAILED";
    }
    return "FAILED";
}

std::string RangeString(const query::QueryHashRange &range)
{
    if (range.fullRing) {
        return "[0," + std::to_string(std::numeric_limits<uint32_t>::max()) + "]";
    }
    return "(" + std::to_string(range.start) + "," + std::to_string(range.end) + "]";
}

py::dict ClusterResultToDict(const query::ClusterQueryResult &result)
{
    py::dict output;
    output["status"] = "OK";
    output["topology_version"] = result.topologyVersion;
    py::list nodes;
    for (const auto &node : result.nodes) {
        py::dict item;
        item["worker_address"] = node.workerAddress;
        item["health"] = HealthName(node.health);
        item["state"] = MemberStateName(node.state);
        py::list ranges;
        for (const auto &range : node.hashRanges) {
            ranges.append(RangeString(range));
        }
        item["hash_ranges"] = std::move(ranges);
        nodes.append(std::move(item));
    }
    output["nodes"] = std::move(nodes);
    return output;
}

py::dict RouteResultToDict(const query::RouteQueryResult &result)
{
    py::dict output;
    output["status"] = "OK";
    output["topology_version"] = result.topologyVersion;
    py::list routes;
    for (const auto &route : result.routes) {
        py::dict item;
        item["worker_address"] = route.workerAddress;
        item["keys"] = route.keys;
        item["health"] = HealthName(route.health);
        routes.append(std::move(item));
    }
    output["routes"] = std::move(routes);
    return output;
}

std::string StatusName(const Status &status)
{
    return status.IsOk() ? "OK" : Status::StatusCodeName(status.GetCode());
}

QueryReturn QueryCluster(query::ClusterQueryOptions options)
{
    query::ClusterQueryClient client(std::move(options));
    query::ClusterQueryResult result;
    Status status;
    {
        py::gil_scoped_release release;
        status = client.Init();
        if (status.IsOk()) {
            status = client.QueryCluster(result);
        }
    }
    return { StatusName(status), status.GetMsg(), status.IsOk() ? ClusterResultToDict(result) : py::dict() };
}

QueryReturn QueryRoutes(query::ClusterQueryOptions options, const std::vector<std::string> &keys)
{
    query::ClusterQueryClient client(std::move(options));
    query::RouteQueryResult result;
    Status status;
    {
        py::gil_scoped_release release;
        status = client.Init();
        if (status.IsOk()) {
            status = client.QueryRoutes(keys, result);
        }
    }
    return { StatusName(status), status.GetMsg(), status.IsOk() ? RouteResultToDict(result) : py::dict() };
}

}  // namespace

PybindDefineRegisterer g_pybind_define_f_ClusterQueryOptions(
    "ClusterQueryOptions", PRIORITY_LOW, [](py::module *module) {
        py::class_<query::ClusterQueryOptions>(*module, "ClusterQueryOptions")
            .def(py::init<>())
            .def_readwrite("cluster_name", &query::ClusterQueryOptions::clusterName)
            .def_readwrite("etcd_address", &query::ClusterQueryOptions::etcdAddress)
            .def_readwrite("coordinator_address", &query::ClusterQueryOptions::coordinatorAddress);
    });

PybindDefineRegisterer g_pybind_define_f_QueryCoordination(
    "QueryCoordination", PRIORITY_MID, [](py::module *module) {
        module->def("query_coordination_cluster", &QueryCluster);
        module->def("query_coordination_routes", &QueryRoutes);
    });

}  // namespace datasystem
