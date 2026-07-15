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
 * Description: Register ServiceDiscovery to python.
 */
#include "datasystem/pybind_api/pybind_register.h"
#include "datasystem/utils/service_discovery.h"

namespace datasystem {
PybindDefineRegisterer g_pybind_define_f_SensitiveValue("SensitiveValue", PRIORITY_LOW, [](const py::module *m) {
    py::class_<SensitiveValue>(*m, "SensitiveValue").def(py::init<>()).def(py::init<const std::string &>());
});

PybindDefineRegisterer g_pybind_define_f_ServiceAffinityPolicy(
    "ServiceAffinityPolicy", PRIORITY_LOW, [](const py::module *m) {
        py::enum_<ServiceAffinityPolicy>(*m, "ServiceAffinityPolicy")
            .value("PREFERRED_SAME_NODE", ServiceAffinityPolicy::PREFERRED_SAME_NODE)
            .value("REQUIRED_SAME_NODE", ServiceAffinityPolicy::REQUIRED_SAME_NODE)
            .value("RANDOM", ServiceAffinityPolicy::RANDOM)
            .export_values();
    });

PybindDefineRegisterer g_pybind_define_f_IServiceDiscovery("IServiceDiscovery", PRIORITY_LOW, [](const py::module *m) {
    py::class_<IServiceDiscovery, std::shared_ptr<IServiceDiscovery>>(*m, "IServiceDiscovery");
});

PybindDefineRegisterer g_pybind_define_f_ServiceDiscoveryOptions(
    "ServiceDiscoveryOptions", PRIORITY_LOW, [](const py::module *m) {
        py::class_<ServiceDiscoveryOptions>(*m, "ServiceDiscoveryOptions")
            .def(py::init<>())
            .def_readwrite("etcd_address", &ServiceDiscoveryOptions::etcdAddress)
            .def_readwrite("cluster_name", &ServiceDiscoveryOptions::clusterName)
            .def_property(
                "etcd_ca",
                [](ServiceDiscoveryOptions &opt) -> std::string {
                    return std::string(opt.etcdCa.GetData(), opt.etcdCa.GetSize());
                },
                [](ServiceDiscoveryOptions &opt, const std::string &val) { opt.etcdCa = val; })
            .def_property(
                "etcd_cert",
                [](ServiceDiscoveryOptions &opt) -> std::string {
                    return std::string(opt.etcdCert.GetData(), opt.etcdCert.GetSize());
                },
                [](ServiceDiscoveryOptions &opt, const std::string &val) { opt.etcdCert = val; })
            .def_property(
                "etcd_key",
                [](ServiceDiscoveryOptions &opt) -> std::string {
                    return std::string(opt.etcdKey.GetData(), opt.etcdKey.GetSize());
                },
                [](ServiceDiscoveryOptions &opt, const std::string &val) { opt.etcdKey = val; })
            .def_readwrite("etcd_dns_name", &ServiceDiscoveryOptions::etcdDNSName)
            .def_readwrite("username", &ServiceDiscoveryOptions::username)
            .def_property(
                "password",
                [](ServiceDiscoveryOptions &opt) -> std::string {
                    return std::string(opt.password.GetData(), opt.password.GetSize());
                },
                [](ServiceDiscoveryOptions &opt, const std::string &val) { opt.password = val; })
            .def_readwrite("token_refresh_interval_sec", &ServiceDiscoveryOptions::tokenRefreshIntervalSec)
            .def_readwrite("host_id_env_name", &ServiceDiscoveryOptions::hostIdEnvName)
            .def_readwrite("affinity_policy", &ServiceDiscoveryOptions::affinityPolicy);
    });

PybindDefineRegisterer g_pybind_define_f_ICoordinatorDiscovery(
    "ICoordinatorDiscovery", PRIORITY_LOW, [](const py::module *m) {
        py::class_<ICoordinatorDiscovery, std::shared_ptr<ICoordinatorDiscovery>>(*m, "ICoordinatorDiscovery");
    });

PybindDefineRegisterer g_pybind_define_f_CoordinatorServiceDiscoveryOptions(
    "CoordinatorServiceDiscoveryOptions", PRIORITY_LOW, [](const py::module *m) {
        py::class_<CoordinatorServiceDiscoveryOptions>(*m, "CoordinatorServiceDiscoveryOptions")
            .def(py::init<>())
            .def_readwrite("service_address", &CoordinatorServiceDiscoveryOptions::serviceAddress)
            .def_readwrite("cluster_name", &CoordinatorServiceDiscoveryOptions::clusterName)
            .def_readwrite("host_id_env_name", &CoordinatorServiceDiscoveryOptions::hostIdEnvName)
            .def_readwrite("affinity_policy", &CoordinatorServiceDiscoveryOptions::affinityPolicy)
            .def_readwrite("coordinator_discovery", &CoordinatorServiceDiscoveryOptions::coordinatorDiscovery);
    });

PybindDefineRegisterer g_pybind_define_f_ServiceDiscovery("ServiceDiscovery", PRIORITY_MID, [](const py::module *m) {
    py::class_<ServiceDiscovery, IServiceDiscovery, std::shared_ptr<ServiceDiscovery>>(*m, "ServiceDiscovery")
        .def(py::init<const ServiceDiscoveryOptions &>())
        .def("init", &ServiceDiscovery::Init)
        .def("select_worker",
             [](ServiceDiscovery &sd) {
                 std::string workerIp;
                 int workerPort = 0;
                 bool isSameNode = false;
                 auto status = sd.SelectWorker(workerIp, workerPort, &isSameNode);
                 return std::make_tuple(status, workerIp, workerPort, isSameNode);
             })
        .def("select_same_node_worker",
             [](ServiceDiscovery &sd) {
                 std::string workerIp;
                 int workerPort = 0;
                 auto status = sd.SelectSameNodeWorker(workerIp, workerPort);
                 return std::make_tuple(status, workerIp, workerPort);
             })
        .def("get_all_workers",
             [](ServiceDiscovery &sd) {
                 std::vector<std::string> sameHostAddrs;
                 std::vector<std::string> otherAddrs;
                 auto status = sd.GetAllWorkers(sameHostAddrs, otherAddrs);
                 return std::make_tuple(status, sameHostAddrs, otherAddrs);
             })
        .def("get_affinity_policy", &ServiceDiscovery::GetAffinityPolicy)
        .def("has_host_affinity", &ServiceDiscovery::HasHostAffinity);
});

PybindDefineRegisterer g_pybind_define_f_CoordinatorServiceDiscovery(
    "CoordinatorServiceDiscovery", PRIORITY_MID, [](const py::module *m) {
        py::class_<CoordinatorServiceDiscovery, IServiceDiscovery, std::shared_ptr<CoordinatorServiceDiscovery>>(
            *m, "CoordinatorServiceDiscovery")
            .def(py::init<const CoordinatorServiceDiscoveryOptions &>())
            .def("init", &CoordinatorServiceDiscovery::Init)
            .def("select_worker",
                 [](CoordinatorServiceDiscovery &sd) {
                     std::string workerIp;
                     int workerPort = 0;
                     bool isSameNode = false;
                     auto status = sd.SelectWorker(workerIp, workerPort, &isSameNode);
                     return std::make_tuple(status, workerIp, workerPort, isSameNode);
                 })
            .def("select_same_node_worker",
                 [](CoordinatorServiceDiscovery &sd) {
                     std::string workerIp;
                     int workerPort = 0;
                     auto status = sd.SelectSameNodeWorker(workerIp, workerPort);
                     return std::make_tuple(status, workerIp, workerPort);
                 })
            .def("get_all_workers",
                 [](CoordinatorServiceDiscovery &sd) {
                     std::vector<std::string> sameHostAddrs;
                     std::vector<std::string> otherAddrs;
                     auto status = sd.GetAllWorkers(sameHostAddrs, otherAddrs);
                     return std::make_tuple(status, sameHostAddrs, otherAddrs);
                 })
            .def("get_affinity_policy", &CoordinatorServiceDiscovery::GetAffinityPolicy)
            .def("has_host_affinity", &CoordinatorServiceDiscovery::HasHostAffinity);
    });
}  // namespace datasystem
