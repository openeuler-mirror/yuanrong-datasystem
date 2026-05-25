# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Service Discovery client python interface.
"""
from __future__ import absolute_import

from enum import Enum
from typing import List, Tuple

from yr.datasystem.lib import libds_client_py as ds
from yr.datasystem.util import Validator as validator


class ServiceAffinityPolicy(Enum):
    """
    The `ServiceAffinityPolicy` class defines the affinity policy for service discovery.

    Currently, the following 'ServiceAffinityPolicy' are supported:

    ===================================  ==================================================================
    Definition                            Description
    ===================================  ==================================================================
    `ServiceAffinityPolicy.PREFERRED_SAME_NODE`  Prefer selecting a worker on the same node if available.
    `ServiceAffinityPolicy.REQUIRED_SAME_NODE`  Require selecting a worker on the same node.
    `ServiceAffinityPolicy.RANDOM`              Select a worker randomly across all available workers.
    ===================================  ==================================================================
    """

    PREFERRED_SAME_NODE = ds.ServiceAffinityPolicy.PREFERRED_SAME_NODE
    REQUIRED_SAME_NODE = ds.ServiceAffinityPolicy.REQUIRED_SAME_NODE
    RANDOM = ds.ServiceAffinityPolicy.RANDOM


class ServiceDiscoveryOptions:
    """
    The `ServiceDiscoveryOptions` class defines the options for ServiceDiscovery.

    Args:
        etcd_address(str): The address of the ETCD server.
        cluster_name(str): The name of the cluster. Default is empty string.
        etcd_ca(str): The CA certificate for ETCD TLS authentication. Default is empty string.
        etcd_cert(str): The client certificate for ETCD TLS authentication. Default is empty string.
        etcd_key(str): The client private key for ETCD TLS authentication. Default is empty string.
        etcd_dns_name(str): The DNS name for ETCD TLS authentication. Default is empty string.
        username(str): The username for ETCD authentication. Default is empty string.
        password(str): The password for ETCD authentication. Default is empty string.
        token_refresh_interval_sec(int): The interval in seconds for refreshing the authentication token.
            Default is 30 seconds.
        host_id_env_name(str): The environment variable name for the host ID. Default is empty string.
        affinity_policy(ServiceAffinityPolicy): The affinity policy for worker selection.
            Default is PREFERRED_SAME_NODE.
    """

    def __init__(self):
        self.etcd_address = ""
        self.cluster_name = ""
        self.etcd_ca = ""
        self.etcd_cert = ""
        self.etcd_key = ""
        self.etcd_dns_name = ""
        self.username = ""
        self.password = ""
        self.token_refresh_interval_sec = 30
        self.host_id_env_name = ""
        self.affinity_policy = ServiceAffinityPolicy.PREFERRED_SAME_NODE


class ServiceDiscovery:
    """
    The `ServiceDiscovery` class provides the ability to discover available workers in the cluster.

    When the SDK does not know which worker to connect to, it can use this feature to obtain
    an available worker for connection.

    Args:
        options(ServiceDiscoveryOptions): The options for service discovery.

    Raises:
        TypeError: Raise a type error if the input parameter is invalid.

    Examples:
        >>> from yr.datasystem import ServiceDiscovery, ServiceDiscoveryOptions, ServiceAffinityPolicy
        >>> options = ServiceDiscoveryOptions()
        >>> options.etcd_address = "127.0.0.1:2379"
        >>> options.affinity_policy = ServiceAffinityPolicy.PREFERRED_SAME_NODE
        >>> sd = ServiceDiscovery(options)
        >>> sd.init()
        >>> status, worker_ip, worker_port, is_same_node = sd.select_worker()
    """

    def __init__(self, options: ServiceDiscoveryOptions):
        """
        Constructor of the ServiceDiscovery class.

        Args:
            options(ServiceDiscoveryOptions): The options for service discovery.

        Raises:
            TypeError: Raise a type error if the input parameter is invalid.
        """
        args = [["options", options, ServiceDiscoveryOptions]]
        validator.check_args_types(args)

        cpp_options = ds.ServiceDiscoveryOptions()
        cpp_options.etcd_address = options.etcd_address
        cpp_options.cluster_name = options.cluster_name
        cpp_options.etcd_ca = options.etcd_ca
        cpp_options.etcd_cert = options.etcd_cert
        cpp_options.etcd_key = options.etcd_key
        cpp_options.etcd_dns_name = options.etcd_dns_name
        cpp_options.username = options.username
        cpp_options.password = options.password
        cpp_options.token_refresh_interval_sec = options.token_refresh_interval_sec
        cpp_options.host_id_env_name = options.host_id_env_name
        cpp_options.affinity_policy = options.affinity_policy.value

        self._sd = ds.ServiceDiscovery(cpp_options)

    def init(self):
        """
        Initialize the ServiceDiscovery.

        Raises:
            RuntimeError: Raise a runtime error if the initialization fails.
        """
        status = self._sd.init()
        if status.is_error():
            raise RuntimeError(status.to_string())

    def select_worker(self) -> Tuple["Status", str, int, bool]:
        """
        Select a worker address based on the configured affinity policy.

        Returns:
            Tuple[Status, str, int, bool]: A tuple containing:
                - Status: The status of the operation.
                - str: The IP address of the selected worker.
                - int: The port of the selected worker.
                - bool: True if the selected worker is on the same node as the client.

        Raises:
            RuntimeError: Raise a runtime error if worker selection fails.
        """
        status, worker_ip, worker_port, is_same_node = self._sd.select_worker()
        if status.is_error():
            raise RuntimeError(status.to_string())
        return status, worker_ip, worker_port, is_same_node

    def select_same_node_worker(self) -> Tuple["Status", str, int]:
        """
        Select a worker that is on the same node as the client.

        Returns:
            Tuple[Status, str, int]: A tuple containing:
                - Status: The status of the operation.
                - str: The IP address of the selected worker.
                - int: The port of the selected worker.

        Raises:
            RuntimeError: Raise a runtime error if worker selection fails.
        """
        status, worker_ip, worker_port = self._sd.select_same_node_worker()
        if status.is_error():
            raise RuntimeError(status.to_string())
        return status, worker_ip, worker_port

    def get_all_workers(self) -> Tuple["Status", List[str], List[str]]:
        """
        Get all available worker addresses, split by host affinity.

        Returns:
            Tuple[Status, List[str], List[str]]: A tuple containing:
                - Status: The status of the operation.
                - List[str]: Addresses of workers on the same node.
                - List[str]: Addresses of workers on other nodes.

        Raises:
            RuntimeError: Raise a runtime error if getting workers fails.
        """
        status, same_host_addrs, other_addrs = self._sd.get_all_workers()
        if status.is_error():
            raise RuntimeError(status.to_string())
        return status, same_host_addrs, other_addrs

    def get_affinity_policy(self) -> ServiceAffinityPolicy:
        """
        Get the current affinity policy.

        Returns:
            ServiceAffinityPolicy: The current affinity policy.
        """
        policy = self._sd.get_affinity_policy()
        return ServiceAffinityPolicy(policy)

    def has_host_affinity(self) -> bool:
        """
        Check if host locality is actually active.

        Returns:
            bool: True if the client can meaningfully select same-node workers.
                  False if the policy is RANDOM or hostId is missing.
        """
        return self._sd.has_host_affinity()
