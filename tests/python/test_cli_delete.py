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
"""Golden tests for dscli delete cluster JSON and exit codes."""

import argparse
import io
import json
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from yr.datasystem.cli import delete


class FakeOptions:
    pass


class FakeNative:
    ClusterAdminOptions = FakeOptions
    status = "OK"
    error = ""
    deleted_members = None
    captured_options = None
    captured_addresses = None

    @classmethod
    def delete_cluster_members(cls, options, addresses):
        cls.captured_options = options
        cls.captured_addresses = addresses
        return cls.status, cls.error, cls.deleted_members


def make_args(**overrides):
    values = {
        "delete_command": "cluster",
        "etcd_address": "127.0.0.1:2379",
        "coordinator_address": None,
        "cluster_name": "",
        "worker_address": ["127.0.0.1:20010"],
    }
    values.update(overrides)
    return argparse.Namespace(**values)


class CliDeleteTest(unittest.TestCase):
    def run_command(self, args):
        command = delete.Command.__new__(delete.Command)
        output = io.StringIO()
        with patch.object(delete, "_load_native", return_value=FakeNative), redirect_stdout(output):
            exit_code = command.run(args)
        return exit_code, output.getvalue(), json.loads(output.getvalue())

    def setUp(self):
        FakeNative.status = "OK"
        FakeNative.error = ""
        FakeNative.deleted_members = [
            {
                "address": "127.0.0.1:20010",
                "membership_deleted": True,
                "notify_deleted": True,
                "probe_deleted": False,
                "ub_health_deleted": True,
                "topology_member_removed": True,
                "topology_version": 6,
                "error": "",
            }
        ]

    def test_delete_returns_cleaned_results(self):
        exit_code, _, payload = self.run_command(make_args())
        self.assertEqual(exit_code, 0)
        self.assertEqual(list(payload),
                         ["schema_version", "cluster_name", "status", "deleted_members"])
        member = payload["deleted_members"][0]
        self.assertEqual(member["address"], "127.0.0.1:20010")
        self.assertTrue(member["membership_deleted"])
        self.assertEqual(member["topology_version"], 6)

    def test_delete_accepts_multiple_addresses(self):
        FakeNative.deleted_members = [
            {"address": "10.0.0.1:20010", "membership_deleted": True, "notify_deleted": True,
             "probe_deleted": True, "ub_health_deleted": True, "topology_member_removed": True,
             "topology_version": 5, "error": ""},
            {"address": "10.0.0.2:20010", "membership_deleted": True, "notify_deleted": False,
             "probe_deleted": False, "ub_health_deleted": False, "topology_member_removed": True,
             "topology_version": 5, "error": ""},
        ]
        args = make_args(worker_address=["10.0.0.1:20010", "10.0.0.2:20010"])
        exit_code, _, payload = self.run_command(args)
        self.assertEqual(exit_code, 0)
        self.assertEqual(len(payload["deleted_members"]), 2)
        self.assertEqual(FakeNative.captured_addresses, ["10.0.0.1:20010", "10.0.0.2:20010"])

    def test_backend_selection_error_when_both_specified(self):
        args = make_args(coordinator_address="127.0.0.1:31511")
        exit_code, _, payload = self.run_command(args)
        self.assertEqual(exit_code, 1)
        self.assertEqual(list(payload), ["schema_version", "cluster_name", "status", "error"])
        self.assertEqual(payload["status"], "Invalid parameter")

    def test_backend_selection_error_when_neither_specified(self):
        args = make_args(etcd_address=None, coordinator_address=None)
        exit_code, _, payload = self.run_command(args)
        self.assertEqual(exit_code, 1)
        self.assertEqual(payload["status"], "Invalid parameter")

    def test_native_rpc_failure_is_minimal_json(self):
        FakeNative.status = "RPC unavailable"
        FakeNative.error = "failed to connect to coordination backend"
        FakeNative.deleted_members = []
        exit_code, _, payload = self.run_command(make_args())
        self.assertEqual(exit_code, 1)
        self.assertEqual(list(payload), ["schema_version", "cluster_name", "status", "error"])
        self.assertEqual(payload["status"], "RPC unavailable")

    def test_coordinator_backend_is_forwarded(self):
        args = make_args(etcd_address=None, coordinator_address="127.0.0.1:31511")
        exit_code, _, payload = self.run_command(args)
        self.assertEqual(exit_code, 0)
        self.assertEqual(FakeNative.captured_options.coordinator_address, "127.0.0.1:31511")
        self.assertEqual(FakeNative.captured_options.etcd_address, "")

    def test_cluster_name_is_forwarded(self):
        args = make_args(cluster_name="test-cluster")
        exit_code, _, payload = self.run_command(args)
        self.assertEqual(exit_code, 0)
        self.assertEqual(FakeNative.captured_options.cluster_name, "test-cluster")


if __name__ == "__main__":
    unittest.main()
