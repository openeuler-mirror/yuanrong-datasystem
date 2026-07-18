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
"""Golden tests for dscli query JSON and exit codes."""

import argparse
import io
import json
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from yr.datasystem.cli import query


class FakeOptions:
    pass


class FakeNative:
    ClusterQueryOptions = FakeOptions
    status = "OK"
    error = ""
    result = None
    route_keys = None

    @classmethod
    def query_coordination_cluster(cls, _options):
        return cls.status, cls.error, cls.result

    @classmethod
    def query_coordination_routes(cls, _options, keys):
        cls.route_keys = keys
        return cls.status, cls.error, cls.result


def make_args(**overrides):
    values = {
        "query_command": "cluster",
        "etcd_address": "127.0.0.1:2379",
        "coordinator_address": None,
        "cluster_name": "",
        "keys": None,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


class CliQueryTest(unittest.TestCase):
    def run_command(self, args):
        command = query.Command.__new__(query.Command)
        output = io.StringIO()
        with patch.object(query, "_load_native", return_value=FakeNative), redirect_stdout(output):
            exit_code = command.run(args)
        return exit_code, output.getvalue(), json.loads(output.getvalue())

    def test_cluster_outputs_one_ordered_json_object(self):
        FakeNative.status = "OK"
        FakeNative.error = ""
        FakeNative.result = {
            "status": "OK",
            "topology_version": 7,
            "nodes": [
                {
                    "worker_address": "127.0.0.1:31501",
                    "health": "HEALTHY",
                    "state": "ACTIVE",
                    "hash_ranges": ["[0,4294967295]"],
                }
            ],
        }
        exit_code, text, payload = self.run_command(make_args())
        self.assertEqual(exit_code, 0)
        _, end = json.JSONDecoder().raw_decode(text)
        self.assertEqual(text[end:].strip(), "")
        self.assertTrue(text.endswith("\n"))
        self.assertEqual(list(payload), ["schema_version", "cluster_name", "status", "topology_version", "nodes"])

    def test_route_accepts_compact_keys_and_preserves_duplicates(self):
        FakeNative.status = "OK"
        FakeNative.error = ""
        FakeNative.result = {
            "status": "OK",
            "topology_version": 7,
            "routes": [
                {
                    "worker_address": "127.0.0.1:31501",
                    "keys": ["key-1", "key-1"],
                    "health": "HEALTHY",
                }
            ],
        }
        args = make_args(query_command="route", keys=["key-1", "key-1"])
        exit_code, _, payload = self.run_command(args)
        self.assertEqual(exit_code, 0)
        self.assertEqual(FakeNative.route_keys, ["key-1", "key-1"])
        self.assertEqual(payload["routes"][0]["keys"], ["key-1", "key-1"])

    def test_backend_selection_error_is_minimal_json(self):
        args = make_args(coordinator_address="127.0.0.1:31511")
        exit_code, _, payload = self.run_command(args)
        self.assertEqual(exit_code, 1)
        self.assertEqual(list(payload), ["schema_version", "cluster_name", "status", "error"])
        self.assertEqual(payload["status"], "Invalid parameter")

    def test_native_rpc_failure_is_minimal_json(self):
        FakeNative.status = "RPC unavailable"
        FakeNative.error = "failed to connect to coordination backend"
        FakeNative.result = {}
        exit_code, _, payload = self.run_command(make_args())
        self.assertEqual(exit_code, 1)
        self.assertEqual(list(payload), ["schema_version", "cluster_name", "status", "error"])
        self.assertEqual(payload["status"], "RPC unavailable")


if __name__ == "__main__":
    unittest.main()
