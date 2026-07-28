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
"""Tests for dsbench child-process environment generation."""

import argparse
import os
import unittest
from unittest.mock import patch

from yr.datasystem.cli.benchmark.common import BenchTestCase
from yr.datasystem.cli.benchmark.kv import validator
from yr.datasystem.cli.benchmark.kv.bench_test_case import KVArgs, KVBenchTestCase
from yr.datasystem.cli.benchmark.kv.bench_suite_builder import KVBenchSuiteBuilder
from yr.datasystem.cli.benchmark.task import BenchArgs


def make_test_case():
    args = argparse.Namespace(min_log_level=2, log_monitor_enable=False)
    bench_args = BenchArgs(
        name="bench_kv",
        start_time="20260724160000",
        cwd="/tmp",
        log_dir="/tmp/dsbench-test",
        result_csv_file="result.csv",
        args=args,
    )
    return BenchTestCase("set", bench_args, None)


def make_kv_test_case(**overrides):
    values = {
        "owner_worker": "",
        "prefix": "Bench",
        "set_worker_addresses": "127.0.0.1:31501",
        "get_worker_addresses": "127.0.0.1:31502",
        "client_num": 1,
        "thread_num": 1,
        "batch_num": 1,
        "num": 1,
        "size": "1KB",
        "access_key": "",
        "secret_key": "",
        "numa": "",
        "skip_local": False,
        "operation": "all",
        "enable_local_cache": "false",
        "data_placement_policy": "PREFERRED_META_OWNER",
        "source_worker_num": None,
    }
    values.update(overrides)
    args = argparse.Namespace(**values)
    bench_args = BenchArgs(
        name="bench_kv",
        start_time="20260724160000",
        cwd="/tmp",
        log_dir="/tmp/dsbench-test",
        result_csv_file="result.csv",
        args=args,
    )
    return KVBenchTestCase("placement", bench_args, None, 1)


class DsbenchEnvironmentTest(unittest.TestCase):
    def test_use_brpc_environment_is_forwarded(self):
        test_case = make_test_case()
        for value in ("true", "false"):
            with self.subTest(value=value), patch.dict(os.environ, {"DATASYSTEM_USE_BRPC": value}):
                self.assertEqual(test_case.generate_env()["DATASYSTEM_USE_BRPC"], value)

    def test_use_brpc_environment_is_omitted_when_unset(self):
        test_case = make_test_case()
        with patch.dict(os.environ, {}, clear=True):
            self.assertNotIn("DATASYSTEM_USE_BRPC", test_case.generate_env())


class DsbenchDataPlacementTest(unittest.TestCase):
    def test_policy_is_forwarded_to_cpp_command(self):
        test_case = make_kv_test_case(owner_worker="127.0.0.1:31503")
        command_args = test_case.to_base_command_args(
            KVArgs(num=1, size="1KB", client_num=1, thread_num=1, batch_num=1)
        )
        command = test_case.generate_commands(command_args, "/tmp/dsbench_cpp")
        self.assertIn("--owner_worker=127.0.0.1:31503", command)
        self.assertIn("--enable_local_cache=false", command)
        self.assertIn("--data_placement_policy=PREFERRED_META_OWNER", command)

    def test_unspecified_policy_is_not_forwarded(self):
        test_case = make_kv_test_case(data_placement_policy=None)
        command_args = test_case.to_base_command_args(
            KVArgs(num=1, size="1KB", client_num=1, thread_num=1, batch_num=1)
        )
        command = test_case.generate_commands(command_args, "/tmp/dsbench_cpp")
        self.assertNotIn("--data_placement_policy", command)

    def test_policy_requires_routing_transport_path(self):
        args = argparse.Namespace(
            all=False,
            testcase_file=None,
            client_num=None,
            thread_num=None,
            batch_num=None,
            num=None,
            size=None,
            concurrent=False,
            operation="set",
            data_placement_policy="PREFERRED_META_OWNER",
            enable_local_cache="true",
            set_worker_addresses="127.0.0.1:31501",
            get_worker_addresses="",
            source_worker_num=None,
        )
        self.assertFalse(validator.validate_mutex_arguments(args))

    def test_source_worker_num_rejected_for_full_flow(self):
        args = argparse.Namespace(
            all=False,
            testcase_file=None,
            client_num=None,
            thread_num=None,
            batch_num=None,
            num=None,
            size=None,
            concurrent=False,
            operation="all",
            data_placement_policy=None,
            enable_local_cache="true",
            set_worker_addresses="127.0.0.1:31501",
            get_worker_addresses="127.0.0.1:31502",
            source_worker_num=2,
        )
        self.assertFalse(validator.validate_mutex_arguments(args))

    def test_standalone_operation_rejected_for_full_or_customized_mode(self):
        for mode in ("all", "testcase_file"):
            with self.subTest(mode=mode):
                args = make_kv_test_case().bench_args.args
                args.all = mode == "all"
                args.testcase_file = "cases.json" if mode == "testcase_file" else None
                args.concurrent = False
                args.operation = "set"
                self.assertFalse(validator.validate_mutex_arguments(args))

    def test_get_ignores_write_policy_and_set_rejects_get_option(self):
        args = make_kv_test_case().bench_args.args
        args.all = False
        args.testcase_file = None
        args.concurrent = False
        args.operation = "get"
        args.data_placement_policy = "PREFERRED_META_OWNER"
        self.assertTrue(validator.validate_mutex_arguments(args))

        args.operation = "set"
        args.data_placement_policy = None
        args.skip_local = True
        self.assertFalse(validator.validate_mutex_arguments(args))

    def test_skip_local_requires_source_worker_addresses(self):
        args = argparse.Namespace(
            all=False,
            testcase_file=None,
            client_num=None,
            thread_num=None,
            batch_num=None,
            num=None,
            size=None,
            concurrent=False,
            operation="get",
            data_placement_policy=None,
            enable_local_cache="false",
            set_worker_addresses="",
            get_worker_addresses="127.0.0.1:31502",
            source_worker_num=2,
            skip_local=True,
        )
        self.assertFalse(validator.validate_mutex_arguments(args))

    def test_skip_local_requires_matching_source_worker_count(self):
        args = argparse.Namespace(
            all=False,
            testcase_file=None,
            client_num=None,
            thread_num=None,
            batch_num=None,
            num=None,
            size=None,
            concurrent=False,
            operation="get",
            data_placement_policy=None,
            enable_local_cache="false",
            set_worker_addresses="127.0.0.1:31501,127.0.0.1:31502",
            get_worker_addresses="127.0.0.1:31502",
            source_worker_num=1,
            skip_local=True,
        )
        self.assertFalse(validator.validate_mutex_arguments(args))

    def test_source_worker_num_must_match_set_worker_count(self):
        for operation in ("get", "del"):
            with self.subTest(operation=operation):
                args = argparse.Namespace(
                    all=False,
                    testcase_file=None,
                    client_num=None,
                    thread_num=None,
                    batch_num=None,
                    num=None,
                    size=None,
                    concurrent=False,
                    operation=operation,
                    data_placement_policy=None,
                    enable_local_cache="false",
                    set_worker_addresses="127.0.0.1:31501,127.0.0.1:31502",
                    get_worker_addresses="127.0.0.1:31503",
                    source_worker_num=1,
                    skip_local=False,
                )
                self.assertFalse(validator.validate_mutex_arguments(args))

    def test_prefill_ignores_empty_set_worker_list(self):
        test_case = make_kv_test_case(set_worker_addresses="")
        kv_args = KVArgs(num=1, size="1KB", client_num=1, thread_num=1, batch_num=1)
        with patch.object(test_case, "add_task_from_command_args") as add_task:
            test_case.add_prefill_task(kv_args)
        add_task.assert_not_called()
        self.assertEqual(test_case.tasks, [])

    def test_concurrent_flow_ignores_empty_worker_lists(self):
        test_case = make_kv_test_case(
            set_worker_addresses="", get_worker_addresses=""
        )
        kv_args = KVArgs(num=1, size="1KB", client_num=1, thread_num=1, batch_num=1)
        with patch.object(test_case, "add_task_from_command_args") as add_task:
            test_case.add_concurrent_task(kv_args)
        add_task.assert_not_called()
        self.assertEqual(test_case.tasks, [])

    def test_set_operation_builds_only_set_task(self):
        test_case = make_kv_test_case()
        test_case.bench_args.args.operation = "set"
        test_case.bench_args.args.concurrent = False
        builder = KVBenchSuiteBuilder.__new__(KVBenchSuiteBuilder)
        builder.bench_args = test_case.bench_args
        builder.final_csv_filepath = None
        kv_args = KVArgs(num=1, size="1KB", client_num=1, thread_num=1, batch_num=1)
        with patch(
            "yr.datasystem.cli.benchmark.kv.bench_suite_builder.KVBenchOutputHandler"
        ), patch(
            "yr.datasystem.cli.benchmark.kv.bench_suite_builder.KVBenchTestCase"
        ) as mock_test_case:
            built = builder.create_testcase_from_args(kv_args, 1)
        built.add_set_task.assert_called_once_with(kv_args)
        built.add_get_task.assert_not_called()
        built.add_del_task.assert_not_called()

    def test_get_operation_builds_only_get_task(self):
        test_case = make_kv_test_case(source_worker_num=1)
        test_case.bench_args.args.operation = "get"
        test_case.bench_args.args.concurrent = False
        builder = KVBenchSuiteBuilder.__new__(KVBenchSuiteBuilder)
        builder.bench_args = test_case.bench_args
        builder.final_csv_filepath = None
        kv_args = KVArgs(num=1, size="1KB", client_num=1, thread_num=1, batch_num=1)
        with patch(
            "yr.datasystem.cli.benchmark.kv.bench_suite_builder.KVBenchOutputHandler"
        ), patch(
            "yr.datasystem.cli.benchmark.kv.bench_suite_builder.KVBenchTestCase"
        ) as mock_test_case:
            built = builder.create_testcase_from_args(kv_args, 1)
        built.add_set_task.assert_not_called()
        built.add_get_task.assert_called_once_with(kv_args)
        built.add_del_task.assert_not_called()

    def test_del_operation_builds_only_del_task(self):
        test_case = make_kv_test_case(source_worker_num=1)
        test_case.bench_args.args.operation = "del"
        test_case.bench_args.args.concurrent = False
        builder = KVBenchSuiteBuilder.__new__(KVBenchSuiteBuilder)
        builder.bench_args = test_case.bench_args
        builder.final_csv_filepath = None
        kv_args = KVArgs(num=1, size="1KB", client_num=1, thread_num=1, batch_num=1)
        with patch(
            "yr.datasystem.cli.benchmark.kv.bench_suite_builder.KVBenchOutputHandler"
        ), patch(
            "yr.datasystem.cli.benchmark.kv.bench_suite_builder.KVBenchTestCase"
        ) as mock_test_case:
            built = builder.create_testcase_from_args(kv_args, 1)
        built.add_set_task.assert_not_called()
        built.add_get_task.assert_not_called()
        built.add_del_task.assert_called_once_with(kv_args)

    def test_concurrent_flow_keeps_cleanup_task(self):
        test_case = make_kv_test_case()
        test_case.bench_args.args.operation = "all"
        test_case.bench_args.args.concurrent = True
        builder = KVBenchSuiteBuilder.__new__(KVBenchSuiteBuilder)
        builder.bench_args = test_case.bench_args
        builder.final_csv_filepath = None
        kv_args = KVArgs(num=1, size="1KB", client_num=1, thread_num=1, batch_num=1)
        with patch(
            "yr.datasystem.cli.benchmark.kv.bench_suite_builder.KVBenchOutputHandler"
        ), patch(
            "yr.datasystem.cli.benchmark.kv.bench_suite_builder.KVBenchTestCase"
        ) as mock_test_case:
            built = builder.create_testcase_from_args(kv_args, 1)
        built.add_prefill_task.assert_called_once_with(kv_args)
        built.add_concurrent_task.assert_called_once_with(kv_args)
        built.add_del_task.assert_called_once_with(kv_args)


if __name__ == "__main__":
    unittest.main()
