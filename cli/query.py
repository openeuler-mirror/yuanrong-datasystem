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
"""Read-only coordination backend diagnostics."""

import json
import sys

from yr.datasystem.cli.command import BaseCommand


SCHEMA_VERSION = "1.0"
MAX_ERROR_BYTES = 1024


class QueryInputError(ValueError):
    """Safe user-facing query validation error."""


def _add_backend_arguments(parser):
    parser.add_argument("--etcd_address")
    parser.add_argument("--coordinator_address")
    parser.add_argument("--cluster_name", default="")


def _validate_backend_arguments(args):
    if bool(args.etcd_address) == bool(args.coordinator_address):
        raise QueryInputError("exactly one coordination backend address is required")


def _safe_error(message):
    safe = str(message).replace("/datasystem", "<redacted-keyspace>")
    return safe.encode("utf-8")[:MAX_ERROR_BYTES].decode("utf-8", errors="ignore")


def _failure(cluster_name, status, error):
    return {
        "schema_version": SCHEMA_VERSION,
        "cluster_name": cluster_name,
        "status": status,
        "error": error,
    }


def _build_native_options(args, native):
    options = native.ClusterQueryOptions()
    options.cluster_name = args.cluster_name
    options.etcd_address = args.etcd_address or ""
    options.coordinator_address = args.coordinator_address or ""
    return options


def _write_json(payload):
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")


def _load_native():
    from yr.datasystem.lib import libds_client_py

    return libds_client_py


class Command(BaseCommand):
    """Query raw coordination facts and project them locally."""

    name = "query"
    description = "query cluster topology or key routes"

    @staticmethod
    def add_arguments(parser):
        subparsers = parser.add_subparsers(dest="query_command")
        cluster = subparsers.add_parser("cluster", allow_abbrev=False)
        _add_backend_arguments(cluster)
        route = subparsers.add_parser("route", allow_abbrev=False)
        _add_backend_arguments(route)
        route.add_argument("--keys", nargs="+")

    def run(self, args):
        cluster_name = getattr(args, "cluster_name", None)
        exit_code = self.FAILURE
        try:
            if args.query_command not in ("cluster", "route"):
                raise QueryInputError("query command must be cluster or route")
            _validate_backend_arguments(args)
            if args.query_command == "route" and not args.keys:
                raise QueryInputError("--keys is required for route query")
            native = _load_native()
            options = _build_native_options(args, native)
            if args.query_command == "cluster":
                status, error, result = native.query_coordination_cluster(options)
            else:
                status, error, result = native.query_coordination_routes(options, args.keys)
            if status == "OK":
                payload = {"schema_version": SCHEMA_VERSION, "cluster_name": cluster_name, **result}
                exit_code = self.SUCCESS
            else:
                payload = _failure(cluster_name, status, _safe_error(error))
        except QueryInputError as error:
            payload = _failure(cluster_name, "Invalid parameter", _safe_error(error))
        except (OSError, RuntimeError, UnicodeError, ValueError) as error:
            payload = _failure(cluster_name, "Runtime error", _safe_error(error))
        _write_json(payload)
        return exit_code
