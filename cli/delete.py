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
"""Delete stale per-address cluster topology records for specified worker addresses."""

import contextlib
import json
import os
import sys
import tempfile

from yr.datasystem.cli.command import BaseCommand


SCHEMA_VERSION = "1.0"
MAX_ERROR_BYTES = 1024


class DeleteInputError(ValueError):
    """Safe user-facing delete validation error."""


def _add_backend_arguments(parser):
    parser.add_argument("--etcd_address")
    parser.add_argument("--coordinator_address")
    parser.add_argument("--cluster_name", default="")


def _validate_backend_arguments(args):
    if bool(args.etcd_address) == bool(args.coordinator_address):
        raise DeleteInputError("exactly one coordination backend address is required")


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
    options = native.ClusterAdminOptions()
    options.cluster_name = args.cluster_name
    options.etcd_address = args.etcd_address or ""
    options.coordinator_address = args.coordinator_address or ""
    return options


def _write_json(payload):
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")


@contextlib.contextmanager
def _suppress_native_stderr():
    saved_stderr = os.dup(2)
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".log", prefix="dscli_delete_")
    try:
        os.dup2(tmp_fd, 2)
        yield tmp_path
    finally:
        os.dup2(saved_stderr, 2)
        os.close(saved_stderr)
        os.close(tmp_fd)
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


def _load_native():
    from yr.datasystem.lib import libds_client_py

    return libds_client_py


class Command(BaseCommand):
    """Delete stale cluster member records from the coordination backend."""

    name = "delete"
    description = "delete cluster member records from coordination backend"

    @staticmethod
    def add_arguments(parser):
        subparsers = parser.add_subparsers(dest="delete_command", required=True)

        cluster = subparsers.add_parser("cluster", allow_abbrev=False)
        _add_backend_arguments(cluster)
        cluster.add_argument("--worker_address", action="append", required=True,
                             help="worker address to clean up, e.g. 7.218.76.39:20010")

    def run(self, args):
        cluster_name = getattr(args, "cluster_name", "")
        exit_code = self.FAILURE
        try:
            if args.delete_command != "cluster":
                raise DeleteInputError("delete command must be cluster")
            _validate_backend_arguments(args)
            if not args.worker_address:
                raise DeleteInputError("--worker_address is required for cluster delete")
            native = _load_native()
            options = _build_native_options(args, native)
            with _suppress_native_stderr() as tmp_log_path:
                status, error, deleted_members = native.delete_cluster_members(options, args.worker_address)
                if status != "OK":
                    with open(tmp_log_path, "r", errors="ignore") as log_file:
                        captured = log_file.read().strip()
                    if captured:
                        error = f"{error}\n--- native framework logs ---\n{captured}"
            if status == "OK":
                payload = {
                    "schema_version": SCHEMA_VERSION,
                    "cluster_name": cluster_name,
                    "status": "OK",
                    "deleted_members": deleted_members,
                }
                exit_code = self.SUCCESS
            else:
                payload = _failure(cluster_name, status, _safe_error(error))
        except DeleteInputError as error:
            payload = _failure(cluster_name, "Invalid parameter", _safe_error(error))
        except (OSError, RuntimeError, UnicodeError) as error:
            payload = _failure(cluster_name, "Runtime error", _safe_error(error))
        _write_json(payload)
        return exit_code
