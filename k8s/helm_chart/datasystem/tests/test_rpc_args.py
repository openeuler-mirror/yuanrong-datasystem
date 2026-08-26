#!/usr/bin/env python3
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

import os
import pathlib
import shutil
import subprocess
import unittest


CHART = pathlib.Path(__file__).resolve().parents[1]
HELM = os.environ.get("HELM_BIN") or shutil.which("helm")


@unittest.skipUnless(HELM, "helm is required")
class WorkerRpcArgsTest(unittest.TestCase):
    def render(self, use_brpc: bool) -> str:
        return subprocess.run(
            [
                HELM,
                "template",
                "datasystem",
                str(CHART),
                "--set",
                f"global.rpc.brpc.useBrpc={str(use_brpc).lower()}",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        ).stdout

    def test_zmq_server_io_context_removed(self):
        # The ZMQ RPC transport flag zmq_server_io_context was removed (ZMQ RPC
        # transport deleted). It must not appear in either brpc or legacy mode.
        self.assertNotIn("-zmq_server_io_context=", self.render(use_brpc=True))
        self.assertNotIn("-zmq_server_io_context=", self.render(use_brpc=False))

    def test_zmq_chunk_sz_removed(self):
        # The ZMQ payload split flag zmq_chunk_sz was removed with the ZMQ RPC transport.
        # The worker binary rejects it as an unknown command line flag, so it must not be
        # rendered in either brpc or legacy mode.
        self.assertNotIn("-zmq_chunk_sz=", self.render(use_brpc=True))
        self.assertNotIn("-zmq_chunk_sz=", self.render(use_brpc=False))

    def test_no_zmq_command_line_flags(self):
        # The ZMQ transport was removed; the worker binary rejects any -zmq_* flag.
        # No ZMQ-specific command line flag may be rendered in either mode.
        for use_brpc in (True, False):
            self.assertNotIn("-zmq_", self.render(use_brpc=use_brpc))


if __name__ == "__main__":
    unittest.main()
