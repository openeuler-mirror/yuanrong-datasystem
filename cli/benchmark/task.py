# Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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

import shlex
import subprocess
from abc import ABC, abstractmethod
from argparse import Namespace
from dataclasses import dataclass
from typing import Any, Optional, Union


@dataclass
class BenchArgs:
    name: str
    start_time: str
    cwd: str
    log_dir: str
    result_csv_file: str
    args: Namespace


@dataclass
class BenchRemoteInfo:
    host: str
    username: str
    ssh_config_path: str
    ssh_port: int


@dataclass
class BenchCommandOutput:
    stdout: str
    stderr: str


class BenchTask(ABC):
    @abstractmethod
    def run(self):
        """Executes the main logic associated with this task."""
        pass


class BenchCommandTask(BenchTask):
    command: str
    env: dict
    remote: Union[BenchRemoteInfo, None]
    output: Union[BenchCommandOutput, None]

    worker_address: Optional[str]

    def __init__(
        self,
        command: str,
        env: Optional[dict[str, str]],
        remote: Optional[BenchRemoteInfo],
        worker_address: Optional[str] = None,
    ):
        self.command = command
        self.env = env
        self.remote = remote

        self.worker_address = worker_address

    @classmethod
    def concat_args(cls, prefix: str, args: dict[str, Any]) -> str:
        """Constructs a command-line string from a dictionary of arguments."""
        args_list = []
        for key, val in args.items():
            if val is None:
                continue
            if isinstance(val, str):
                if len(val) == 0:
                    continue
                args_list.append(f"{prefix}{key}={shlex.quote(val)}")
            elif isinstance(val, bool):
                args_list.append(f"{prefix}{key}={str(val).lower()}")
            elif isinstance(val, int):
                args_list.append(f"{prefix}{key}={val}")
            else:
                raise RuntimeError(f"unknown type of key {key}")
        return " ".join(args_list)

    def execute_command_locally(self):
        """Executes a command on the local machine and captures its output."""
        env = self.env or {}
        process = subprocess.Popen(
            self.command,
            shell=True,
            env=env,
            executable="/bin/bash",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            stdout, stderr = process.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            process.kill()
            error_msg = f"Local command execution timed out: '{self.command}'"
            raise RuntimeError(error_msg) from None
        return stdout.decode("utf-8"), stderr.decode("utf-8")

    def execute_command_remotely(self, remote: BenchRemoteInfo):
        """Executes a command on a remote machine via SSH and captures its output."""
        new_command = self.command
        if self.env is not None:
            new_command = f"{BenchCommandTask.concat_args('', self.env)} {new_command}"

        ssh_command = (
            f"ssh -q {remote.username}@{remote.host} -p {remote.ssh_port} "
            f"-i {shlex.quote(remote.ssh_config_path)} {shlex.quote(new_command)}"
        )
        process = subprocess.Popen(
            ssh_command,
            shell=True,
            env=self.env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            stdout, stderr = process.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            process.kill()
            error_msg = (
                f"Remote command execution timed out after 600 seconds. "
                f"Process has been killed. SSH command: '{ssh_command}'"
            )
            raise RuntimeError(error_msg) from None
        return stdout.decode("utf-8"), stderr.decode("utf-8")

    def get_output(self) -> Union[BenchCommandOutput, None]:
        """Retrieves the output of the executed command."""
        return self.output

    def run(self):
        if self.remote is None:
            stdout, stderr = self.execute_command_locally()
        else:
            stdout, stderr = self.execute_command_remotely(self.remote)
        self.output = BenchCommandOutput(stdout, stderr)
