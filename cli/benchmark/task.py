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
import logging
from abc import ABC, abstractmethod
from argparse import Namespace
from dataclasses import dataclass
from typing import Any, Optional, Union

logger = logging.getLogger("dsbench")


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
    def run(self, handler=None):
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
                if val:
                    args_list.append(f"{prefix}{key}")
            elif isinstance(val, int):
                args_list.append(f"{prefix}{key}={val}")
            else:
                raise RuntimeError(f"unknown type of key {key}")
        return " ".join(args_list)

    def execute_command_locally(self):
        """Executes a command on the local machine and captures its output."""
        env = self.env or {}

        logger.debug("Executing local command: %s", self.command)
        logger.debug("Local command environment: %s", env)

        process = subprocess.Popen(
            shlex.split(self.command),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            stdout, stderr = process.communicate(timeout=300)
        except subprocess.TimeoutExpired:
            process.kill()
            error_msg = f"Local command execution timed out: '{self.command}'"
            logger.error(error_msg)
            raise RuntimeError(error_msg) from None

        stdout_str = stdout.decode("utf-8")
        stderr_str = stderr.decode("utf-8")

        logger.debug("Local command stdout: %s", stdout_str)
        logger.debug("Local command stderr: %s", stderr_str)
        logger.debug("Local command return code: %d", process.returncode)

        # Log error if return code is not 0 and stderr is not empty
        if process.returncode != 0 and stderr_str.strip():
            logger.error("Local command failed with return code %d. Error: %s", process.returncode, stderr_str.strip())
        return stdout_str, stderr_str

    def execute_command_remotely(self, remote: BenchRemoteInfo):
        """Executes a command on a remote machine via SSH and captures its output."""

        logger.debug("Executing remote command on %s", remote.host)
        logger.debug("Original remote command: %s", self.command)
        logger.debug("Remote command environment: %s", self.env)

        new_command = self.command
        if self.env is not None:
            new_command = f"{BenchCommandTask.concat_args('', self.env)} {new_command}"

        ssh_command = (
            f"ssh -q {remote.username}@{remote.host} "
            f"-i {shlex.quote(remote.ssh_config_path)} {shlex.quote(new_command)}"
        )
        logger.debug("SSH command: %s", ssh_command)

        process = subprocess.Popen(
            shlex.split(ssh_command),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            stdout, stderr = process.communicate(timeout=300)
        except subprocess.TimeoutExpired:
            process.kill()
            error_msg = (
                f"Remote command execution timed out after 600 seconds. "
                f"Process has been killed. SSH command: '{ssh_command}'"
            )
            logger.error(error_msg)
            raise RuntimeError(error_msg) from None

        stdout_str = stdout.decode("utf-8")
        stderr_str = stderr.decode("utf-8")

        logger.debug("Remote command stdout: %s", stdout_str)
        logger.debug("Remote command stderr: %s", stderr_str)
        logger.debug("Remote command return code: %d", process.returncode)

        # Log error if return code is not 0 and stderr is not empty
        if process.returncode != 0 and stderr_str.strip():
            logger.error("Remote command failed with return code %d. Error: %s", process.returncode, stderr_str.strip())
        return stdout_str, stderr_str

    def get_output(self) -> Union[BenchCommandOutput, None]:
        """Retrieves the output of the executed command."""
        return self.output

    def execute(self):
        """Executes the command either locally or remotely."""
        logger.debug("Running command task for address: %s", self.worker_address)

        if self.remote is None:
            stdout, stderr = self.execute_command_locally()
        else:
            stdout, stderr = self.execute_command_remotely(self.remote)

        self.output = BenchCommandOutput(stdout, stderr)
        logger.debug("Command task completed for address: %s", self.worker_address)

    def run(self, handler=None):
        if handler:
            handler.before_execute(self)
        self.execute()
        if handler:
            handler.after_execute(self)


class BenchParallelCommandTask(BenchTask):
    def __init__(self, tasks: list[BenchCommandTask]):
        self.tasks = tasks

    def run(self, handler=None):
        import concurrent.futures

        if not self.tasks:
            return

        # Create a new thread pool each time run is called
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.tasks)) as executor:
            # Submit all tasks for execution with handler before_execute calls
            futures = []
            for task in self.tasks:
                if handler:
                    handler.before_execute(task)
                futures.append(executor.submit(task.run))

            # Wait for all futures to complete and call handler after_execute
            for future, task in zip(futures, self.tasks):
                future.result()  # This will propagate any exceptions
                if handler:
                    handler.after_execute(task)
