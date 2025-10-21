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
"""YuanRong datasystem CLI constant module."""

from enum import Enum


class ClusterConfig(Enum):
    SSH_USER_NAME = "ssh_auth.ssh_user_name"
    SSH_PRIVATE_KEY = "ssh_auth.ssh_private_key"
    WORKER_CONFIG_PATH = "worker_config_path"
    WORKER_NODES = "worker_nodes"
    WORKER_PORT = "worker_port"
