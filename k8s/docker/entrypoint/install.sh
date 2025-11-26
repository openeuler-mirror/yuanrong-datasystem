#!/bin/bash
# Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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

set -e

typeset bashrc_file="${HOME}/.connect_bashrc"

if [ ! -f "${bashrc_file}" ];then
 echo "" > ${bashrc_file}
fi

#delete
sed -i "/alias log=/d" ${bashrc_file}
sed -i "/alias bin=/d" ${bashrc_file}
sed -i "/alias ll=/d" ${bashrc_file}

#append
sed -i "\$a\alias log='cd ${WORKER_LOG_DIR}'" ${bashrc_file}
sed -i "\$a\alias bin='cd /home/yuanrong/datasystem/bin'" ${bashrc_file}
sed -i "\$a\alias ll='ls -lrta --color=auto'" ${bashrc_file}

sed -i '/source ~\/.connect_bashrc/d' ${HOME}/.bashrc
sed -i "\$a\source ~\/.connect_bashrc" ${HOME}/.bashrc
source /home/yuanrong/.bashrc
chmod 600 ${bashrc_file}