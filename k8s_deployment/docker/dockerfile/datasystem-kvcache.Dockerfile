# Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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

######################## Usage ########################
 # docker build --build-arg no_proxy \
 #              --build-arg DS_BASE_IMAGE=xxx \
 #              --build-arg CONFIG_NAME=worker.config \
 #              -t datasystem:version \
 #              -f datasystem-kvcache.Dockerfile .
#######################################################

ARG DS_BASE_IMAGE
FROM ${DS_BASE_IMAGE}

ARG CONFIG_NAME

ENV KVCACHE_DIR=/home/kvcache \
    CONFIG_FILE=/home/kvcache/${CONFIG_NAME}

######################## Modify yum source ########################
RUN sed -i 's|repo.openeuler.org|mirrors.huaweicloud.com/openeuler|g' /etc/yum.repos.d/*.repo
RUN sed -i '/^metalink=/d' /etc/yum.repos.d/*.repo

######################## Install datasystem and generate config ########################
RUN mkdir -p ${KVCACHE_DIR} && \
    mkdir -p ${KVCACHE_DIR}/docker

COPY ./openyuanrong_datasystem-*.whl ${KVCACHE_DIR}
COPY ./${CONFIG_NAME} ${KVCACHE_DIR}

RUN python3 -m pip install ${KVCACHE_DIR}/openyuanrong_datasystem-*.whl --force-reinstall && \
    rm ${KVCACHE_DIR}/openyuanrong_datasystem-*.whl && \
    dscli generate_docker_entryfile -o ${KVCACHE_DIR}/docker

WORKDIR ${KVCACHE_DIR}

ENTRYPOINT ${KVCACHE_DIR}/docker/worker_entry.sh
