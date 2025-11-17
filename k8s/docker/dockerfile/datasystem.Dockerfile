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

ARG DS_BASE_IMAGE
FROM ${DS_BASE_IMAGE}

ARG UID

ENV USER_UID="${UID}" \
    USER_NAME=sn \
    GROUP_ID="${UID}" \
    GROUP_NAME=sn \
    HOME=/home/sn

ARG DATASYSTEM_ROOT=${HOME}/datasystem
ARG TARGET_SYSTEM
ARG ARCHITECTURE

RUN sed -i 's|repo.openeuler.org|mirrors.huaweicloud.com/openeuler|g' /etc/yum.repos.d/*.repo

RUN dnf clean all && \
    dnf makecache && \
    dnf install -y shadow-utils && \
    dnf clean all

RUN mkdir -p ${DATASYSTEM_ROOT} && \
    groupadd -g ${GROUP_ID} ${GROUP_NAME} && \
    useradd -u ${USER_UID} -g ${GROUP_ID} -s /sbin/nologin ${USER_NAME} && \
    chown -R ${USER_UID}:${GROUP_ID} ${HOME} && \
    chmod 700 ${DATASYSTEM_ROOT}

COPY --chown=sn:sn ./worker_entry.sh ${HOME}/
COPY --chown=sn:sn ./uninstall.sh ${HOME}/
COPY --chown=sn:sn ./install.sh ${HOME}/
COPY --chown=sn:sn ./liveness_check.sh ${HOME}/
COPY --chown=sn:sn ./file_check.sh ${HOME}/
COPY --chown=sn:sn ./utils.sh ${HOME}/
COPY --chown=sn:sn ./check_taint.sh ${HOME}/
RUN chmod 500 ${HOME}/worker_entry.sh && \
    chmod 500 ${HOME}/liveness_check.sh && \
    chmod 500 ${HOME}/uninstall.sh && \
    chmod 500 ${HOME}/install.sh && \
    chmod 500 ${HOME}/file_check.sh && \
    chmod 500 ${HOME}/utils.sh && \
    chmod 500 ${HOME}/check_taint.sh && \
    cp /etc/skel/.bashrc ${HOME}/.bashrc && \
    chown sn:sn ${HOME}/.bashrc && \
    chmod -R 700 ${HOME}/.bashrc

# install operator binary
ADD --chown=sn:sn bin ${DATASYSTEM_ROOT}/bin
ADD --chown=sn:sn lib ${DATASYSTEM_ROOT}/lib
RUN chmod -R 500 ${DATASYSTEM_ROOT}/bin && \
    chmod 500 ${DATASYSTEM_ROOT}/lib && \
    chmod 400 ${DATASYSTEM_ROOT}/lib/*

RUN sh -c 'if [ -d "${DATASYSTEM_ROOT}/lib/urma" ]; then \
        chmod 500 ${DATASYSTEM_ROOT}/lib/urma && \
        chmod 400 ${DATASYSTEM_ROOT}/lib/urma/*; \
    fi'

RUN if [ -f /etc/sudoers ]; then \
        sed -i "s|%wheel|#%wheel|g" "/etc/sudoers"; \
    fi

USER ${USER_UID}

ENV PATH=${DATASYSTEM_ROOT}/bin:${PATH}

WORKDIR ${DATASYSTEM_ROOT}