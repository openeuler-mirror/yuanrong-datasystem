#!/bin/bash
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

set -e
BASE_DIR=$(
  cd "$(dirname "$0")"
  pwd
)
INPUT_DIR="${BASE_DIR}/../../output"
BUILD_DIR="${BASE_DIR}/build"

ARCHITECTURE="x86"
USER_ID="1002"
IMAGE_TAG=2.2
IMAGE_NAME="datasystem"
TARGET_SYSTEM="ubuntu"
BASIC_IMAGE_NAME_TAG="ubuntu:18.04"

# ----------------------------------------------------------------------
# funcname:     log_error.
# description:  Print build error log.
# parameters:   NA
# return value: NA
# ----------------------------------------------------------------------
log_error() {
  echo "[BUILD_ERROR][$(date +%b\ %d\ %H:%M:%S)]$*"
}

# ----------------------------------------------------------------------
# funcname:     die.
# description:  Print build error log.
# parameters:   NA
# return value: NA
# ----------------------------------------------------------------------
die() {
  log_error "$*"
  stty echo
  exit 1
}

function usage() {
  echo "Usage:"
  echo "build.sh [-n] [-t] [-s] [-b] [-u] [-i] [-o]"
  echo ""
  echo "Options:"
  echo "    -a <CPU ARCHITECTURE> CPU Artitecture, default is ${ARCHITECTURE}, The other option is arm"
  echo "    -n <NAME> Image name of datasystem, default is ${IMAGE_NAME}"
  echo "    -t <TAG> Image tag of datasystem, default is ${IMAGE_TAG}"
  echo "    -s <OS NAME> Specify the type of operating system to compile, choice from: ubuntu/openeuler/euler/centos"
  echo "    -b <IMAGE NAME:TAG> Specifies a basic image name and tag. It must exist locally or in the docker hub, such as -b ubuntu:18.04 "
  echo "    -u <UID> Specifies the image user id and group id, default is ${USER_ID}"
  echo "    -i <INPUT DIR> Specifies the path of the binary files and so files required for building the data system image, default is ${INPUT_DIR}"
  echo "    -o <OUTPUT DIR> Specifies the output path of the data system image, default is ${BUILD_DIR}"
  echo "    -h Show usage"
  exit 1
}

function image_build() {
  # Prepare image components and Dockerfile
  [[ -d "${BUILD_DIR}" ]] && rm -rf "${BUILD_DIR}"
  mkdir -p "${BUILD_DIR}/bin"
  mkdir -p "${BUILD_DIR}/lib"
  cp -ar "${INPUT_DIR}/service/datasystem_worker" "${BUILD_DIR}/bin"
  cp -ar "${INPUT_DIR}/service/lib"/* "${BUILD_DIR}/lib"

  cp -ar "${BASE_DIR}/entrypoint/worker_entry.sh" "${BUILD_DIR}"
  cp -ar "${BASE_DIR}/entrypoint/install.sh" "${BUILD_DIR}"
  cp -ar "${BASE_DIR}/entrypoint/liveness_check.sh" "${BUILD_DIR}"
  cp -ar "${BASE_DIR}/entrypoint/check_taint.sh" "${BUILD_DIR}"
  cp -ar "${BASE_DIR}/entrypoint/file_check.sh" "${BUILD_DIR}"
  cp -ar "${BASE_DIR}/entrypoint/utils.sh" "${BUILD_DIR}"
  cp -ar "${BASE_DIR}/exitpoint/uninstall.sh" "${BUILD_DIR}"

  cp -ar "${BASE_DIR}/dockerfile/datasystem.Dockerfile" "${BUILD_DIR}"

  cd "${BUILD_DIR}" || die "${BUILD_DIR} not exist"

  local dockerfilename
  dockerfilename="datasystem.Dockerfile"
  if ! docker build --network host \
                    --build-arg no_proxy \
                    --build-arg DS_BASE_IMAGE="${BASIC_IMAGE_NAME_TAG}" \
                    --build-arg TARGET_SYSTEM="${TARGET_SYSTEM}" \
                    --build-arg ARCHITECTURE="${ARCHITECTURE}" \
                    --build-arg UID="${USER_ID}" \
                    -t "${IMAGE_NAME}":"${IMAGE_TAG}" \
                    -f "${dockerfilename}" .; then
    log_error "Failed to docker build datasystem!!!"
    exit 1
  fi

  local save_name
  save_name=$(echo ${IMAGE_NAME} | awk '{gsub(/\//, "_");print}')
  if ! docker save -o "${BUILD_DIR}/${save_name}_${IMAGE_TAG}".tar "${IMAGE_NAME}":"${IMAGE_TAG}"; then
    log_error "Failed to docker save datasystem!!!"
    exit 1
  fi
}

function check_system() {
  if [[ "X$1" != "Xubuntu" && "X$1" != "Xeuler" && "X$1" != "Xopeneuler" && "X$1" != "Xcentos" ]]; then
    echo -e "Invalid value $1 for option -$2"
    echo -e "${USAGE}"
    exit 1
  fi
}

function main() {
  while getopts 'h:a:c:n:t:s:u:b:i:o:' OPT; do
    case "${OPT}" in
    a) ARCHITECTURE="${OPTARG}" ;;
    n) IMAGE_NAME="${OPTARG}" ;;
    t) IMAGE_TAG="${OPTARG}" ;;
    s)
      TARGET_SYSTEM="${OPTARG}"
      check_system "${OPTARG}" s
      ;;
    b) BASIC_IMAGE_NAME_TAG="${OPTARG}" ;;
    i) INPUT_DIR="${OPTARG}" ;;
    u) USER_ID="${OPTARG}" ;;
    o) BUILD_DIR="${OPTARG}" ;;
    h) usage ;;
    ?) usage ;;
    esac
  done

  echo "----- build datasystem images in ${BASE_DIR}"
  image_build
}

main "$@"
