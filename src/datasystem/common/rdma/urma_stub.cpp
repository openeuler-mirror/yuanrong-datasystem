/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: stub functions for client side linking.
 */
#include "datasystem/common/rdma/urma_manager.h"
namespace datasystem {
__attribute__((weak)) UrmaManager::UrmaManager() = default;
__attribute__((weak)) UrmaManager::~UrmaManager() = default;

Status __attribute__((weak)) UrmaManager::ExchangeJfr(const UrmaHandshakeReqPb &req, UrmaHandshakeRspPb &rsp)
{
    (void)req;
    (void)rsp;
    return Status::OK();
}

std::vector<uint32_t> __attribute__((weak)) UrmaManager::GetJfrIds()
{
    return std::vector<uint32_t>();
}

Status __attribute__((weak)) UrmaManager::ImportRemoteJfr(const UrmaJfrInfo &urmaInfo)
{
    (void)urmaInfo;
    return Status::OK();
}

uint64_t __attribute__((weak)) UrmaManager::GetUasid()
{
    return 0;
}

std::string __attribute__((weak)) UrmaManager::GetEid()
{
    return "";
}

Status __attribute__((weak)) UrmaManager::StrToEid(const std::string &eid, urma_eid_t &out)
{
    (void)eid;
    (void)out;
    return Status::OK();
}

UrmaManager __attribute__((weak)) &UrmaManager::Instance()
{
    static UrmaManager manager;
    return manager;
}

__attribute__((weak)) Segment::~Segment() = default;
__attribute__((weak)) RemoteDevice::~RemoteDevice() = default;
}  // namespace datasystem
