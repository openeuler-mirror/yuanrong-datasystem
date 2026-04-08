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
#include "datasystem/common/rdma/urma_info.h"

#include "datasystem/common/rdma/urma_manager.h"
#include "datasystem/utils/status.h"

namespace datasystem {

std::string UrmaJfrInfo::EidToFmtStr(const urma_eid_t &eid)
{
    char s[URMA_EID_STR_LEN + 1] = { 0 };
    int ret = sprintf_s(s, URMA_EID_STR_LEN + 1, EID_FMT, EID_ARGS(eid));
    return ret == -1 ? "invalid" : s;
}

std::string UrmaJfrInfo::ToString() const
{
    std::stringstream oss;
    if (!uniqueInstanceId.empty()) {
        oss << "Instance id " << uniqueInstanceId << ", ";
    }
    oss << "address " << localAddress.ToString() << ", ";
    if (!clientId.empty()) {
        oss << "client_id " << clientId << ", ";
    }
    oss << "eid ";
    // eid is not really printable as a string. So we will dump its context in hex
    urma_eid_t e;
    if (UrmaManager::StrToEid(eid, e).IsOk()) {
        oss << EidToFmtStr(e);
    } else {
        oss << eid;
    }
    oss << " uasid " << uasid << ", jfr_id " << jfrId;
    return oss.str();
}

void UrmaSeg::ToProto(const urma_seg_t &seg, UrmaSegPb &proto)
{
    proto.set_eid(UrmaManager::EidToStr(seg.ubva.eid));
    proto.set_uasid(seg.ubva.uasid);
    proto.set_va(seg.ubva.va);
    proto.set_len(seg.len);
    proto.set_attr(seg.attr.value);
    proto.set_token_id(seg.token_id);
}

Status UrmaSeg::FromProto(const UrmaSegPb &proto, urma_seg_t &seg)
{
    urma_eid_t eid;
    RETURN_IF_NOT_OK(UrmaManager::StrToEid(proto.eid(), eid));
    seg.ubva.eid = eid;
    seg.ubva.uasid = proto.uasid();
    seg.ubva.va = proto.va();
    seg.len = proto.len();
    seg.attr.value = proto.attr();
    seg.token_id = proto.token_id();
    return Status::OK();
}

std::string UrmaSeg::ToString(const urma_seg_t &seg)
{
    std::stringstream ss;
    ss << "ubva: { eid: " << UrmaJfrInfo::EidToFmtStr(seg.ubva.eid);
    ss << ", uasid: " << seg.ubva.uasid;
    ss << ", va: " << seg.ubva.va;
    ss << "}, len: " << seg.len;
    ss << ", attr: " << seg.attr.value;
    ss << ", token_id: " << seg.token_id;
    return ss.str();
}

std::string UrmaSeg::ToString()
{
    return UrmaSeg::ToString(raw);
}

void UrmaSeg::ToProto(UrmaSegPb &proto) const
{
    UrmaSeg::ToProto(raw, proto);
}

Status UrmaSeg::FromProto(const UrmaSegPb &proto)
{
    return UrmaSeg::FromProto(proto, raw);
}
}  // namespace datasystem
