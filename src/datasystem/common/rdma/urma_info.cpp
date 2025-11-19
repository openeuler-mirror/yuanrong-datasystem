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
    oss << "address " << localAddress.ToString() << ", eid ";
    // eid is not really printable as a string. So we will dump its context in hex
    urma_eid_t e;
    if (UrmaManager::StrToEid(eid, e).IsOk()) {
        oss << EidToFmtStr(e);
    } else {
        oss << eid;
    }
    oss << " uasid " << uasid << " jfr_id [";
    bool first = true;
    for (auto jfr_id : jfrIds) {
        if (first) {
            first = false;
        } else {
            oss << " ";
        }
        oss << jfr_id;
    }
    oss << "]";
    oss << " bondInfos [";
    first = true;
#ifdef URMA_OVER_UB
    for (auto bondInfo : bondInfos) {
        if (first) {
            first = false;
        } else {
            oss << ",";
        }
        oss << "base_id:" << EidToFmtStr(bondInfo.base_id.eid) << "+" << bondInfo.base_id.uasid << "+"
            << bondInfo.base_id.id << " ";
        for (size_t index = 0; index < URMA_UBAGG_DEV_MAX_NUM; index++) {
            if (bondInfo.slave_id[index].id > 0) {
                oss << "slave_id[" << index << "]:" << EidToFmtStr(bondInfo.slave_id[index].eid) << "+"
                    << bondInfo.slave_id[index].uasid << "+" << bondInfo.slave_id[index].id << " ";
            }
        }
        oss << "dev_num:" << bondInfo.dev_num << " ";
        oss << "is_in_matrix_server:" << bondInfo.is_in_matrix_server << " ";
        oss << "is_multipath:" << bondInfo.is_multipath;
    }
#endif
    oss << "]";
    return oss.str();
}

#ifdef URMA_OVER_UB
void UrmaJfrInfo::UrmaBondIdInfoToProto(const urma_bond_id_info_out &info, JfrBondInfo &proto)
{
    auto baseId = proto.mutable_base_id();
    baseId->set_eid(UrmaManager::EidToStr(info.base_id.eid));
    baseId->set_uasid(info.base_id.uasid);
    baseId->set_id(info.base_id.id);
    for (int index = 0; index < URMA_UBAGG_DEV_MAX_NUM; index++) {
        auto slaveId = proto.add_slave_ids();
        slaveId->set_eid(UrmaManager::EidToStr(info.slave_id[index].eid));
        slaveId->set_uasid(info.slave_id[index].uasid);
        slaveId->set_id(info.slave_id[index].id);
    }
    proto.set_dev_num(info.dev_num);
    proto.set_is_in_matrix_server(info.is_in_matrix_server);
    proto.set_is_multipath(info.is_multipath);
}

Status UrmaJfrInfo::UrmaBondIdInfoFromProto(const JfrBondInfo &proto, urma_bond_id_info_out &info)
{
    const auto &baseId = proto.base_id();
    RETURN_IF_NOT_OK(UrmaManager::StrToEid(baseId.eid(), info.base_id.eid));
    info.base_id.uasid = baseId.uasid();
    info.base_id.id = baseId.id();
    auto slaveSize = proto.slave_ids_size();
    for (int index = 0; index < slaveSize; index++) {
        const auto &slaveId = proto.slave_ids(index);
        RETURN_IF_NOT_OK(UrmaManager::StrToEid(slaveId.eid(), info.slave_id[index].eid));
        info.slave_id[index].uasid = slaveId.uasid();
        info.slave_id[index].id = slaveId.id();
    }
    info.dev_num = proto.dev_num();
    info.is_in_matrix_server = proto.is_in_matrix_server();
    info.is_multipath = proto.is_multipath();
    return Status::OK();
}
#endif

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

#ifdef URMA_OVER_UB
std::string UrmaBondSegInfo::ToString()
{
    std::stringstream ss;
    ss << "base: {" << UrmaSeg::ToString(raw.base) << "}";
    for (int index = 0; index < URMA_UBAGG_DEV_MAX_NUM; index++) {
        ss << ", slaves[" << index << "]: {" << UrmaSeg::ToString(raw.slaves[index]) << "}";
    }
    ss << ", dev_num: " << raw.dev_num;
    return ss.str();
}

void UrmaBondSegInfo::ToProto(UrmaBondSegInfoPb &proto) const
{
    auto base = proto.mutable_base();
    UrmaSeg::ToProto(raw.base, *base);
    for (int index = 0; index < URMA_UBAGG_DEV_MAX_NUM; index++) {
        auto slave = proto.add_slaves();
        UrmaSeg::ToProto(raw.slaves[index], *slave);
    }
    proto.set_dev_num(raw.dev_num);
}

Status UrmaBondSegInfo::FromProto(const UrmaBondSegInfoPb &proto)
{
    const auto &base = proto.base();
    RETURN_IF_NOT_OK(UrmaSeg::FromProto(base, raw.base));
    auto slaveSize = proto.slaves_size();
    for (int index = 0; index < slaveSize; index++) {
        const auto &slave = proto.slaves(index);
        RETURN_IF_NOT_OK(UrmaSeg::FromProto(slave, raw.slaves[index]));
    }
    raw.dev_num = proto.dev_num();
    return Status::OK();
}
#endif
}  // namespace datasystem
