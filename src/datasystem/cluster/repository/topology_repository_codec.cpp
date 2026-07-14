/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

/**
 * Description: Canonical protobuf boundary for cluster topology repository values.
 */
#include "datasystem/cluster/repository/topology_repository_codec.h"

#include <algorithm>
#include <utility>

#include "datasystem/cluster/model/topology_snapshot.h"
#include "datasystem/cluster/repository/topology_key_helper.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/cluster_topology.pb.h"
#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/io/zero_copy_stream_impl_lite.h"

namespace datasystem::cluster {
namespace {
constexpr size_t MAX_TASK_RANGES = 4'096;
constexpr size_t MAX_VALUE_BYTES = 4 * 1024 * 1024;
constexpr size_t MAX_NOTIFY_TASK_REFS = 4'096;
constexpr size_t MAX_NOTIFY_VALUE_BYTES = 256 * 1024;
constexpr size_t MAX_TOPOLOGY_MEMBERS = 10'000;
constexpr size_t MAX_TOPOLOGY_TOKENS = 40'000;
constexpr size_t SHA256_HEX_SIZE = 64;
constexpr char SCHEMA_VERSION[] = "1";
const std::string VALIDATION_DIGEST(SHA256_HEX_SIZE, '0');

Status SerializeCanonical(const google::protobuf::MessageLite &message, std::string &value)
{
    std::string encoded;
    {
        google::protobuf::io::StringOutputStream output(&encoded);
        google::protobuf::io::CodedOutputStream coded(&output);
        coded.SetSerializationDeterministic(true);
        CHECK_FAIL_RETURN_STATUS(message.SerializeToCodedStream(&coded) && !coded.HadError(), K_RUNTIME_ERROR,
                                 "serialize repository value failed");
    }
    CHECK_FAIL_RETURN_STATUS(encoded.size() <= MAX_VALUE_BYTES, K_INVALID, "repository value exceeds limit");
    value = std::move(encoded);
    return Status::OK();
}

Status ToPbType(TopologyChangeType type, ::datasystem::TypePb &output)
{
    switch (type) {
        case TopologyChangeType::SCALE_OUT:
            output = ::datasystem::SCALE_OUT;
            return Status::OK();
        case TopologyChangeType::SCALE_IN:
            output = ::datasystem::SCALE_IN;
            return Status::OK();
        case TopologyChangeType::FAILURE:
            output = ::datasystem::FAILURE;
            return Status::OK();
    }
    RETURN_STATUS(K_INVALID, "invalid topology change type");
}

Status FromPbType(::datasystem::TypePb type, TopologyChangeType &output)
{
    switch (type) {
        case ::datasystem::SCALE_OUT:
            output = TopologyChangeType::SCALE_OUT;
            return Status::OK();
        case ::datasystem::SCALE_IN:
            output = TopologyChangeType::SCALE_IN;
            return Status::OK();
        case ::datasystem::FAILURE:
            output = TopologyChangeType::FAILURE;
            return Status::OK();
        default:
            RETURN_STATUS(K_INVALID, "invalid topology change type");
    }
}

Status ValidateTaskId(const std::string &taskId)
{
    std::string key;
    return TopologyKeyHelper::TaskKey(taskId, key);
}

Status ValidateTaskContext(const std::string &taskId, char kind, uint64_t epoch)
{
    RETURN_IF_NOT_OK(ValidateTaskId(taskId));
    const std::string prefix{ kind, '-', 'e' };
    const std::string expected = prefix + std::to_string(epoch) + "-";
    CHECK_FAIL_RETURN_STATUS(taskId.rfind(expected, 0) == 0, K_INVALID, "task id context mismatch");
    return Status::OK();
}

Status ValidateMemberAddress(const std::string &address)
{
    std::string key;
    return TopologyKeyHelper::NotifyKey(address, key);
}

bool IsCanonicalRanges(const std::vector<TopologyTaskRange> &ranges, const std::string &executor)
{
    if (ranges.empty() || ranges.size() > MAX_TASK_RANGES || executor.empty()) {
        return false;
    }
    for (size_t index = 0; index < ranges.size(); ++index) {
        const auto &range = ranges[index];
        if (range.ownerAddress != executor || range.range.from > range.range.end) {
            return false;
        }
        if (index > 0 && ranges[index - 1].range.end >= range.range.from) {
            return false;
        }
    }
    return true;
}

void EncodeRanges(const std::vector<TopologyTaskRange> &ranges,
                  google::protobuf::RepeatedPtrField<::datasystem::TokenRangePb> *output)
{
    for (const auto &range : ranges) {
        auto *rangePb = output->Add();
        rangePb->set_owner_address(range.ownerAddress);
        rangePb->set_from(range.range.from);
        rangePb->set_end(range.range.end);
        rangePb->set_finished(range.finished);
    }
}

Status DecodeRanges(const google::protobuf::RepeatedPtrField<::datasystem::TokenRangePb> &input,
                    std::vector<TopologyTaskRange> &ranges, std::string &executor)
{
    CHECK_FAIL_RETURN_STATUS(!input.empty() && static_cast<size_t>(input.size()) <= MAX_TASK_RANGES, K_INVALID,
                             "invalid task range count");
    std::vector<TopologyTaskRange> decoded;
    decoded.reserve(input.size());
    for (const auto &rangePb : input) {
        decoded.emplace_back(
            TopologyTaskRange{ rangePb.owner_address(), { rangePb.from(), rangePb.end() }, rangePb.finished() });
    }
    CHECK_FAIL_RETURN_STATUS(!decoded.empty(), K_INVALID, "task has no ranges");
    const std::string owner = decoded.front().ownerAddress;
    CHECK_FAIL_RETURN_STATUS(IsCanonicalRanges(decoded, owner), K_INVALID, "invalid task ranges");
    ranges = std::move(decoded);
    executor = owner;
    return Status::OK();
}
}  // namespace

Status TopologyRepositoryCodec::EncodeTopology(const TopologyState &state, std::string &value)
{
    std::shared_ptr<const TopologySnapshot> snapshot;
    RETURN_IF_NOT_OK(TopologySnapshot::Create(state, 0, VALIDATION_DIGEST, snapshot));
    ::datasystem::ClusterTopologyPb pb;
    pb.set_cluster_has_init(snapshot->ClusterHasInit());
    pb.set_version(snapshot->Version());
    pb.set_schema_version(SCHEMA_VERSION);
    for (const auto &member : snapshot->Members()) {
        auto &memberPb = (*pb.mutable_members())[member.identity.address];
        memberPb.set_id(member.identity.id);
        memberPb.set_state(static_cast<::datasystem::MembershipPb::StatePb>(member.state));
        for (uint32_t token : member.tokens) {
            memberPb.add_tokens(token);
        }
    }
    if (snapshot->GetActiveBatch().has_value()) {
        ::datasystem::TypePb typePb;
        RETURN_IF_NOT_OK(ToPbType(snapshot->GetActiveBatch()->type, typePb));
        pb.mutable_active_batch()->set_type(typePb);
        pb.mutable_active_batch()->set_epoch(snapshot->GetActiveBatch()->epoch);
    }
    return SerializeCanonical(pb, value);
}

Status TopologyRepositoryCodec::DecodeTopology(const std::string &value, TopologyState &state)
{
    CHECK_FAIL_RETURN_STATUS(value.size() <= MAX_VALUE_BYTES, K_INVALID, "topology value exceeds limit");
    ::datasystem::ClusterTopologyPb pb;
    CHECK_FAIL_RETURN_STATUS(pb.ParseFromString(value) && pb.schema_version() == SCHEMA_VERSION, K_INVALID,
                             "invalid topology value");
    CHECK_FAIL_RETURN_STATUS(static_cast<size_t>(pb.members_size()) <= MAX_TOPOLOGY_MEMBERS, K_INVALID,
                             "topology member count exceeds limit");
    size_t tokenCount = 0;
    for (const auto &[address, memberPb] : pb.members()) {
        static_cast<void>(address);
        tokenCount += static_cast<size_t>(memberPb.tokens_size());
        CHECK_FAIL_RETURN_STATUS(tokenCount <= MAX_TOPOLOGY_TOKENS, K_INVALID, "topology token count exceeds limit");
    }
    TopologyState decoded;
    decoded.clusterHasInit = pb.cluster_has_init();
    decoded.version = pb.version();
    decoded.members.reserve(static_cast<size_t>(pb.members_size()));
    for (const auto &[address, memberPb] : pb.members()) {
        const int stateValue = static_cast<int>(memberPb.state());
        CHECK_FAIL_RETURN_STATUS(stateValue >= static_cast<int>(::datasystem::MembershipPb::INITIAL)
                                     && stateValue <= static_cast<int>(::datasystem::MembershipPb::FAILED),
                                 K_INVALID, "invalid member state");
        Member member{ { memberPb.id(), address }, static_cast<MemberState>(memberPb.state()), {} };
        member.tokens.assign(memberPb.tokens().begin(), memberPb.tokens().end());
        decoded.members.emplace_back(std::move(member));
    }
    if (pb.has_active_batch()) {
        TopologyChangeType type;
        RETURN_IF_NOT_OK(FromPbType(pb.active_batch().type(), type));
        decoded.activeBatch = ActiveBatch{ type, pb.active_batch().epoch() };
    }
    std::shared_ptr<const TopologySnapshot> validated;
    RETURN_IF_NOT_OK(TopologySnapshot::Create(decoded, 0, VALIDATION_DIGEST, validated));
    decoded.members = validated->Members();
    state = std::move(decoded);
    return Status::OK();
}

Status TopologyRepositoryCodec::EncodeMigrateTask(const TopologyMigrateTask &task, std::string &value)
{
    RETURN_IF_NOT_OK(ValidateTaskContext(task.taskId, 'm', task.epoch));
    RETURN_IF_NOT_OK(ValidateMemberAddress(task.targetAddress));
    RETURN_IF_NOT_OK(ValidateMemberAddress(task.executorAddress));
    CHECK_FAIL_RETURN_STATUS(task.epoch > 0 && IsCanonicalRanges(task.sourceRanges, task.executorAddress), K_INVALID,
                             "invalid migrate task");
    ::datasystem::MigrateTaskPb pb;
    pb.set_target_address(task.targetAddress);
    EncodeRanges(task.sourceRanges, pb.mutable_source_ranges());
    return SerializeCanonical(pb, value);
}

Status TopologyRepositoryCodec::DecodeMigrateTask(const std::string &taskId, TopologyChangeType type, uint64_t epoch,
                                                  const std::string &value, TopologyMigrateTask &task)
{
    RETURN_IF_NOT_OK(ValidateTaskContext(taskId, 'm', epoch));
    CHECK_FAIL_RETURN_STATUS(epoch > 0 && value.size() <= MAX_VALUE_BYTES, K_INVALID, "invalid task context");
    ::datasystem::MigrateTaskPb pb;
    CHECK_FAIL_RETURN_STATUS(pb.ParseFromString(value) && !pb.target_address().empty(), K_INVALID,
                             "invalid migrate task value");
    RETURN_IF_NOT_OK(ValidateMemberAddress(pb.target_address()));
    TopologyMigrateTask decoded{ taskId, type, epoch, "", pb.target_address(), {} };
    RETURN_IF_NOT_OK(DecodeRanges(pb.source_ranges(), decoded.sourceRanges, decoded.executorAddress));
    task = std::move(decoded);
    return Status::OK();
}

Status TopologyRepositoryCodec::EncodeDeleteTask(const TopologyDeleteTask &task, std::string &value)
{
    RETURN_IF_NOT_OK(ValidateTaskContext(task.taskId, 'd', task.epoch));
    RETURN_IF_NOT_OK(ValidateMemberAddress(task.failedAddress));
    RETURN_IF_NOT_OK(ValidateMemberAddress(task.executorAddress));
    CHECK_FAIL_RETURN_STATUS(task.epoch > 0 && IsCanonicalRanges(task.recoveryRanges, task.executorAddress), K_INVALID,
                             "invalid delete task");
    ::datasystem::DeleteNodeTaskPb pb;
    pb.set_failed_address(task.failedAddress);
    EncodeRanges(task.recoveryRanges, pb.mutable_recovery_ranges());
    return SerializeCanonical(pb, value);
}

Status TopologyRepositoryCodec::DecodeDeleteTask(const std::string &taskId, uint64_t epoch, const std::string &value,
                                                 TopologyDeleteTask &task)
{
    RETURN_IF_NOT_OK(ValidateTaskContext(taskId, 'd', epoch));
    CHECK_FAIL_RETURN_STATUS(epoch > 0 && value.size() <= MAX_VALUE_BYTES, K_INVALID, "invalid task context");
    ::datasystem::DeleteNodeTaskPb pb;
    CHECK_FAIL_RETURN_STATUS(pb.ParseFromString(value) && !pb.failed_address().empty(), K_INVALID,
                             "invalid delete task value");
    RETURN_IF_NOT_OK(ValidateMemberAddress(pb.failed_address()));
    TopologyDeleteTask decoded{ taskId, epoch, "", pb.failed_address(), {} };
    RETURN_IF_NOT_OK(DecodeRanges(pb.recovery_ranges(), decoded.recoveryRanges, decoded.executorAddress));
    task = std::move(decoded);
    return Status::OK();
}

Status TopologyRepositoryCodec::EncodeNotify(const TopologyTaskNotify &notify, std::string &value)
{
    auto duplicate = std::adjacent_find(notify.taskIds.begin(), notify.taskIds.end());
    CHECK_FAIL_RETURN_STATUS(!notify.taskIds.empty() && std::is_sorted(notify.taskIds.begin(), notify.taskIds.end())
                                 && duplicate == notify.taskIds.end() && notify.taskIds.size() <= MAX_NOTIFY_TASK_REFS,
                             K_INVALID, "invalid notify task ids");
    ::datasystem::TypePb typePb;
    RETURN_IF_NOT_OK(ToPbType(notify.type, typePb));
    ::datasystem::TaskNotifyPb pb;
    pb.set_type(typePb);
    for (const auto &taskId : notify.taskIds) {
        RETURN_IF_NOT_OK(ValidateTaskId(taskId));
        const std::string prefix = notify.type == TopologyChangeType::FAILURE ? "d-e" : "m-e";
        CHECK_FAIL_RETURN_STATUS(taskId.rfind(prefix, 0) == 0, K_INVALID, "notify task kind mismatch");
        pb.add_task_ids(taskId);
    }
    std::string encoded;
    RETURN_IF_NOT_OK(SerializeCanonical(pb, encoded));
    CHECK_FAIL_RETURN_STATUS(encoded.size() <= MAX_NOTIFY_VALUE_BYTES, K_INVALID, "notify value exceeds limit");
    value = std::move(encoded);
    return Status::OK();
}

Status TopologyRepositoryCodec::DecodeNotify(const std::string &value, TopologyTaskNotify &notify)
{
    CHECK_FAIL_RETURN_STATUS(value.size() <= MAX_NOTIFY_VALUE_BYTES, K_INVALID, "notify value exceeds limit");
    ::datasystem::TaskNotifyPb pb;
    CHECK_FAIL_RETURN_STATUS(pb.ParseFromString(value), K_INVALID, "invalid notify value");
    TopologyTaskNotify decoded;
    RETURN_IF_NOT_OK(FromPbType(pb.type(), decoded.type));
    decoded.taskIds.assign(pb.task_ids().begin(), pb.task_ids().end());
    std::string canonical;
    RETURN_IF_NOT_OK(EncodeNotify(decoded, canonical));
    CHECK_FAIL_RETURN_STATUS(canonical == value, K_INVALID, "notify value is not canonical");
    notify = std::move(decoded);
    return Status::OK();
}

}  // namespace datasystem::cluster
