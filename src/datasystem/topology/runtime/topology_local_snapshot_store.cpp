/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
 * Description: Local topology snapshot store.
 */
#include "datasystem/topology/runtime/topology_local_snapshot_store.h"

#include <cctype>
#include <limits>
#include <map>
#include <sstream>
#include <utility>

#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/topology/repository/topology_repository_codec.h"
#include "datasystem/topology/runtime/topology_state_digest.h"

namespace datasystem {
namespace topology {
namespace {

constexpr char SNAPSHOT_SCHEMA_VERSION[] = "1";
constexpr char SCHEMA_FIELD[] = "schema";
constexpr char REVISION_FIELD[] = "revision";
constexpr char TRANSFER_FIELD[] = "transfer";
constexpr char RECOVERY_FIELD[] = "recovery";
constexpr char DIGEST_FIELD[] = "digest";
constexpr char TOPOLOGY_FIELD[] = "topology";
constexpr int DECIMAL_BASE = 10;
constexpr int HEX_BASE_OFFSET = 10;
constexpr size_t HEX_CHARS_PER_BYTE = 2;
constexpr uint8_t LOW_NIBBLE_MASK = 0x0F;
constexpr uint8_t HIGH_NIBBLE_SHIFT = 4;

char ToHex(uint8_t value)
{
    constexpr char digits[] = "0123456789abcdef";
    return digits[value & LOW_NIBBLE_MASK];
}

int FromHex(char ch)
{
    if (ch >= '0' && ch <= '9') {
        return ch - '0';
    }
    ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    return (ch >= 'a' && ch <= 'f') ? ch - 'a' + HEX_BASE_OFFSET : -1;
}

std::string HexEncode(const std::string &bytes)
{
    std::string encoded;
    encoded.reserve(bytes.size() * HEX_CHARS_PER_BYTE);
    for (unsigned char byte : bytes) {
        encoded.push_back(ToHex(byte >> HIGH_NIBBLE_SHIFT));
        encoded.push_back(ToHex(byte));
    }
    return encoded;
}

Status HexDecode(const std::string &encoded, std::string &bytes)
{
    bytes.clear();
    CHECK_FAIL_RETURN_STATUS(encoded.size() % HEX_CHARS_PER_BYTE == 0, K_INVALID, "hex payload length is invalid");
    bytes.reserve(encoded.size() / HEX_CHARS_PER_BYTE);
    for (size_t i = 0; i < encoded.size(); i += HEX_CHARS_PER_BYTE) {
        int high = FromHex(encoded[i]);
        int low = FromHex(encoded[i + 1]);
        CHECK_FAIL_RETURN_STATUS(high >= 0 && low >= 0, K_INVALID, "hex payload contains invalid digit");
        bytes.push_back(static_cast<char>((high << HIGH_NIBBLE_SHIFT) | low));
    }
    return Status::OK();
}

Status ParseUint64(const std::string &text, uint64_t &value)
{
    value = 0;
    CHECK_FAIL_RETURN_STATUS(!text.empty(), K_INVALID, "empty integer field");
    for (char ch : text) {
        CHECK_FAIL_RETURN_STATUS(ch >= '0' && ch <= '9', K_INVALID, "invalid integer field");
        auto digit = static_cast<uint64_t>(ch - '0');
        CHECK_FAIL_RETURN_STATUS(value <= (std::numeric_limits<uint64_t>::max() - digit) / DECIMAL_BASE, K_INVALID,
                                 "integer field overflow");
        value = value * DECIMAL_BASE + digit;
    }
    return Status::OK();
}

Status ReadFields(const std::string &content, std::map<std::string, std::string> &fields)
{
    fields.clear();
    std::istringstream input(content);
    std::string line;
    while (std::getline(input, line)) {
        auto pos = line.find('=');
        CHECK_FAIL_RETURN_STATUS(pos != std::string::npos, K_INVALID, "invalid snapshot line");
        auto inserted = fields.emplace(line.substr(0, pos), line.substr(pos + 1));
        CHECK_FAIL_RETURN_STATUS(inserted.second, K_INVALID, "duplicated snapshot field");
    }
    return Status::OK();
}

Status GetField(const std::map<std::string, std::string> &fields, const std::string &name, std::string &value)
{
    auto iter = fields.find(name);
    CHECK_FAIL_RETURN_STATUS(iter != fields.end(), K_INVALID, "missing snapshot field: " + name);
    value = iter->second;
    return Status::OK();
}

}  // namespace

FileTopologyLocalSnapshotStore::FileTopologyLocalSnapshotStore(std::string filePath)
    : filePath_(std::move(filePath))
{
}

Status FileTopologyLocalSnapshotStore::Save(const LocalTopologySnapshot &snapshot)
{
    CHECK_FAIL_RETURN_STATUS(!filePath_.empty(), K_INVALID, "local snapshot path is empty");
    std::string expectedDigest;
    RETURN_IF_NOT_OK(TopologyStateDigest::Build(snapshot, expectedDigest));
    CHECK_FAIL_RETURN_STATUS(snapshot.digest == expectedDigest, K_INVALID, "local snapshot digest mismatch");
    std::string topologyBytes;
    TopologyRepositoryCodec codec;
    RETURN_IF_NOT_OK(codec.EncodeTopology(snapshot.topology, topologyBytes));

    std::ostringstream output;
    output << SCHEMA_FIELD << '=' << SNAPSHOT_SCHEMA_VERSION << '\n';
    output << REVISION_FIELD << '=' << snapshot.topologyRevision << '\n';
    output << TRANSFER_FIELD << '=' << snapshot.taskSummary.unfinishedTransferTasks << '\n';
    output << RECOVERY_FIELD << '=' << snapshot.taskSummary.unfinishedRecoveryTasks << '\n';
    output << DIGEST_FIELD << '=' << snapshot.digest << '\n';
    output << TOPOLOGY_FIELD << '=' << HexEncode(topologyBytes) << '\n';
    return AtomicWriteTextFile(filePath_, output.str());
}

Status FileTopologyLocalSnapshotStore::Load(LocalTopologySnapshot &snapshot) const
{
    snapshot = {};
    CHECK_FAIL_RETURN_STATUS(!filePath_.empty(), K_INVALID, "local snapshot path is empty");
    CHECK_FAIL_RETURN_STATUS(FileExist(filePath_), K_NOT_FOUND, "local topology snapshot is absent");
    std::string content;
    RETURN_IF_NOT_OK(ReadWholeFile(filePath_, content));

    std::map<std::string, std::string> fields;
    RETURN_IF_NOT_OK(ReadFields(content, fields));
    std::string field;
    RETURN_IF_NOT_OK(GetField(fields, SCHEMA_FIELD, field));
    CHECK_FAIL_RETURN_STATUS(field == SNAPSHOT_SCHEMA_VERSION, K_INVALID, "unsupported snapshot schema");
    RETURN_IF_NOT_OK(GetField(fields, REVISION_FIELD, field));
    uint64_t revision = 0;
    RETURN_IF_NOT_OK(ParseUint64(field, revision));
    CHECK_FAIL_RETURN_STATUS(revision <= static_cast<uint64_t>(std::numeric_limits<Revision>::max()), K_INVALID,
                             "topology revision overflow");
    snapshot.topologyRevision = static_cast<Revision>(revision);
    RETURN_IF_NOT_OK(GetField(fields, TRANSFER_FIELD, field));
    RETURN_IF_NOT_OK(ParseUint64(field, snapshot.taskSummary.unfinishedTransferTasks));
    RETURN_IF_NOT_OK(GetField(fields, RECOVERY_FIELD, field));
    RETURN_IF_NOT_OK(ParseUint64(field, snapshot.taskSummary.unfinishedRecoveryTasks));
    RETURN_IF_NOT_OK(GetField(fields, DIGEST_FIELD, snapshot.digest));
    RETURN_IF_NOT_OK(GetField(fields, TOPOLOGY_FIELD, field));
    std::string topologyBytes;
    RETURN_IF_NOT_OK(HexDecode(field, topologyBytes));
    TopologyRepositoryCodec codec;
    RETURN_IF_NOT_OK(codec.DecodeTopology(topologyBytes, snapshot.topology));

    std::string expectedDigest;
    RETURN_IF_NOT_OK(TopologyStateDigest::Build(snapshot, expectedDigest));
    CHECK_FAIL_RETURN_STATUS(snapshot.digest == expectedDigest, K_INVALID, "local snapshot digest mismatch");
    return Status::OK();
}

}  // namespace topology
}  // namespace datasystem
