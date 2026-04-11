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
 * Description: Slot index encoding and recovery helpers.
 */

#include "datasystem/common/l2cache/slot_client/slot_index_codec.h"

#include <cstring>

#include <fcntl.h>
#include <unistd.h>

#include "datasystem/common/l2cache/slot_client/slot_file_util.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace {
template <typename T>
void AppendRaw(std::string &buffer, const T &value)
{
    buffer.append(reinterpret_cast<const char *>(&value), sizeof(T));
}

template <typename T>
Status ReadRaw(const std::string &buffer, size_t &offset, T &value)
{
    CHECK_FAIL_RETURN_STATUS(offset + sizeof(T) <= buffer.size(), StatusCode::K_RUNTIME_ERROR, "Incomplete record");
    std::memcpy(&value, buffer.data() + offset, sizeof(T));
    offset += sizeof(T);
    return Status::OK();
}

template <typename T>
bool TryReadRaw(const std::string &buffer, size_t &offset, T &value)
{
    if (offset + sizeof(T) > buffer.size()) {
        return false;
    }
    std::memcpy(&value, buffer.data() + offset, sizeof(T));
    offset += sizeof(T);
    return true;
}

void WriteHeader(std::string &buffer)
{
    uint32_t reserved = 0;
    AppendRaw(buffer, SlotIndexCodec::MAGIC);
    AppendRaw(buffer, SlotIndexCodec::VERSION);
    constexpr int filedNum = 6;
    for (size_t i = 0; i < filedNum; ++i) {
        AppendRaw(buffer, reserved);
    }
}

Status VerifyHeader(const std::string &buffer)
{
    CHECK_FAIL_RETURN_STATUS(buffer.size() >= SlotIndexCodec::HEADER_SIZE, StatusCode::K_RUNTIME_ERROR,
                             "Index header incomplete");
    uint32_t magic = 0;
    uint32_t version = 0;
    size_t offset = 0;
    RETURN_IF_NOT_OK(ReadRaw(buffer, offset, magic));
    RETURN_IF_NOT_OK(ReadRaw(buffer, offset, version));
    CHECK_FAIL_RETURN_STATUS(magic == SlotIndexCodec::MAGIC, StatusCode::K_RUNTIME_ERROR, "Index magic mismatch");
    CHECK_FAIL_RETURN_STATUS(version == SlotIndexCodec::VERSION, StatusCode::K_RUNTIME_ERROR, "Index version mismatch");
    return Status::OK();
}

Status WritePayload(const std::string &path, const std::string &payload, bool sync)
{
    int fd = -1;
    RETURN_IF_NOT_OK(OpenFile(path, O_CREAT | O_RDWR, 0644, &fd));
    Raii closeFd([fd]() {
        if (fd >= 0) {
            close(fd);
        }
    });
    auto offset = static_cast<off_t>(FdFileSize(fd));
    RETURN_IF_NOT_OK(WriteFile(fd, payload.data(), payload.size(), offset));
    if (sync) {
        RETURN_IF_NOT_OK(FsyncFd(fd));
    }
    return Status::OK();
}

Status RewriteHeader(const std::string &indexPath)
{
    RETURN_IF_NOT_OK(SlotIndexCodec::TruncateTail(indexPath, 0));
    std::string header;
    WriteHeader(header);
    RETURN_IF_NOT_OK(WritePayload(indexPath, header, true));
    return Status::OK();
}
}  // namespace

uint32_t SlotIndexCodec::CalcCrc32(const uint8_t *data, size_t len)
{
    uint32_t crc = 0;
    for (size_t i = 0; i < len; ++i) {
        crc ^= data[i];
        for (int bit = 0; bit < 8; ++bit) {
            crc = (crc & 1u) ? ((crc >> 1u) ^ 0xedb88320u) : (crc >> 1u);
        }
    }
    return crc;
}

Status SlotIndexCodec::EnsureIndexFile(const std::string &indexPath)
{
    if (!FileExist(indexPath)) {
        std::string header;
        WriteHeader(header);
        RETURN_IF_NOT_OK(WritePayload(indexPath, header, true));
        return Status::OK();
    }
    std::string content;
    RETURN_IF_NOT_OK(ReadWholeFile(indexPath, content));
    if (content.empty() || content.size() < HEADER_SIZE) {
        LOG(WARNING) << "Reset incomplete slot index header, path=" << indexPath;
        RETURN_IF_NOT_OK(RewriteHeader(indexPath));
        return Status::OK();
    }
    auto rc = VerifyHeader(content);
    if (rc.IsError()) {
        LOG(WARNING) << "Reset invalid slot index header, path=" << indexPath << ", err=" << rc.ToString();
        RETURN_IF_NOT_OK(RewriteHeader(indexPath));
        return Status::OK();
    }
    return Status::OK();
}

Status SlotIndexCodec::AppendPut(const std::string &indexPath, const SlotPutRecord &record)
{
    std::string payload;
    RETURN_IF_NOT_OK(EncodePut(record, payload));
    RETURN_IF_NOT_OK(AppendEncodedRecords(indexPath, payload, true));
    return Status::OK();
}

Status SlotIndexCodec::AppendDelete(const std::string &indexPath, const SlotDeleteRecord &record)
{
    std::string payload;
    RETURN_IF_NOT_OK(EncodeDelete(record, payload));
    RETURN_IF_NOT_OK(AppendEncodedRecords(indexPath, payload, true));
    return Status::OK();
}

Status SlotIndexCodec::AppendImportBegin(const std::string &indexPath, const SlotImportRecord &record)
{
    std::string payload;
    RETURN_IF_NOT_OK(EncodeImportBegin(record, payload));
    RETURN_IF_NOT_OK(AppendEncodedRecords(indexPath, payload, true));
    return Status::OK();
}

Status SlotIndexCodec::AppendImportEnd(const std::string &indexPath, const SlotImportRecord &record)
{
    std::string payload;
    RETURN_IF_NOT_OK(EncodeImportEnd(record, payload));
    RETURN_IF_NOT_OK(AppendEncodedRecords(indexPath, payload, true));
    return Status::OK();
}

Status SlotIndexCodec::EncodePut(const SlotPutRecord &record, std::string &payload)
{
    CHECK_FAIL_RETURN_STATUS(!record.key.empty(), StatusCode::K_INVALID, "PUT key must not be empty");
    payload.clear();
    AppendRaw(payload, static_cast<uint8_t>(SlotRecordType::PUT));
    auto keyLen = static_cast<uint32_t>(record.key.size());
    AppendRaw(payload, keyLen);
    payload.append(record.key);
    AppendRaw(payload, record.fileId);
    AppendRaw(payload, record.offset);
    AppendRaw(payload, record.size);
    AppendRaw(payload, record.version);
    auto writeMode = static_cast<uint8_t>(record.writeMode);
    AppendRaw(payload, writeMode);
    auto crc = CalcCrc32(reinterpret_cast<const uint8_t *>(payload.data()), payload.size());
    AppendRaw(payload, crc);
    return Status::OK();
}

Status SlotIndexCodec::EncodeDelete(const SlotDeleteRecord &record, std::string &payload)
{
    CHECK_FAIL_RETURN_STATUS(!record.key.empty(), StatusCode::K_INVALID, "DELETE key must not be empty");
    payload.clear();
    AppendRaw(payload, static_cast<uint8_t>(SlotRecordType::DELETE));
    auto keyLen = static_cast<uint32_t>(record.key.size());
    AppendRaw(payload, keyLen);
    payload.append(record.key);
    AppendRaw(payload, record.version);
    auto crc = CalcCrc32(reinterpret_cast<const uint8_t *>(payload.data()), payload.size());
    AppendRaw(payload, crc);
    return Status::OK();
}

Status SlotIndexCodec::EncodeImportBegin(const SlotImportRecord &record, std::string &payload)
{
    CHECK_FAIL_RETURN_STATUS(!record.txnId.empty(), StatusCode::K_INVALID, "IMPORT_BEGIN txn id must not be empty");
    payload.clear();
    AppendRaw(payload, static_cast<uint8_t>(SlotRecordType::IMPORT_BEGIN));
    auto txnLen = static_cast<uint32_t>(record.txnId.size());
    AppendRaw(payload, txnLen);
    payload.append(record.txnId);
    auto crc = CalcCrc32(reinterpret_cast<const uint8_t *>(payload.data()), payload.size());
    AppendRaw(payload, crc);
    return Status::OK();
}

Status SlotIndexCodec::EncodeImportEnd(const SlotImportRecord &record, std::string &payload)
{
    CHECK_FAIL_RETURN_STATUS(!record.txnId.empty(), StatusCode::K_INVALID, "IMPORT_END txn id must not be empty");
    payload.clear();
    AppendRaw(payload, static_cast<uint8_t>(SlotRecordType::IMPORT_END));
    auto txnLen = static_cast<uint32_t>(record.txnId.size());
    AppendRaw(payload, txnLen);
    payload.append(record.txnId);
    auto crc = CalcCrc32(reinterpret_cast<const uint8_t *>(payload.data()), payload.size());
    AppendRaw(payload, crc);
    return Status::OK();
}

Status SlotIndexCodec::AppendEncodedRecords(const std::string &indexPath, const std::string &payload, bool sync)
{
    RETURN_IF_NOT_OK(EnsureIndexFile(indexPath));
    RETURN_IF_NOT_OK(WritePayload(indexPath, payload, sync));
    return Status::OK();
}

Status SlotIndexCodec::AppendEncodedRecords(int indexFd, uint64_t &offset, const std::string &payload)
{
    CHECK_FAIL_RETURN_STATUS(indexFd >= 0, StatusCode::K_INVALID, "Invalid index fd");
    if (payload.empty()) {
        return Status::OK();
    }
    RETURN_IF_NOT_OK(WriteFile(indexFd, payload.data(), payload.size(), static_cast<off_t>(offset)));
    offset += payload.size();
    return Status::OK();
}

Status SlotIndexCodec::ReadAllRecords(const std::string &indexPath, std::vector<SlotRecord> &records,
                                      size_t &validBytes)
{
    std::vector<SlotRecordFrame> frames;
    RETURN_IF_NOT_OK(ReadAllRecordFrames(indexPath, frames, validBytes));
    records.clear();
    records.reserve(frames.size());
    for (auto &frame : frames) {
        records.emplace_back(std::move(frame.record));
    }
    return Status::OK();
}

Status SlotIndexCodec::ReadAllRecordFrames(const std::string &indexPath, std::vector<SlotRecordFrame> &frames,
                                           size_t &validBytes)
{
    frames.clear();
    validBytes = 0;
    RETURN_IF_NOT_OK(EnsureIndexFile(indexPath));
    std::string content;
    RETURN_IF_NOT_OK(ReadWholeFile(indexPath, content));
    RETURN_IF_NOT_OK(VerifyHeader(content));
    size_t offset = HEADER_SIZE;
    validBytes = HEADER_SIZE;
    while (offset < content.size()) {
        const size_t recordStart = offset;
        uint8_t rawType = 0;
        if (!TryReadRaw(content, offset, rawType)) {
            break;
        }
        SlotRecordFrame frame;
        frame.startOffset = recordStart;
        frame.record.type = static_cast<SlotRecordType>(rawType);
        if (frame.record.type == SlotRecordType::PUT) {
            uint32_t keyLen = 0;
            if (!TryReadRaw(content, offset, keyLen) || offset + keyLen > content.size()) {
                break;
            }
            frame.record.put.key = content.substr(offset, keyLen);
            offset += keyLen;
            if (!TryReadRaw(content, offset, frame.record.put.fileId)
                || !TryReadRaw(content, offset, frame.record.put.offset)
                || !TryReadRaw(content, offset, frame.record.put.size)
                || !TryReadRaw(content, offset, frame.record.put.version)) {
                break;
            }
            uint8_t writeMode = 0;
            if (!TryReadRaw(content, offset, writeMode)) {
                break;
            }
            frame.record.put.writeMode = static_cast<WriteMode>(writeMode);
            uint32_t storedCrc = 0;
            if (!TryReadRaw(content, offset, storedCrc)) {
                break;
            }
            auto computedCrc =
                CalcCrc32(reinterpret_cast<const uint8_t *>(content.data() + recordStart), offset - recordStart - 4);
            if (storedCrc != computedCrc) {
                break;
            }
        } else if (frame.record.type == SlotRecordType::DELETE) {
            uint32_t keyLen = 0;
            if (!TryReadRaw(content, offset, keyLen) || offset + keyLen > content.size()) {
                break;
            }
            frame.record.del.key = content.substr(offset, keyLen);
            offset += keyLen;
            if (!TryReadRaw(content, offset, frame.record.del.version)) {
                break;
            }
            uint32_t storedCrc = 0;
            if (!TryReadRaw(content, offset, storedCrc)) {
                break;
            }
            auto computedCrc =
                CalcCrc32(reinterpret_cast<const uint8_t *>(content.data() + recordStart), offset - recordStart - 4);
            if (storedCrc != computedCrc) {
                break;
            }
        } else if (frame.record.type == SlotRecordType::IMPORT_BEGIN
                   || frame.record.type == SlotRecordType::IMPORT_END) {
            uint32_t txnLen = 0;
            if (!TryReadRaw(content, offset, txnLen) || offset + txnLen > content.size()) {
                break;
            }
            frame.record.import.txnId = content.substr(offset, txnLen);
            offset += txnLen;
            uint32_t storedCrc = 0;
            if (!TryReadRaw(content, offset, storedCrc)) {
                break;
            }
            auto computedCrc =
                CalcCrc32(reinterpret_cast<const uint8_t *>(content.data() + recordStart), offset - recordStart - 4);
            if (storedCrc != computedCrc) {
                break;
            }
        } else {
            break;
        }
        frame.endOffset = offset;
        frames.emplace_back(std::move(frame));
        validBytes = offset;
    }
    if (validBytes < content.size()) {
        RETURN_IF_NOT_OK(TruncateTail(indexPath, validBytes));
    }
    return Status::OK();
}

Status SlotIndexCodec::TruncateTail(const std::string &indexPath, size_t validBytes)
{
    if (truncate(indexPath.c_str(), static_cast<off_t>(validBytes)) != 0) {
        RETURN_STATUS_LOG_ERROR(StatusCode::K_IO_ERROR,
                                FormatString("truncate index failed, path=%s, errno=%d, errmsg=%s", indexPath, errno,
                                             StrErr(errno)));
    }
    int fd = -1;
    RETURN_IF_NOT_OK(OpenFile(indexPath, O_RDWR, &fd));
    Raii closeFd([fd]() {
        if (fd >= 0) {
            close(fd);
        }
    });
    RETURN_IF_NOT_OK(FsyncFd(fd));
    return Status::OK();
}
}  // namespace datasystem
