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

#ifndef DATASYSTEM_COMMON_L2CACHE_SFS_SLOT_STORE_SLOT_INDEX_CODEC_H
#define DATASYSTEM_COMMON_L2CACHE_SFS_SLOT_STORE_SLOT_INDEX_CODEC_H

#include <cstdint>
#include <string>
#include <vector>

#include "datasystem/object/object_enum.h"
#include "datasystem/utils/status.h"

namespace datasystem {

enum class SlotRecordType : uint8_t {
    PUT = 0x01,
    DELETE = 0x02,
    IMPORT_BEGIN = 0x03,
    IMPORT_END = 0x04,
};

/**
 * @brief Serialized PUT record stored in the slot index.
 */
struct SlotPutRecord {
    std::string key;
    uint32_t fileId{ 0 };
    uint64_t offset{ 0 };
    uint64_t size{ 0 };
    uint64_t version{ 0 };
    WriteMode writeMode{ WriteMode::NONE_L2_CACHE };
    uint32_t ttlSecond{ 0 };
};

/**
 * @brief Serialized DELETE record stored in the slot index.
 */
struct SlotDeleteRecord {
    std::string key;
    uint64_t version{ 0 };
};

/**
 * @brief Serialized IMPORT boundary record stored in the slot index.
 */
struct SlotImportRecord {
    std::string txnId;
};

/**
 * @brief Tagged union used while replaying index records.
 */
struct SlotRecord {
    SlotRecordType type{ SlotRecordType::PUT };
    SlotPutRecord put;
    SlotDeleteRecord del;
    SlotImportRecord import;
};

struct SlotRecordFrame {
    SlotRecord record;
    size_t startOffset{ 0 };
    size_t endOffset{ 0 };
};

/**
 * @brief Encode, append and recover slot index records.
 */
class SlotIndexCodec {
public:
    ~SlotIndexCodec() = default;

    static constexpr uint32_t MAGIC = 0x534c5431;  // SLT1
    static constexpr uint32_t VERSION = 1;
    static constexpr size_t HEADER_SIZE = 32;

    /**
     * @brief Ensure that the index file exists and has a valid header.
     * @param[in] indexPath The index file path.
     * @return Status of the call.
     */
    static Status EnsureIndexFile(const std::string &indexPath);

    /**
     * @brief Append a PUT record to the active index file.
     * @param[in] indexPath The index file path.
     * @param[in] record The PUT record to append.
     * @return Status of the call.
     */
    static Status AppendPut(const std::string &indexPath, const SlotPutRecord &record);

    /**
     * @brief Append a DELETE record to the active index file.
     * @param[in] indexPath The index file path.
     * @param[in] record The DELETE record to append.
     * @return Status of the call.
     */
    static Status AppendDelete(const std::string &indexPath, const SlotDeleteRecord &record);

    /**
     * @brief Append an IMPORT_BEGIN record to the active index file.
     * @param[in] indexPath The index file path.
     * @param[in] record The IMPORT_BEGIN record to append.
     * @return Status of the call.
     */
    static Status AppendImportBegin(const std::string &indexPath, const SlotImportRecord &record);

    /**
     * @brief Append an IMPORT_END record to the active index file.
     * @param[in] indexPath The index file path.
     * @param[in] record The IMPORT_END record to append.
     * @return Status of the call.
     */
    static Status AppendImportEnd(const std::string &indexPath, const SlotImportRecord &record);

    /**
     * @brief Encode a PUT record into the durable binary payload.
     * @param[in] record The PUT record to encode.
     * @param[out] payload The encoded payload bytes.
     * @return Status of the call.
     */
    static Status EncodePut(const SlotPutRecord &record, std::string &payload);

    /**
     * @brief Encode a DELETE record into the durable binary payload.
     * @param[in] record The DELETE record to encode.
     * @param[out] payload The encoded payload bytes.
     * @return Status of the call.
     */
    static Status EncodeDelete(const SlotDeleteRecord &record, std::string &payload);

    /**
     * @brief Encode an IMPORT_BEGIN record into the durable binary payload.
     * @param[in] record The IMPORT_BEGIN record to encode.
     * @param[out] payload The encoded payload bytes.
     * @return Status of the call.
     */
    static Status EncodeImportBegin(const SlotImportRecord &record, std::string &payload);

    /**
     * @brief Encode an IMPORT_END record into the durable binary payload.
     * @param[in] record The IMPORT_END record to encode.
     * @param[out] payload The encoded payload bytes.
     * @return Status of the call.
     */
    static Status EncodeImportEnd(const SlotImportRecord &record, std::string &payload);

    /**
     * @brief Append pre-encoded records to an index file with one durable step.
     * @param[in] indexPath The index file path.
     * @param[in] payload The pre-encoded record bytes.
     * @param[in] sync Whether to fsync after the append.
     * @return Status of the call.
     */
    static Status AppendEncodedRecords(const std::string &indexPath, const std::string &payload, bool sync = true);

    /**
     * @brief Append pre-encoded records to an already-open index fd.
     * @param[in] indexFd The active index fd.
     * @param[in,out] offset The append offset that will be advanced on success.
     * @param[in] payload The pre-encoded record bytes.
     * @return Status of the call.
     */
    static Status AppendEncodedRecords(int indexFd, uint64_t &offset, const std::string &payload);

    /**
     * @brief Read all valid records and truncate any damaged tail bytes.
     * @param[in] indexPath The index file path.
     * @param[out] records The parsed valid records.
     * @param[out] validBytes The valid byte length after replay.
     * @return Status of the call.
     */
    static Status ReadAllRecords(const std::string &indexPath, std::vector<SlotRecord> &records, size_t &validBytes);

    /**
     * @brief Read all valid records with their byte ranges in the index file.
     * @param[in] indexPath The index file path.
     * @param[out] frames The parsed valid record frames.
     * @param[out] validBytes The valid byte length after replay.
     * @return Status of the call.
     */
    static Status ReadAllRecordFrames(const std::string &indexPath, std::vector<SlotRecordFrame> &frames,
                                      size_t &validBytes);

    /**
     * @brief Truncate the index file to the validated byte length.
     * @param[in] indexPath The index file path.
     * @param[in] validBytes The validated byte length to keep.
     * @return Status of the call.
     */
    static Status TruncateTail(const std::string &indexPath, size_t validBytes);

private:
    static uint32_t CalcCrc32(const uint8_t *data, size_t len);
};
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_L2CACHE_SFS_SLOT_STORE_SLOT_INDEX_CODEC_H
