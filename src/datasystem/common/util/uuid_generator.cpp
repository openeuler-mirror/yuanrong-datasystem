/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: Generator for uuid.
 */

#include "datasystem/common/util/uuid_generator.h"

#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <random>

#include <securec.h>

#include "datasystem/common/util/random_data.h"
#include "datasystem/common/log/log.h"

namespace datasystem {

std::string GetBytesUuid()
{
    static thread_local std::mt19937 gen(RandomData::GetRandomSeed());
    uint8_t random[UUID_SIZE];
    std::uniform_int_distribution<> dist(0, UINT8_MAX);
    for (size_t i = 0; i < UUID_SIZE; i++) {
        random[i] = dist(gen);
    }

    // set version digit to 4
    // must be 0b0100xxxx
    random[UUID_VERSION_BYTEINDEX] &= 0x4F;
    random[UUID_VERSION_BYTEINDEX] |= 0x40;
    // set variant digit to 8-b
    // must be 0b10xxxxxx
    random[UUID_VARIANT_BYTEINDEX] &= 0xBF;
    random[UUID_VARIANT_BYTEINDEX] |= 0x80;

    return std::string(reinterpret_cast<char *>(random), sizeof(random));
}

std::string GetStringUuid()
{
    return BytesUuidToString(GetBytesUuid());
}

std::string BytesUuidToString(const std::string &bytesUuid)
{
    if (bytesUuid.size() != UUID_SIZE) {
        DLOG(ERROR) << "Converting non-standard UUID, has length " << bytesUuid.size() << " skipping...";
        // If the UUID is not a standard UUID, the UUID is not converted.
        return bytesUuid;
    }

    std::stringstream result;
    result << std::setfill('0');
    constexpr uint32_t hexWidth = 2; // represent uint8_t in hex string, width is 2
    auto *p = reinterpret_cast<const uint8_t *>(bytesUuid.data());
    for (size_t i = 0; i < bytesUuid.size(); p++, i++) {
        result << std::setw(hexWidth) << std::hex << +*p;
        // uuid format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx, '-' at 3rd,5th,7th,9th byte
        if (i == 3 || i == 5 || i == 7 || i == 9) {
            result << "-";
        }
    }

    return result.str();
}

Status IndexUuidGenerator(const uint64_t uuidNumber, std::string &stringUuid)
{
    // uint64_t max is 18,446,744,073,709,551,615 (len = 20 ： 4 + 4 + 12)
    // xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    // (8)-(4)-(4)-(4)-(12)
    std::stringstream ss;
    const auto dataLen4 = 4;
    const auto dataLen12 = 12;

    // cmath std::power can't get the exact value of uint64_t ( (uint64_t)power(10, 4) = 9999 )
    const uint64_t power16 = 10000000000000000;
    const uint64_t power12 = 1000000000000;
    const uint64_t power4 = 10000;

    ss << "00000000-0000-" << std::setw(dataLen4) << std::setfill('0') << (uuidNumber / power16) << "-"
       << std::setw(dataLen4) << std::setfill('0') << (uuidNumber / power12 % power4) << "-" << std::setw(dataLen12)
       << std::setfill('0') << (uuidNumber % power12);

    stringUuid = ss.str();
    return Status::OK();
}

Status StringUuidToBytes(const std::string &stringUuid, std::string &byteUuid)
{
    // Converting the input string of 36 bytes length of the following format
    // xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    // (8)-(4)-(4)-(4)-(12)
    // Each character must only be [0-9], [a-f], or [A-F].
    const auto inputLength = 36;
    const auto four = 4;
    const auto len16 = 16;
    const auto offset8 = 8;
    const auto offset13 = 13;
    const auto offset18 = 18;
    const auto offset23 = 23;

    // Input string must be 36 bytes, if not, will not convert.
    CHECK_FAIL_RETURN_STATUS(stringUuid.size() == inputLength, StatusCode::K_INVALID,
                             "The size of string uuid should be 36.");
    char res[16];
    auto inputOffset = 0;
    auto outputOffset = 0;
    bool shift = true;
    uint8_t hi = 0;
    uint8_t lo = 0;
    const int diffLowerLetter = 0x57;
    const int diffUpperLetter = 0x37;
    while (inputOffset < inputLength) {
        if (stringUuid[inputOffset] >= '0' && stringUuid[inputOffset] <= '9') {
            lo = stringUuid[inputOffset] - '0';
        } else if (stringUuid[inputOffset] >= 'a' && stringUuid[inputOffset] <= 'f') {
            lo = stringUuid[inputOffset] - diffLowerLetter;
        } else if (stringUuid[inputOffset] >= 'A' && stringUuid[inputOffset] <= 'F') {
            lo = stringUuid[inputOffset] - diffUpperLetter;
        } else {
            RETURN_STATUS(StatusCode::K_INVALID, "Invalid input character");
        }
        if (shift) {
            hi = lo << four;  // Multiply by 16.
            shift = false;
        } else {
            res[outputOffset] = hi + lo;
            ++outputOffset;
            shift = true;
        }
        ++inputOffset;
        if (inputOffset == offset8 || inputOffset == offset13 || inputOffset == offset18 || inputOffset == offset23) {
            ++inputOffset;
            shift = true;
        }
    }
    byteUuid = std::string(res, len16);
    return Status::OK();
}
}  // namespace datasystem
