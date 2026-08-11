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
#include <cstring>
#include <random>
#include <unistd.h>

#include <securec.h>

#include "datasystem/common/util/random_data.h"
#include "datasystem/common/log/log.h"

namespace datasystem {
namespace {
constexpr char HEX_DIGITS[] = "0123456789abcdef";
constexpr char UUID_FMT[] = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx";
constexpr char INDEX_PREFIX[] = "00000000-0000-";
constexpr uint64_t INDEX_DIV16 = 10000000000000000ULL;
constexpr uint64_t INDEX_DIV12 = 1000000000000ULL;
constexpr uint64_t INDEX_MOD4 = 10000ULL;
constexpr size_t DECIMAL_BASE = 10;
constexpr size_t NIBBLE_BITS = 4;
constexpr size_t GROUP_WIDTH = 4;
constexpr size_t TAIL_WIDTH = 12;

Status FillDecimalPadded(uint64_t value, size_t width, char *out)
{
    CHECK_FAIL_RETURN_STATUS(out != nullptr, StatusCode::K_INVALID, "output is nullptr");
    for (size_t i = 0; i < width; ++i) {
        out[width - i - 1] = static_cast<char>('0' + (value % DECIMAL_BASE));
        value /= DECIMAL_BASE;
    }
    CHECK_FAIL_RETURN_STATUS(value == 0, StatusCode::K_INVALID, "value exceeds decimal field width.");
    return Status::OK();
}

void FillRandomUuidBytes(uint8_t random[UUID_SIZE])
{
    static thread_local std::mt19937 gen(RandomData::GetRandomSeed());
    // fork() leaves the child with the parent's already-initialized gen (mt19937
    // state + the static-init guard), so without intervention the child would
    // continue the parent's sequence and likely reproduce a UUID the parent is
    // about to emit. Detect the fork by comparing the live pid against the one
    // cached when this thread's gen was last (re)seeded: fork() changes the
    // child's live pid, but the child inherits the parent's cached value, so a
    // mismatch on the next call means a fork happened since the last seed ->
    // re-seed from a fresh entropy source (which reads the child's live pid) so
    // the child diverges. Updating the cached pid afterwards keeps arbitrary
    // nesting (grandchild forks again -> pid changes again) fork-safe. This is
    // per-thread and lock-free; no global pthread_atfork registration or
    // process-wide state is needed.
    static thread_local pid_t observedPid = ::getpid();
    pid_t curPid = ::getpid();
    if (curPid != observedPid) {
        gen.seed(RandomData::GetRandomSeed());
        observedPid = curPid;
    }
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
}
}  // namespace

std::string GetBytesUuid()
{
    uint8_t random[UUID_SIZE];
    FillRandomUuidBytes(random);

    return std::string(reinterpret_cast<char *>(random), sizeof(random));
}

std::string GetStringUuid()
{
    char uuid[UUID_STRING_BUFFER_SIZE];
    auto rc = GetStringUuid(uuid, sizeof(uuid));
    if (rc.IsError()) {
        LOG(ERROR) << "GetStringUuid failed: " << rc.ToString();
        return "";
    }
    return std::string(uuid, UUID_STRING_SIZE);
}

std::string BytesUuidToString(const std::string &bytesUuid)
{
    if (bytesUuid.size() != UUID_SIZE) {
        DLOG(ERROR) << "Converting non-standard UUID, has length " << bytesUuid.size() << " skipping...";
        // If the UUID is not a standard UUID, the UUID is not converted.
        return bytesUuid;
    }

    char uuid[UUID_STRING_BUFFER_SIZE];
    auto rc = BytesUuidToString(reinterpret_cast<const uint8_t *>(bytesUuid.data()), bytesUuid.size(), uuid,
                                sizeof(uuid));
    if (rc.IsError()) {
        LOG(ERROR) << "BytesUuidToString failed: " << rc.ToString();
        return bytesUuid;
    }

    return std::string(uuid, UUID_STRING_SIZE);
}

Status BytesUuidToString(const uint8_t *bytesUuid, size_t bytesUuidSize, char *stringUuid, size_t stringUuidSize)
{
    CHECK_FAIL_RETURN_STATUS(bytesUuid != nullptr, StatusCode::K_INVALID, "bytesUuid is nullptr");
    CHECK_FAIL_RETURN_STATUS(stringUuid != nullptr, StatusCode::K_INVALID, "stringUuid is nullptr");
    CHECK_FAIL_RETURN_STATUS(bytesUuidSize == UUID_SIZE, StatusCode::K_INVALID, "The size of byte uuid should be 16.");
    CHECK_FAIL_RETURN_STATUS(stringUuidSize >= UUID_STRING_BUFFER_SIZE, StatusCode::K_INVALID,
                             "The size of string uuid buffer should be at least 37.");

    size_t pos = 0;
    for (size_t i = 0; i < UUID_SIZE; ++i) {
        const uint8_t value = bytesUuid[i];
        stringUuid[pos++] = HEX_DIGITS[value >> NIBBLE_BITS];
        stringUuid[pos++] = HEX_DIGITS[value & 0x0F];
        if (UUID_FMT[pos] == '-') {
            stringUuid[pos++] = '-';
        }
    }
    stringUuid[pos] = '\0';
    return Status::OK();
}

Status GetStringUuid(char *stringUuid, size_t stringUuidSize)
{
    uint8_t random[UUID_SIZE];
    FillRandomUuidBytes(random);
    return BytesUuidToString(random, UUID_SIZE, stringUuid, stringUuidSize);
}

Status IndexUuidGenerator(const uint64_t uuidNumber, std::string &stringUuid)
{
    // uint64_t max is 18,446,744,073,709,551,615 (len = 20 ： 4 + 4 + 12)
    // xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    // (8)-(4)-(4)-(4)-(12)
    char uuid[UUID_STRING_BUFFER_SIZE];
    RETURN_IF_NOT_OK(IndexUuidGenerator(uuidNumber, uuid, sizeof(uuid)));
    stringUuid.assign(uuid, UUID_STRING_SIZE);
    return Status::OK();
}

Status IndexUuidGenerator(const uint64_t uuidNumber, char *stringUuid, size_t stringUuidSize)
{
    CHECK_FAIL_RETURN_STATUS(stringUuid != nullptr, StatusCode::K_INVALID, "stringUuid is nullptr");
    CHECK_FAIL_RETURN_STATUS(stringUuidSize >= UUID_STRING_BUFFER_SIZE, StatusCode::K_INVALID,
                             "The size of string uuid buffer should be at least 37.");

    // uint64_t max has 20 decimal digits, split as 4 + 4 + 12 into the UUID tail:
    // 00000000-0000-xxxx-xxxx-xxxxxxxxxxxx
    size_t pos = sizeof(INDEX_PREFIX) - 1;
    std::memcpy(stringUuid, INDEX_PREFIX, pos);
    RETURN_IF_NOT_OK(FillDecimalPadded(uuidNumber / INDEX_DIV16, GROUP_WIDTH, stringUuid + pos));
    stringUuid[pos += GROUP_WIDTH] = '-';
    ++pos;
    RETURN_IF_NOT_OK(FillDecimalPadded(uuidNumber / INDEX_DIV12 % INDEX_MOD4, GROUP_WIDTH, stringUuid + pos));
    stringUuid[pos += GROUP_WIDTH] = '-';
    ++pos;
    RETURN_IF_NOT_OK(FillDecimalPadded(uuidNumber % INDEX_DIV12, TAIL_WIDTH, stringUuid + pos));
    stringUuid[UUID_STRING_SIZE] = '\0';
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
