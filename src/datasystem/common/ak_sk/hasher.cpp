/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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

#include "datasystem/common/ak_sk/hasher.h"

#include <climits>
#include <cstring>
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/utils/status.h"
#include <securec.h>

#include <openssl/sha.h>
#include <openssl/hmac.h>

const int DIVIDE_TO_BYTES = 2;

namespace datasystem {
Status Hasher::HexEncode(std::unique_ptr<unsigned char[]> &inputData, unsigned int inputDataSize,
                         std::string &signature)
{
    std::unique_ptr<unsigned char[]> outData;
    unsigned int outSize;
    RETURN_IF_NOT_OK(HexEncode(inputData, inputDataSize, outData, outSize));
    signature = std::string((char *)outData.get(), outSize);
    return Status::OK();
}

Status Hasher::HexEncode(std::unique_ptr<unsigned char[]> &inputData, unsigned int inputDataSize,
                         std::unique_ptr<unsigned char[]> &outData, unsigned int &outSize)
{
    if (inputDataSize <= 0) {
        return Status::OK();
    }
    if ((UINT_MAX - 1) / DIVIDE_TO_BYTES < inputDataSize) {
        RETURN_STATUS(K_RUNTIME_ERROR,
            FormatString("Input data is too large to encode to hex, input size %u.", inputDataSize));
    }
    outSize = inputDataSize * DIVIDE_TO_BYTES;
    outData = std::make_unique<unsigned char[]>(outSize + 1);  // the dest size is 2n + 1
    for (size_t i = 0; i < inputDataSize; i++) {
        size_t offset = i * DIVIDE_TO_BYTES;
        int ret = sprintf_s((char *)outData.get() + offset, outSize + 1 - offset, "%02x", inputData.get()[i]);
        if (ret == -1) {
            RETURN_STATUS(K_RUNTIME_ERROR, "ak/sk signature hex encode sprintf_s failed.");
        }
    }
    return Status::OK();
}

Status Hasher::HexEncodeForAkSk(std::unique_ptr<unsigned char[]> &inputData, unsigned int inputDataSize,
                                std::string &signature)
{
    std::unique_ptr<unsigned char[]> outData;
    unsigned int outSize;
    RETURN_IF_NOT_OK(HexEncodeForAkSk(inputData, inputDataSize, outData, outSize));
    signature = std::string((char *)outData.get(), outSize);
    return Status::OK();
}

Status Hasher::HexEncodeForAkSk(std::unique_ptr<unsigned char[]> &inputData, unsigned int inputDataSize,
                                std::unique_ptr<unsigned char[]> &outData, unsigned int &outSize)
{
    if (inputDataSize <= 0) {
        return Status::OK();
    }
    if ((UINT_MAX - 1) / DIVIDE_TO_BYTES < inputDataSize) {
        RETURN_STATUS(K_RUNTIME_ERROR,
            FormatString("Input data is too large to encode to hex, input size %u.", inputDataSize));
    }
    outSize = inputDataSize * DIVIDE_TO_BYTES + 1;  // the dest size is 2n + 1
    outData = std::make_unique<unsigned char[]>(outSize);
    for (size_t i = 0; i < inputDataSize; i++) {
        size_t offset = i * DIVIDE_TO_BYTES;
        int ret = sprintf_s((char *)outData.get() + offset, outSize - offset, "%02x", inputData.get()[i]);
        if (ret == -1) {
            RETURN_STATUS(K_RUNTIME_ERROR, "ak/sk signature hex encode sprintf_s failed.");
        }
    }
    return Status::OK();
}

Status Hasher::HashSHA256(const char *data, size_t size, std::unique_ptr<unsigned char[]> &outData,
                          unsigned int &outSize)
{
    outData = std::make_unique<unsigned char[]>(SHA256_DIGEST_LENGTH);
    SHA256_CTX sha256;
    CHECK_FAIL_RETURN_STATUS(SHA256_Init(&sha256) == 1, K_RUNTIME_ERROR, "SHA256_Init failed.");
    CHECK_FAIL_RETURN_STATUS(SHA256_Update(&sha256, data, size) == 1, K_RUNTIME_ERROR,
                             "SHA256_Update failed.");
    CHECK_FAIL_RETURN_STATUS(SHA256_Final(outData.get(), &sha256) == 1, K_RUNTIME_ERROR, "SHA256_Final failed.");
    outSize = SHA256_DIGEST_LENGTH;
    return Status::OK();
}

Status Hasher::Hmac(const void *key, int keyLen, std::unique_ptr<unsigned char[]> &inputData,
                    unsigned int inputDataSize, std::unique_ptr<unsigned char[]> &outData, unsigned int &outSize)
{
    const EVP_MD *engine = EVP_sha256();
    outData = std::make_unique<unsigned char[]>(SHA256_DIGEST_LENGTH);
    if (HMAC(engine, key, keyLen, (const unsigned char *)(inputData.get()), inputDataSize, outData.get(), &outSize)
        == NULL) {
        RETURN_STATUS(K_RUNTIME_ERROR, "Failed to calc HMAC for AK/SK");
    }
    return Status::OK();
}

Status Hasher::GetHMACSha256(const SensitiveValue &key, const std::string &data, std::string &sha256)
{
    CHECK_FAIL_RETURN_STATUS(key.GetSize() <= INT_MAX, K_INVALID,
                             FormatString("Key size %zu exceed INT_MAX", key.GetSize()));
    auto keySize = static_cast<int>(key.GetSize());
    const EVP_MD *engine = EVP_sha256();
    std::unique_ptr<unsigned char[]> outData = std::make_unique<unsigned char[]>(SHA256_DIGEST_LENGTH);
    unsigned int outSize;
    if (HMAC(engine, key.GetData(), keySize, reinterpret_cast<const unsigned char *>(data.data()), data.size(),
             outData.get(), &outSize)
        == NULL) {
        RETURN_STATUS(K_RUNTIME_ERROR, "Failed to calc HMAC for AK/SK");
    }
    sha256 = std::string((char *)outData.get(), outSize);
    return Status::OK();
}

Status Hasher::GetHMACSha256Hex(const SensitiveValue &key, const std::string &data, std::string &sha256)
{
    CHECK_FAIL_RETURN_STATUS(key.GetSize() <= INT_MAX, K_INVALID,
                             FormatString("Key size %zu exceed INT_MAX", key.GetSize()));
    auto keySize = static_cast<int>(key.GetSize());
    const EVP_MD *engine = EVP_sha256();
    std::unique_ptr<unsigned char[]> outData = std::make_unique<unsigned char[]>(SHA256_DIGEST_LENGTH);
    unsigned int outSize;
    if (HMAC(engine, key.GetData(), keySize, reinterpret_cast<const unsigned char *>(data.data()), data.size(),
             outData.get(), &outSize)
        == NULL) {
        RETURN_STATUS(K_RUNTIME_ERROR, "Failed to calc HMAC for AK/SK");
    }
    RETURN_IF_NOT_OK(HexEncode(outData, outSize, sha256));
    return Status::OK();
}

Status Hasher::GetSha256Hex(const std::string &str, std::string &hashVal)
{
    std::unique_ptr<unsigned char[]> outHashData;
    unsigned int outHashSize;
    RETURN_IF_NOT_OK(HashSHA256(str.c_str(), str.size(), outHashData, outHashSize));
    std::unique_ptr<unsigned char[]> data;
    RETURN_IF_NOT_OK(HexEncode(outHashData, outHashSize, hashVal));
    return Status::OK();
}
}  // namespace datasystem
