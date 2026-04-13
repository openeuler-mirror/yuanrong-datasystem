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
#include <openssl/evp.h>

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

Status Hasher::GetHMACSha1(const SensitiveValue &key, const std::string &data, std::string &output)
{
    CHECK_FAIL_RETURN_STATUS(key.GetSize() <= INT_MAX, K_INVALID,
                             FormatString("Key size %zu exceed INT_MAX", key.GetSize()));
    auto keySize = static_cast<int>(key.GetSize());
    unsigned char result[EVP_MAX_MD_SIZE];
    unsigned int resultLen = 0;
    if (HMAC(EVP_sha1(), key.GetData(), keySize,
             reinterpret_cast<const unsigned char *>(data.data()), data.size(),
             result, &resultLen) == nullptr) {
        RETURN_STATUS(K_RUNTIME_ERROR, "Failed to calc HMAC-SHA1");
    }
    output.assign(reinterpret_cast<char *>(result), resultLen);
    return Status::OK();
}

Status Hasher::Base64Encode(const std::string &input, std::string &output)
{
    if (input.empty()) {
        output.clear();
        return Status::OK();
    }
    // BoringSSL does not support nullptr output buffer in EVP_EncodeBlock,
    // so calculate the encoded length directly: 4 * ceil(input_size / 3)
    size_t inputSize = input.size();
    size_t encodedLen = 4 * ((inputSize + 2) / 3);
    output.resize(encodedLen);
    int actualLen = EVP_EncodeBlock(reinterpret_cast<unsigned char *>(&output[0]),
                    reinterpret_cast<const unsigned char *>(input.data()), static_cast<int>(input.size()));
    output.resize(actualLen);
    while (!output.empty() && (output.back() == '\n' || output.back() == '\r' || output.back() == '\0')) {
        output.pop_back();
    }
    return Status::OK();
}

Status Hasher::GetHMACSha1Base64(const SensitiveValue &key, const std::string &data, std::string &output)
{
    std::string hmacResult;
    RETURN_IF_NOT_OK(GetHMACSha1(key, data, hmacResult));
    return Base64Encode(hmacResult, output);
}

Status Hasher::GetMD5(const std::string &data, std::string &output)
{
    unsigned char result[EVP_MAX_MD_SIZE];
    unsigned int resultLen = 0;
    EVP_Digest(data.data(), data.size(), result, &resultLen, EVP_md5(), nullptr);
    output.assign(reinterpret_cast<char *>(result), resultLen);
    return Status::OK();
}

Status Hasher::GetMD5Base64(const std::string &data, std::string &output)
{
    std::string md5Result;
    RETURN_IF_NOT_OK(GetMD5(data, md5Result));
    return Base64Encode(md5Result, output);
}
}  // namespace datasystem
