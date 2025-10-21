/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: The cipher of AES implement.
 */
#include "datasystem/common/aes/aes_impl.h"

#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/aes.h>
#include <openssl/rand.h>
#include <cstddef>
#include <memory>
#include <utility>
#include <iostream>
#include <fstream>

#include "securec.h"

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/utils/sensitive_value.h"
#include "datasystem/utils/status.h"

namespace datasystem {
const int AES_128_GCM_KEY_LEN = 16;
const int AES_192_GCM_KEY_LEN = 24;
const int AES_256_GCM_KEY_LEN = 32;
namespace {
const EVP_CIPHER *GetCipher(AesImpl::Algorithm &mode)
{
    // Select cipher based on key size
    switch (mode) {
        case AesImpl::Algorithm::AES_128_GCM:
            return EVP_aes_128_gcm();
        case AesImpl::Algorithm::AES_192_GCM:
            return EVP_aes_192_gcm();
        case AesImpl::Algorithm::AES_256_GCM:
            return EVP_aes_256_gcm();
        default:
            LOG(ERROR) << "Unsupported key size";
            return nullptr;
    }
}
}  // namespace

Status AesImpl::CheckKey()
{
    switch (algorithm_) {
        case AES_128_GCM:
            CHECK_FAIL_RETURN_STATUS(key_.GetSize() == AES_128_GCM_KEY_LEN, K_INVALID, "Invalid key for aes 128 gcm!");
            break;
        case AES_192_GCM:
            CHECK_FAIL_RETURN_STATUS(key_.GetSize() == AES_192_GCM_KEY_LEN, K_INVALID, "Invalid key for aes 192 gcm!");
            break;
        case AES_256_GCM:
            CHECK_FAIL_RETURN_STATUS(key_.GetSize() == AES_256_GCM_KEY_LEN, K_INVALID, "Invalid key for aes 256 gcm!");
            break;
        default:
            RETURN_STATUS_LOG_ERROR(K_INVALID, "Unsupported algorithm!");
    }
    return Status::OK();
}

Status AesImpl::Encrypt(const SensitiveValue &plainText, std::string &cipherText)
{
    LOG(INFO) << "Start encrypt with mode: " << algorithm_;
    RETURN_IF_NOT_OK(CheckKey());
    unsigned char IV[AES_IV_LEN] = { 0 };
    unsigned char tag[AES_TAG_LEN] = { 0 };

    int ret = RAND_priv_bytes(IV, AES_IV_LEN);  // generate bytes string.
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(ret == 1, K_RUNTIME_ERROR, FormatString("Generate iv failed! ret = %d", ret));
    EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();
    auto cipher = GetCipher(algorithm_);
    RETURN_RUNTIME_ERROR_IF_NULL(cipher);
    Raii evpRaii([ctx]() { EVP_CIPHER_CTX_free(ctx); });
    ret = EVP_EncryptInit_ex(ctx, cipher, NULL, (unsigned char *)key_.GetData(), IV);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(ret == 1, K_RUNTIME_ERROR, FormatString("Encrypt init failed! ret = %d", ret));

    auto plainTextSize = plainText.GetSize();
    auto ctxSize = EVP_CIPHER_CTX_block_size(ctx);
    if (static_cast<size_t>(ctxSize) > std::numeric_limits<size_t>::max() - plainTextSize) {
        RETURN_STATUS_LOG_ERROR(K_INVALID, "The sum of plainText and ctx exceeds the size_t boundary.");
    }
    size_t cipherTextLen = plainTextSize + static_cast<size_t>(ctxSize);
    std::unique_ptr<unsigned char[]> encCipher(new unsigned char[cipherTextLen]);
    int encCipherSize = 0;
    int outSize = 0;
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(plainText.GetSize() <= INT_MAX, K_INVALID,
                                         FormatString("The plainText size %zu exceed INT_MAX", plainText.GetSize()));
    int srcSize = static_cast<int>(plainText.GetSize());
    ret = EVP_EncryptUpdate(ctx, encCipher.get(), &outSize, (const unsigned char *)plainText.GetData(), srcSize);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(ret == 1, K_RUNTIME_ERROR,
                                         FormatString("Encrypt update failed! ret = %d", ret));
    if (outSize > 0) {
        encCipherSize = outSize;
    }

    ret = EVP_EncryptFinal_ex(ctx, encCipher.get() + outSize, &outSize);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(ret == 1, K_RUNTIME_ERROR,
                                         FormatString("Encrypt final failed! ret = %d", ret));
    if (outSize > 0) {
        encCipherSize += outSize;
    }

    ret = EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_AEAD_GET_TAG, AES_TAG_LEN, tag);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(ret == 1, K_RUNTIME_ERROR, FormatString("Create tag failed! ret = %d", ret));
    std::string ivStr(reinterpret_cast<char *>(IV), AES_IV_LEN);
    std::string tagStr(reinterpret_cast<char *>(tag), AES_TAG_LEN);
    // Convert to hex string
    cipherText.clear();
    cipherText = EncodeToHexString(ivStr) + ":" + EncodeToHexString(tagStr) + ":";
    cipherText.append((char *)encCipher.get(), encCipherSize);
    LOG(INFO) << "Finish encrypt with mode: " << algorithm_;
    return Status::OK();
}

Status AesImpl::Decrypt(const std::string &cipherText, SensitiveValue &plainText)
{
    LOG(INFO) << "Start decrypt with mode: " << algorithm_;
    RETURN_IF_NOT_OK(CheckKey());

    std::unique_ptr<char[]> ivBytes;
    std::unique_ptr<char[]> tagBytes;
    size_t cipherOffset = 0;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SplitCipherStr(cipherText, ivBytes, tagBytes, cipherOffset),
                                     "Split cipher failed!");
    EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();
    Raii evpRaii([ctx]() { EVP_CIPHER_CTX_free(ctx); });
    auto cipher = GetCipher(algorithm_);
    RETURN_RUNTIME_ERROR_IF_NULL(cipher);
    int ret = EVP_DecryptInit_ex(ctx, cipher, NULL, (unsigned char *)key_.GetData(), (unsigned char *)ivBytes.get());
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(ret == 1, K_RUNTIME_ERROR, FormatString("Decrypt init failed! ret = %d", ret));

    ret = EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_AEAD_SET_TAG, AES_TAG_LEN, tagBytes.get());
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(ret == 1, K_RUNTIME_ERROR,
                                         FormatString("Tag authentication failed! ret = %d", ret));
    int decCipherSize = 0;
    int tempSize = 0;
    size_t decryptedTextLen = cipherText.length();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(cipherText.size() <= INT_MAX, K_INVALID,
                                         FormatString("The cipherText size %zu exceed INT_MAX", plainText.GetSize()));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        cipherText.size() > cipherOffset, K_INVALID,
        FormatString("Invalid cipherOffset %zu, exceed %zu", cipherOffset, cipherText.size()));
    auto copyCipherSize = static_cast<int>(cipherText.size() - cipherOffset);
    std::unique_ptr<char[]> decCipher(new char[decryptedTextLen]);
    ret = EVP_DecryptUpdate(ctx, (unsigned char *)(decCipher.get()), &tempSize,
                            (const unsigned char *)cipherText.data() + cipherOffset, copyCipherSize);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(ret == 1, K_RUNTIME_ERROR,
                                         FormatString("Decrypt update failed! ret = %d", ret));
    if (tempSize > 0) {
        decCipherSize = tempSize;
    }
    ret = EVP_DecryptFinal_ex(ctx, (unsigned char *)decCipher.get() + tempSize, &tempSize);
    if (ret != 1) {
        unsigned long err = ERR_get_error();
        RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, FormatString("Decrypt final failed! ret: %d, err msg: %s", ret,
                                                              ERR_error_string(err, NULL)));
    }
    if (tempSize > 0) {
        decCipherSize += tempSize;
    }

    plainText = SensitiveValue(std::move(decCipher), decCipherSize);
    LOG(INFO) << "Finish decrypt with mode: " << algorithm_;
    return Status::OK();
}

std::string AesImpl::EncodeToHexString(const std::string &str)
{
    std::string hexString = "0123456789abcdef";
    std::string result;
    int headFourBitMove = 4;
    int tailFourBitMove = 0;
    for (size_t i = 0; i < str.size(); i++) {
        result += hexString.at(((unsigned int)str[i] & 0xf0) >> headFourBitMove);
        result += hexString.at(((unsigned int)str[i] & 0x0f) >> tailFourBitMove);
    }
    return result;
}

Status AesImpl::DecodeToString(const char *str, size_t strLen, char *result, size_t resultLen, bool isCap)
{
    std::string hexString = "0123456789abcdef";
    if (isCap) {
        hexString = "0123456789ABCDEF";
    }
    CHECK_FAIL_RETURN_STATUS(str != nullptr, StatusCode::K_INVALID, "Empty encode text!");
    unsigned int nextBitStep = 2;
    CHECK_FAIL_RETURN_STATUS((strLen % BYTE_PRE_HEX == 0) && (strLen > 0)
                                 && (std::numeric_limits<unsigned int>::max() - strLen >= +nextBitStep),
                             StatusCode::K_INVALID,
                             FormatString("The number of text %u for decode must be an even number.", strLen));

    std::string temp;
    int headFourBitMove = 4;
    for (size_t i = 0; i < strLen; i += nextBitStep) {
        auto headPos = hexString.find(str[i]);
        if (headPos == std::string::npos) {
            RETURN_STATUS_LOG_ERROR(K_INVALID, "Decode failed: invalid string!");
        }
        auto pos = hexString.find(str[i + 1]);
        if (pos == std::string::npos) {
            RETURN_STATUS_LOG_ERROR(K_INVALID, "Decode failed: invalid string!");
        }
        temp += static_cast<char>((headPos << headFourBitMove) | pos);
    }
    int ret = memcpy_s(result, resultLen, temp.c_str(), temp.length());
    ClearStr(temp);
    CHECK_FAIL_RETURN_STATUS(ret == EOK, StatusCode::K_RUNTIME_ERROR,
                             FormatString("Copy text failed, the memcpy_s return: %d", ret));
    return Status::OK();
}

Status AesImpl::SplitCipherStr(const std::string &cipher, std::unique_ptr<char[]> &ivBytes,
                               std::unique_ptr<char[]> &tagBytes, size_t &cipherOffset)
{
    auto pos1 = cipher.find(':', 0);
    if (pos1 == std::string::npos) {
        RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, "Incorrect cipher text format!");
    }

    auto ivHexStr = cipher.substr(0, pos1);
    auto ivHexLen = ivHexStr.length() / BYTE_PRE_HEX;
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(ivHexLen == AES_IV_LEN, K_RUNTIME_ERROR,
                                         "The length of the IV does not match the requirements.");
    auto pos2 = cipher.find(':', pos1 + 1);
    if (pos2 == std::string::npos) {
        RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, "Incorrect cipher text format!");
    }

    auto tagHexStr = cipher.substr(pos1 + 1, pos2 - pos1 - 1);
    auto tagBytesLen = tagHexStr.length() / BYTE_PRE_HEX;
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(tagBytesLen == AES_TAG_LEN, K_RUNTIME_ERROR,
                                         "The length of the tag does not match the requirements.");
    cipherOffset = pos2 + 1;
    ivBytes = std::make_unique<char[]>(ivHexLen + 1);
    RETURN_IF_NOT_OK(DecodeToString(ivHexStr.c_str(), ivHexStr.length(), ivBytes.get(), ivHexLen));
    tagBytes = std::make_unique<char[]>(tagBytesLen + 1);
    RETURN_IF_NOT_OK(DecodeToString(tagHexStr.c_str(), tagHexStr.length(), tagBytes.get(), tagBytesLen));
    return Status::OK();
}
}  // namespace datasystem
