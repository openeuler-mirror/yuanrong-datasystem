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

/**
 * Description: a dummy encrypt kit implement, means no encrypt
 */

#include "datasystem/common/log/log.h"
#include "datasystem/common/encrypt/encrypt_kit.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"

namespace datasystem {
Status EncryptKit::LoadSecretKeys()
{
    RETURN_STATUS_LOG_ERROR(K_INVALID, "Invalid invoke, not support encrypt.");
}

Status EncryptKit::GenerateRootKey()
{
    RETURN_STATUS_LOG_ERROR(K_INVALID, "Invalid invoke, not support encrypt.");
}

Status EncryptKit::Decrypt(const std::string &cipher, std::unique_ptr<char[]> &plainText, int &outSize)
{
    std::string realCipher = RemoveNewlineOfStr(cipher);
    outSize = static_cast<int>(realCipher.size());
    plainText = std::make_unique<char[]>(outSize + 1);
    int ret = memcpy_s(plainText.get(), outSize + 1, realCipher.c_str(), outSize);
    if (ret != EOK) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "memcpy failed");
    }
    ClearStr(realCipher);
    return Status::OK();
}

Status EncryptKit::Encrypt(const std::string &plaintText, std::string &cipher)
{
    cipher = plaintText;
    return Status::OK();
}

Status EncryptKit::DestroyKeyFactor()
{
    RETURN_STATUS_LOG_ERROR(K_INVALID, "Invalid invoke, not support encrypt.");
}

Status EncryptKit::DestroyRootKey()
{
    RETURN_STATUS_LOG_ERROR(K_INVALID, "Invalid invoke, not support encrypt.");
}

bool EncryptKit::CheckPreCondition()
{
    return true;
}

bool EncryptKit::IsActive()
{
    return false;
}

Status EncryptKit::GenerateAllKeyComponent(const std::string &k1Path, const std::string &k2Path,
                                           const std::string &k3Path, const std::string &saltPath)
{
    (void)k1Path;
    (void)k2Path;
    (void)k3Path;
    (void)saltPath;
    RETURN_STATUS_LOG_ERROR(K_INVALID, "Invalid invoke, not support encrypt.");
}
}  // namespace datasystem