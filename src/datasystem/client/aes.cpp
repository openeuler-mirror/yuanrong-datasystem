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
#include "datasystem/utils/aes.h"
#include "datasystem/common/aes/aes_impl.h"

namespace datasystem {
Aes::Aes(SensitiveValue key, Aes::Algorithm algorithm)
{
    impl_ = std::make_shared<AesImpl>(key, static_cast<AesImpl::Algorithm>(algorithm));
}

Status Aes::Encrypt(const SensitiveValue &plainText, std::string &cipherText)
{
    return impl_->Encrypt(plainText, cipherText);
}

Status Aes::Decrypt(const std::string &cipherText, SensitiveValue &plainText)
{
    return impl_->Decrypt(cipherText, plainText);
}
}  // namespace datasystem