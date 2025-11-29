/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: the key type define
 */

#ifndef DATASYSTEM_COMMON_STRING_INTERN_KEY_TYPE_H
#define DATASYSTEM_COMMON_STRING_INTERN_KEY_TYPE_H

#include <cstddef>
namespace datasystem {
namespace intern {

#define KEY_TYPE_DEF(keyType, keyEnum) keyEnum,
enum class KeyType : size_t {
#include "datasystem/common/string_intern/key_type.def"
};
#undef KEY_TYPE_DEF

}  // namespace intern
}  // namespace datasystem
#endif
