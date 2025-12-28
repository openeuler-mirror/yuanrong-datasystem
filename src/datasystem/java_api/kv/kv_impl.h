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
 * Description: state cache implement for state client.
 */

#include <jni.h>

#include "datasystem/common/log/log.h"
#include "datasystem/client/object_cache/object_client_impl.h"
#include "datasystem/java_api/jni_util.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace java_api {
using datasystem::ConnectOptions;
using datasystem::CreateParam;
using datasystem::object_cache::ObjectClientImpl;

Status SetDirectBufferNativeImpl(JNIEnv *env, jlong handle, jstring keyJO, jobject valueJO, jobject paramJO);

Status SetHeapBufferNativeImpl(JNIEnv *env, jlong handle, jstring keyJO, jbyteArray byteArray, jlong size,
                               jobject paramJO);

Status GetKeyNativeImpl(JNIEnv *env, jlong handle, jstring keyJO, jint timeoutMs, jint &totalSize, jobject &heapBuffer);

Status GetKeysNativeImpl(JNIEnv *env, jlong handle, jobject keysJO, jint timeoutMs, jint &totalSize, jobject &ListJO);

Status DelKeyNativeImpl(JNIEnv *env, jlong handle, jstring keyJO, std::vector<std::string> &failedKeys);

Status DelKeysNativeImpl(JNIEnv *env, jlong handle, jobject keysJO, std::vector<std::string> &failedKeys);
}  // namespace java_api
}  // namespace datasystem