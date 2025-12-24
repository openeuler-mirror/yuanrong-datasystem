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
 * Description: Jni implement for state client.
 */
#include <string>

#include <jni.h>

#include "datasystem/context/context.h"
#include "datasystem/java_api/jni_util.h"

namespace datasystem {
namespace java_api {
extern "C" {

using datasystem::Context;

JNIEXPORT void JNICALL Java_org_yuanrong_datasystem_Context_setTraceId(JNIEnv *env, jclass, jstring jTraceId)
{
    std::string traceId = ToString(env, jTraceId);
    JNI_CHECK_RESULT(env, Context::SetTraceId(traceId), (void)0);
}
}  // namespace java_api
}  // namespace datasystem
}