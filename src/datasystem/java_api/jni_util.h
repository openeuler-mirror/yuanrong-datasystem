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
 * Description: Jni utils
 */
#ifndef DATASYSTEM_JAVA_API_JNI_UTIL_H
#define DATASYSTEM_JAVA_API_JNI_UTIL_H

#include <jni.h>

#include <string>
#include <sstream>

#include "datasystem/object/buffer.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/object_client.h"
#include "datasystem/kv_client.h"

#define JNI_CHECK_RESULT(env, rc, ret)                                                \
    do {                                                                              \
        auto rc_ = (rc);                                                              \
        if (rc_.IsError()) {                          \
            datasystem::java_api::ThrowException((env), rc_.GetCode(), rc_.GetMsg()); \
            return (ret);                                                             \
        }                                                                             \
    } while (false)

namespace datasystem {
namespace java_api {

using datasystem::ConnectOptions;
using datasystem::CreateParam;

constexpr int LOG_LEVEL = 1;  // Normal output log level

/**
 * @brief Throw an exception to jvm.
 * @param[in] env The JNI environment.
 * @param[in] code The error code.
 * @param[in] msg The error message.
 */
void ThrowException(JNIEnv *env, int code, const std::string &msg);

/**
 * @brief Convert jstring to std::string.
 * @param[in] env The JNI environment.
 * @param[in] str The java string object.
 * @return std::string.
 */
std::string ToString(JNIEnv *env, jstring str);

/**
 * @brief Convert jbyteArray to SensitiveValue.
 * @param[in] env The JNI environment.
 * @param[in] byteArray The java jbyteArray object.
 * @return std::string.
 */
SensitiveValue ToSensitiveValue(JNIEnv *env, jbyteArray byteArray);

/**
 * @brief Get the String Field object from a java object.
 * @param[in] env The JNI environment.
 * @param[in] clazz The class of the java object.
 * @param[in] object The java object.
 * @param[in] fieldName The field name.
 * @return std::string.
 */
std::string GetStringField(JNIEnv *env, jclass clazz, jobject object, const char *fieldName);

/**
 * @brief Check field ID.
 * @param[in] env The JNI environment.
 * @param[in] fieldId The field ID.
 * @param[in] fieldName The field name.
 */
void CheckFieldId(JNIEnv *env, jfieldID fieldId, const char *fieldName);

/**
 * @brief Get the int Field from a java object.
 * @param[in] env The JNI environment.
 * @param[in] clazz The class of the java object.
 * @param[in] object The java object.
 * @param[in] fieldName The field name.
 * @return  int.
 */
int GetIntField(JNIEnv *env, jclass clazz, jobject object, const char *fieldName);

/**
 * @brief Get the Long Field from a java object.
 * @param[in] env The JNI environment.
 * @param[in] clazz The class of the java object.
 * @param[in] object The java object.
 * @param[in] fieldName The field name.
 * @return  int.
 */
int GetLongField(JNIEnv *env, jclass clazz, jobject object, const char *fieldName);

/**
 * @brief Convert the List<String> type data passed from java to the set, unordered_set or vector type data of c++.
 * @param[in] env The JNI environment.
 * @param[in] objectJO The java object.
 * @param[out] listVal The set, unordered_set or vector type variable.
 */
template <typename T>
void GetJavaStringListVal(JNIEnv *env, jobject objectJO, T &listVal)
{
    if (objectJO == nullptr) {
        return;
    }
    listVal.clear();
    jclass objectJC = env->GetObjectClass(objectJO);
    jmethodID getId = env->GetMethodID(objectJC, "get", "(I)Ljava/lang/Object;");
    jmethodID sizeId = env->GetMethodID(objectJC, "size", "()I");
    jint len = env->CallIntMethod(objectJO, sizeId);
    // Check len. Based on the service scenario, the maximum value is tentatively set at 1 million.
    int maxLimit = 1000000;
    if (len > maxLimit) {
        std::stringstream ss;
        ss << "The length of the provided java list exceeds the maximum limit, which is" << maxLimit;
        LOG(ERROR) << ss.str();
        ThrowException(env, -1, ss.str());
        return;
    }
    for (int i = 0; i < len; i++) {
        jstring objectKeyJO = (jstring)env->CallObjectMethod(objectJO, getId, i);
        std::string objectKey = ToString(env, objectKeyJO);
        (void)listVal.insert(listVal.end(), objectKey);
    }
    // Clean up
    env->DeleteLocalRef(objectJC);
}

/**
 * @brief Convert the java ConnectOptions to c++ ConnectOptions.
 * @param[in] env The JNI environment.
 * @param[in] connectOptionsJO The java ConnectOptions object.
 * @return ConnectOptions.
 */
ConnectOptions ToCppConnectOptions(JNIEnv *env, jobject connectOptionsJO);

/**
 * @brief Convert the java CreateParam to c++ CreateParam.
 * @param[in] env The JNI environment.
 * @param[in] createParamJO The java CreateParam object.
 * @return CreateParam
 */
CreateParam ToCppCreateParam(JNIEnv *env, jobject createParamJO);

/**
 * @brief Convert the java SetParam to c++ SetParam.
 * @param[in] env The JNI environment.
 * @param[in] getParamJO The java SetParam object.
 * @return SetParam
 */
SetParam ToCppSetParam(JNIEnv *env, jobject getParamJO);

/**
 * @brief Convert the std::vector<std::string> type data of c++ to the List<String> type data of Java.
 * @param[in] env The JNI environment.
 * @param[in] data The std::vector<std::string> type data.
 * @return The List<String> type data.
 */
jobject ToJavaStringList(JNIEnv *env, std::vector<std::string> &data);

/**
 * @brief Use the provided pointer and size of data to construct the Java heapBuffer and return it.
 * @param[in] env The JNI environment.
 * @param[in] value The pointer of data.
 * @param[in] size The size of data.
 * @return The HeapByteBuffer of java.
 */
jobject ToHeapBuffer(JNIEnv *env, const char *value, long size);

/**
 * @brief Copy java bytes to buffer.
 * @param[in] env The JNI environment.
 * @param[in] byteArray java bytes
 * @param[in] size The size of java bytes.
 * @param[out] buffer The object buffer.
 * @return Status of the call.
 */
Status CopyToBufferNoLatch(JNIEnv *env, jbyteArray byteArray, jlong size, Buffer &buffer);

/**
 * @brief To get the limit field of the ByteBuffer.
 * @param[in] env The JNI environment.
 * @param[in] bufferJO The java ByteBuffer object.
 * @return The value of ByteBuffer.limit().
 */
int GetByteBufferLimit(JNIEnv *env, jobject bufferJO);
}  // namespace java_api
}  // namespace datasystem
#endif