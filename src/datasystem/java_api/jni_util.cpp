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
#include "datasystem/java_api/jni_util.h"

#include <iostream>

#include "datasystem/common/util/status_helper.h"
#include "datasystem/utils/sensitive_value.h"
#include "jni.h"

namespace datasystem {
namespace java_api {
void ThrowException(JNIEnv *env, int code, const std::string &msg)
{
    jclass clazz = env->FindClass("org/yuanrong/datasystem/DataSystemException");
    jmethodID initMethod = env->GetMethodID(clazz, "<init>", "(ILjava/lang/String;)V");
    jstring message = env->NewStringUTF(msg.c_str());
    jthrowable exception = (jthrowable)env->NewObject(clazz, initMethod, code, message);
    env->Throw(exception);
    // Release objects references.
    env->DeleteLocalRef(clazz);
    env->DeleteLocalRef(message);
    env->DeleteLocalRef(exception);
}

std::string GetStringField(JNIEnv *env, jclass clazz, jobject object, const char *fieldName)
{
    jfieldID fieldId = env->GetFieldID(clazz, fieldName, "Ljava/lang/String;");
    if (fieldId == nullptr) {
        std::stringstream ss;
        if (fieldName == nullptr) {
            ss << "Error getting String Field object from java object because java object is empty.";
        } else {
            ss << "Field " << fieldName << " not found";
        }
        LOG(ERROR) << ss.str();
        ThrowException(env, -1, ss.str());
        return "";
    } else {
        jstring fieldObject = (jstring)env->GetObjectField(object, fieldId);
        std::string cString = ToString(env, fieldObject);
        env->DeleteLocalRef(fieldObject);
        return cString;
    }
}

SensitiveValue GetSecretField(JNIEnv *env, jclass clazz, jobject object, const char *fieldName)
{
    SensitiveValue ret;
    jfieldID fieldId = env->GetFieldID(clazz, fieldName, "[B");
    if (fieldId == nullptr) {
        std::stringstream ss;
        if (fieldName == nullptr) {
            ss << "Error getting String Field object from java object because java object is empty.";
        } else {
            ss << "Field " << fieldName << " not found";
        }
        LOG(ERROR) << ss.str();
        ThrowException(env, -1, ss.str());
    } else {
        jbyteArray fieldObject = (jbyteArray)env->GetObjectField(object, fieldId);
        ret = ToSensitiveValue(env, fieldObject);
        env->DeleteLocalRef(fieldObject);
    }
    return ret;
}

void CheckFieldId(JNIEnv *env, jfieldID fieldId, const char *fieldName)
{
    if (fieldId == nullptr) {
        std::stringstream ss;
        if (fieldName == nullptr) {
            ss << "Error getting String Field object from java object because java object is empty.";
        } else {
            ss << "Field" << fieldName << "not found";
        }
        LOG(ERROR) << ss.str();
        ThrowException(env, -1, ss.str());
    }
}

int GetIntField(JNIEnv *env, jclass clazz, jobject object, const char *fieldName)
{
    jfieldID fieldId = env->GetFieldID(clazz, fieldName, "I");
    CheckFieldId(env, fieldId, fieldName);
    return env->GetIntField(object, fieldId);
}

bool GetBooleanField(JNIEnv *env, jclass clazz, jobject object, const char *fieldName)
{
    jfieldID fieldId = env->GetFieldID(clazz, fieldName, "Z");
    CheckFieldId(env, fieldId, fieldName);
    return env->GetBooleanField(object, fieldId);
}

int GetLongField(JNIEnv *env, jclass clazz, jobject object, const char *fieldName)
{
    jfieldID fieldId = env->GetFieldID(clazz, fieldName, "J");
    CheckFieldId(env, fieldId, fieldName);
    return env->GetLongField(object, fieldId);
}

std::string ToString(JNIEnv *env, jstring str)
{
    std::string ret;
    if (str != nullptr) {
        jboolean isCopy;
        const char *ptr = env->GetStringUTFChars(str, &isCopy);
        if (ptr != nullptr) {
            ret = ptr;
            if (isCopy) {
                env->ReleaseStringUTFChars(str, ptr);
            }
        }
    }
    return ret;
}

SensitiveValue ToSensitiveValue(JNIEnv *env, jbyteArray byteArray)
{
    SensitiveValue ret;
    if (byteArray != nullptr) {
        jboolean isCopy;
        jsize size = env->GetArrayLength(byteArray);
        jbyte *ptr = env->GetByteArrayElements(byteArray, &isCopy);
        if (ptr != nullptr) {
            ret = SensitiveValue(reinterpret_cast<const char *>(ptr), size);
            if (isCopy) {
                env->ReleaseByteArrayElements(byteArray, ptr, JNI_ABORT);
            }
        }
    }
    return ret;
}

ConnectOptions ToCppConnectOptions(JNIEnv *env, jobject connectOptionsJO)
{
    if (connectOptionsJO == nullptr) {
        return ConnectOptions{};
    }
    jclass connectOptsJC = env->GetObjectClass(connectOptionsJO);
    auto host = GetStringField(env, connectOptsJC, connectOptionsJO, "host");
    auto port = GetIntField(env, connectOptsJC, connectOptionsJO, "port");
    auto connectTimeoutMs = GetIntField(env, connectOptsJC, connectOptionsJO, "connectTimeoutMs");
    auto requestTimeoutMs = GetIntField(env, connectOptsJC, connectOptionsJO, "requestTimeoutMs");
    auto token = GetSecretField(env, connectOptsJC, connectOptionsJO, "token");
    auto clientPublicKey = GetStringField(env, connectOptsJC, connectOptionsJO, "clientPublicKey");
    auto clientPrivateKey = GetSecretField(env, connectOptsJC, connectOptionsJO, "clientPrivateKey");
    auto serverPublicKey = GetStringField(env, connectOptsJC, connectOptionsJO, "serverPublicKey");
    auto accessKey = GetStringField(env, connectOptsJC, connectOptionsJO, "accessKey");
    auto secretKey = GetSecretField(env, connectOptsJC, connectOptionsJO, "secretKey");
    auto tenantId = GetStringField(env, connectOptsJC, connectOptionsJO, "tenantID");
    auto enableCrossNodeConnection = GetBooleanField(env, connectOptsJC, connectOptionsJO, "enableCrossNodeConnection");
    ConnectOptions connectOptions = { .host = std::move(host),
                                      .port = port,
                                      .connectTimeoutMs = connectTimeoutMs,
                                      .requestTimeoutMs = std::move(requestTimeoutMs),
                                      .token = token,
                                      .clientPublicKey = std::move(clientPublicKey),
                                      .clientPrivateKey = std::move(clientPrivateKey),
                                      .serverPublicKey = std::move(serverPublicKey),
                                      .accessKey = std::move(accessKey),
                                      .secretKey = std::move(secretKey),
                                      .tenantId = std::move(tenantId),
                                      .enableCrossNodeConnection = enableCrossNodeConnection };
    env->DeleteLocalRef(connectOptsJC);
    return connectOptions;
}

CreateParam ToCppCreateParam(JNIEnv *env, jobject createParamJO)
{
    CreateParam createParam;
    jclass createParamJC = env->GetObjectClass(createParamJO);
    // get the value of ConsistencyType
    jfieldID consistencyTypeId = env->GetFieldID(createParamJC,
        "consistencyType", "Lorg/yuanrong/datasystem/ConsistencyType;");
    if (consistencyTypeId == nullptr) {
        std::stringstream ss;
    ss << "Failed to get the field ID of the consistencyType member variable of the CreateParam class.";
        LOG(ERROR) << ss.str();
        ThrowException(env, -1, ss.str());
        return createParam;
    } else {
        jobject consistencyTypeJO = env->GetObjectField(createParamJO, consistencyTypeId);
        jclass consistencyTypeJC = env->GetObjectClass(consistencyTypeJO);
        jmethodID ordinalId = env->GetMethodID(consistencyTypeJC, "ordinal", "()I");
        int consistencyType = env->CallIntMethod(consistencyTypeJO, ordinalId);
        createParam.consistencyType = static_cast<ConsistencyType>(consistencyType);
        // Clean up
        env->DeleteLocalRef(consistencyTypeJO);
        env->DeleteLocalRef(consistencyTypeJC);
    }
    env->DeleteLocalRef(createParamJC);
    return createParam;
}

SetParam ToCppSetParam(JNIEnv *env, jobject getParamJO)
{
    SetParam setParam;
    jclass createParamJC = env->GetObjectClass(getParamJO);
    // get the value of WriteMode
    jfieldID writeModeId = env->GetFieldID(createParamJC, "writeMode", "Lorg/yuanrong/datasystem/WriteMode;");
    if (writeModeId == nullptr) {
        std::stringstream ss;
        ss << "Failed to get the field ID of the writeMode member variable of the CreateParam class.";
        LOG(ERROR) << ss.str();
        ThrowException(env, -1, ss.str());
    } else {
        jobject writeModeJO = env->GetObjectField(getParamJO, writeModeId);
        jclass writeModeJC = env->GetObjectClass(writeModeJO);
        jmethodID ordinalId = env->GetMethodID(writeModeJC, "ordinal", "()I");
        int writeMode = env->CallIntMethod(writeModeJO, ordinalId);
        setParam.writeMode = static_cast<WriteMode>(writeMode);
        auto ttlSecond = GetLongField(env, createParamJC, getParamJO, "ttlSecond");
        setParam.ttlSecond = ttlSecond;
        // Clean up
        env->DeleteLocalRef(writeModeJO);
        env->DeleteLocalRef(writeModeJC);
    }
    env->DeleteLocalRef(createParamJC);
    return setParam;
}

jobject ToJavaStringList(JNIEnv *env, std::vector<std::string> &data)
{
    jclass listJC = env->FindClass("java/util/ArrayList");
    jmethodID listInitId = env->GetMethodID(listJC, "<init>", "()V");
    jobject listJO = env->NewObject(listJC, listInitId);
    jmethodID addId = env->GetMethodID(listJC, "add", "(Ljava/lang/Object;)Z");
    jstring stringJO = NULL;
    for (auto &val : data) {
        stringJO = env->NewStringUTF(val.data());
        env->CallBooleanMethod(listJO, addId, stringJO);
    }
    // Clean up
    env->DeleteLocalRef(listJC);
    env->DeleteLocalRef(stringJO);
    return listJO;
}

jobject ToHeapBuffer(JNIEnv *env, const char *value, long size)
{
    if (value == nullptr) {
        std::string ss("The value pointer provided to the ToHeapBuffer function is null.");
        LOG(ERROR) << ss;
        ThrowException(env, -1, ss);
        return 0;
    }
    jclass heapBufferJC = env->FindClass("java/nio/ByteBuffer");
    jmethodID allocateId = env->GetStaticMethodID(heapBufferJC, "allocate", "(I)Ljava/nio/ByteBuffer;");
    jmethodID arrayId = env->GetMethodID(heapBufferJC, "array", "()[B");
    jobject heapBuffer = env->CallStaticObjectMethod(heapBufferJC, allocateId, size);
    if (heapBuffer == nullptr) {
        std::string ss("Failed to allocate HeapByteBuffer When the ToHeapBuffer function is called.");
        LOG(ERROR) << ss;
        ThrowException(env, -1, ss);
        return 0;
    }
    jbyteArray array = static_cast<jbyteArray>(env->CallObjectMethod(heapBuffer, arrayId));
    env->SetByteArrayRegion(array, 0, size, reinterpret_cast<const jbyte *>(value));
    // Clean up
    env->DeleteLocalRef(heapBufferJC);
    env->DeleteLocalRef(array);
    return heapBuffer;
}

Status CopyToBufferNoLatch(JNIEnv *env, jbyteArray byteArray, jlong size, Buffer &buffer)
{
    auto data = env->GetPrimitiveArrayCritical(byteArray, nullptr);
    CHECK_FAIL_RETURN_STATUS(data != nullptr, K_UNKNOWN_ERROR, "can't get data address");
    // no need call WLatch, the other thread cannot change before publish.
    auto status = buffer.MemoryCopy(data, size);
    // JNI_ABORT: free the data without copying back the possible change in data.
    env->ReleasePrimitiveArrayCritical(byteArray, data, JNI_ABORT);
    return status;
}

int GetByteBufferLimit(JNIEnv *env, jobject bufferJO)
{
    jclass bufferJC = env->GetObjectClass(bufferJO);
    jmethodID limitId = env->GetMethodID(bufferJC, "limit", "()I");
    if (limitId == nullptr) {
        std::string msg = "Failed to get the field ID of the limit member variable of the ByteBuffer class.";
        LOG(ERROR) << msg;
        ThrowException(env, -1, msg);
        return 0;
    } else {
        return env->CallIntMethod(bufferJO, limitId);
    }
}
}  // namespace java_api
}  // namespace datasystem