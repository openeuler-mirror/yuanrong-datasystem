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
 * Description: Jni implement for object client.
 */

#include <jni.h>

#include "datasystem/client/object_cache/object_client_impl.h"
#include "datasystem/common/log/log.h"
#include "datasystem/java_api/jni_util.h"
#include "datasystem/common/log/trace.h"

namespace datasystem {
namespace java_api {
extern "C" {

using datasystem::Buffer;
using datasystem::CreateParam;
using datasystem::object_cache::ObjectClientImpl;

JNIEXPORT jlong JNICALL Java_org_yuanrong_datasystem_object_ObjectClient_init(JNIEnv *env, jclass, jobject jConnectOpts)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    VLOG(LOG_LEVEL) << "JNICALL ObjectClient.init";
    ConnectOptions connectOpts = ToCppConnectOptions(env, jConnectOpts);
    if (env->ExceptionOccurred()) {
        return 0;
    }
    auto clientSharedPtr = std::make_shared<ObjectClientImpl>(connectOpts);
    bool needRollbackState;
    auto rc = clientSharedPtr->Init(needRollbackState, true);
    clientSharedPtr->CompleteHandler(rc.IsError(), needRollbackState);
    JNI_CHECK_RESULT(env, rc, 0);
    auto clientUniquePtr = std::make_unique<std::shared_ptr<ObjectClientImpl>>(std::move(clientSharedPtr));
    return reinterpret_cast<jlong>(clientUniquePtr.release());
}

JNIEXPORT jlong JNICALL Java_org_yuanrong_datasystem_object_ObjectClient_createNative(JNIEnv *env, jclass, jlong handle,
                                                                                  jstring objectKeyJO, jint size,
                                                                                  jobject CreateParamJO)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    VLOG(LOG_LEVEL) << "JNICALL ObjectClient.create";
    auto client = reinterpret_cast<std::shared_ptr<ObjectClientImpl> *>(handle);
    std::string objectKey = ToString(env, objectKeyJO);
    CreateParam createParam = ToCppCreateParam(env, CreateParamJO);
    if (env->ExceptionOccurred()) {
        return 0;
    }
    object_cache::FullParam innerParam;
    innerParam.writeMode = WriteMode::NONE_L2_CACHE;
    innerParam.consistencyType = createParam.consistencyType;
    innerParam.cacheType = createParam.cacheType;
    // The purpose of using make_unique is to use the release() function to prevent the buffer from being destructed.
    auto buffer = std::make_unique<std::shared_ptr<Buffer>>();
    JNI_CHECK_RESULT(env, (*client)->Create(objectKey, size, innerParam, *buffer), 0);
    return reinterpret_cast<jlong>(buffer.release());
}

JNIEXPORT void JNICALL Java_org_yuanrong_datasystem_object_ObjectClient_putDirectBufferNative(
    JNIEnv *env, jclass, jlong handle, jstring objectKeyJO, jobject bufferJO, jobject CreateParamJO,
    jobject nestedObjectKeysJO)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    VLOG(LOG_LEVEL) << "JNICALL ObjectClient.put";
    auto client = reinterpret_cast<std::shared_ptr<ObjectClientImpl> *>(handle);
    std::string objectKey = ToString(env, objectKeyJO);
    CreateParam createParam = ToCppCreateParam(env, CreateParamJO);
    object_cache::FullParam innerParam;
    innerParam.writeMode = WriteMode::NONE_L2_CACHE;
    innerParam.consistencyType = createParam.consistencyType;
    innerParam.cacheType = createParam.cacheType;
    auto body = env->GetDirectBufferAddress(bufferJO);
    if (!body) {
        ThrowException(env, -1, "cant get data address");
        return;
    }
    // get nestedObjectKeys passed from java
    std::unordered_set<std::string> nestedObjectKeys;
    GetJavaStringListVal(env, nestedObjectKeysJO, nestedObjectKeys);
    int limit = GetByteBufferLimit(env, bufferJO);
    if (env->ExceptionOccurred()) {
        return;
    }
    JNI_CHECK_RESULT(env, (*client)->Put(objectKey, static_cast<uint8_t *>(body), limit, innerParam, nestedObjectKeys),
                     (void)0);
    body = NULL;
}

JNIEXPORT void JNICALL Java_org_yuanrong_datasystem_object_ObjectClient_putHeapBufferNative(
    JNIEnv *env, jclass, jlong handle, jstring objectKeyJO, jbyteArray byteArray, jlong size, jobject CreateParamJO,
    jobject nestedObjectKeysJO)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    VLOG(LOG_LEVEL) << "JNICALL ObjectClient.putHeapBufferNative";
    auto client = reinterpret_cast<std::shared_ptr<ObjectClientImpl> *>(handle);
    std::string objectKey = ToString(env, objectKeyJO);
    CreateParam createParam = ToCppCreateParam(env, CreateParamJO);
    object_cache::FullParam innerParam;
    innerParam.writeMode = WriteMode::NONE_L2_CACHE;
    innerParam.consistencyType = createParam.consistencyType;
    innerParam.cacheType = createParam.cacheType;
    std::unordered_set<std::string> nestedObjectKeys;
    GetJavaStringListVal(env, nestedObjectKeysJO, nestedObjectKeys);
    if (env->ExceptionOccurred()) {
        LOG(ERROR) << "Exception Occurs when \
                       Java_org_yuanrong_datasystem_object_ObjectClient_putHeapBufferNative function "
                      "to be called";
        return (void)0;
    }
    // When putting HeapByteBuffer data, if the code uses the shared memory logic, the put interface is copy-free.
    if ((*client)->ShmCreateable(size)) {
        std::shared_ptr<Buffer> buffer;
        JNI_CHECK_RESULT(env, (*client)->Create(objectKey, size, innerParam, buffer), (void)0);
        // no need call WLatch, the other thread cannot change before publish.
        JNI_CHECK_RESULT(env, CopyToBufferNoLatch(env, byteArray, size, *buffer), (void)0);
        JNI_CHECK_RESULT(env, buffer->Publish(), (void)0);
    } else {  // if the code uses the non-shared memory logic, the put interface copies data once more.
        jbyte *bytes = env->GetByteArrayElements(byteArray, 0);
        auto rc =
            (*client)->Put(objectKey, reinterpret_cast<const uint8_t *>(bytes), size, innerParam, nestedObjectKeys);
        env->ReleaseByteArrayElements(byteArray, bytes, 0);
        if (rc.IsError()) {
            datasystem::java_api::ThrowException((env), rc.GetCode(), rc.GetMsg());
        }
    }
}

JNIEXPORT jobject JNICALL Java_org_yuanrong_datasystem_object_ObjectClient_getNative(JNIEnv *env, jclass, jlong handle,
                                                                                 jobject objectKeysJO, jint timeoutMs)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    VLOG(LOG_LEVEL) << "JNICALL ObjectClient.get";
    auto client = reinterpret_cast<std::shared_ptr<ObjectClientImpl> *>(handle);
    // get objectKeys passed from java
    std::vector<std::string> objectKeys;
    GetJavaStringListVal(env, objectKeysJO, objectKeys);
    // get the value of buffers
    std::vector<Optional<Buffer>> buffers;
    JNI_CHECK_RESULT(env, (*client)->Get(objectKeys, timeoutMs, buffers), 0);
    // Construct Java Buffer object and return a List<Buffer> object
    jclass listJC = env->FindClass("java/util/ArrayList");
    jmethodID listInitId = env->GetMethodID(listJC, "<init>", "()V");
    jobject listJO = env->NewObject(listJC, listInitId);
    jmethodID addId = env->GetMethodID(listJC, "add", "(Ljava/lang/Object;)Z");
    jclass bufferJC = env->FindClass("org/yuanrong/datasystem/object/BufferImpl");
    jmethodID bufferInitId = env->GetMethodID(bufferJC, "<init>", "(J)V");

    jobject bufferJO = NULL;
    for (auto &buffer : buffers) {
        if (buffer) {
            // The purpose of converting to make_unique is to use the release() function to prevent the buffer from
            // being destructed.
            auto bufferShare = std::make_shared<Buffer>(std::move(*buffer));
            auto bufferUnique = std::make_unique<std::shared_ptr<Buffer>>(std::move(bufferShare));
            bufferJO = env->NewObject(bufferJC, bufferInitId, bufferUnique.release());
            env->CallBooleanMethod(listJO, addId, bufferJO);
            env->DeleteLocalRef(bufferJO);
        } else {
            env->CallBooleanMethod(listJO, addId, nullptr);
        }
    }
    // Clean up
    env->DeleteLocalRef(listJC);
    env->DeleteLocalRef(bufferJC);
    env->DeleteLocalRef(bufferJO);
    return listJO;
}

JNIEXPORT jobject JNICALL Java_org_yuanrong_datasystem_object_ObjectClient_gIncreaseRefNative(JNIEnv *env, jclass,
                                                                                          jlong handle,
                                                                                          jobject objectKeysJO)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    VLOG(LOG_LEVEL) << "JNICALL ObjectClient.gIncreaseRef";
    auto client = reinterpret_cast<std::shared_ptr<ObjectClientImpl> *>(handle);
    // get objectKeys passed from java
    std::vector<std::string> objectKeys;
    GetJavaStringListVal(env, objectKeysJO, objectKeys);
    // get the fail delete object list.
    std::vector<std::string> failedObjectKeys;
    JNI_CHECK_RESULT(env, (*client)->GIncreaseRef(objectKeys, failedObjectKeys), 0);
    return ToJavaStringList(env, failedObjectKeys);
}

JNIEXPORT jobject JNICALL Java_org_yuanrong_datasystem_object_ObjectClient_gDecreaseRefNative(JNIEnv *env, jclass,
                                                                                          jlong handle,
                                                                                          jobject objectKeysJO)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    VLOG(LOG_LEVEL) << "JNICALL ObjectClient.gDecreaseRef";
    auto client = reinterpret_cast<std::shared_ptr<ObjectClientImpl> *>(handle);
    // get objectKeys passed from java
    std::vector<std::string> objectKeys;
    GetJavaStringListVal(env, objectKeysJO, objectKeys);
    // get the success delete object list.
    std::vector<std::string> failedObjectKeys;
    JNI_CHECK_RESULT(env, (*client)->GDecreaseRef(objectKeys, failedObjectKeys), 0);
    return ToJavaStringList(env, failedObjectKeys);
}

JNIEXPORT jint JNICALL Java_org_yuanrong_datasystem_object_ObjectClient_queryGlobalRefNumNative(JNIEnv *env, jclass,
                                                                                            jlong handle,
                                                                                            jstring objectKeyJO)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    VLOG(LOG_LEVEL) << "JNICALL ObjectClient.queryGlobalRefNum";
    auto client = reinterpret_cast<std::shared_ptr<ObjectClientImpl> *>(handle);
    std::string objectKey = ToString(env, objectKeyJO);
    return (*client)->QueryGlobalRefNum(objectKey);
}

JNIEXPORT void JNICALL Java_org_yuanrong_datasystem_object_ObjectClient_freeObjectClientPtr(JNIEnv *,
    jclass, jlong handle)
{
    auto client = reinterpret_cast<std::shared_ptr<ObjectClientImpl> *>(handle);
    delete client;
}

JNIEXPORT void JNICALL Java_com_datasystem_object_ObjectClient_updateTokenNative(JNIEnv *env, jclass,
    jlong handle, jbyteArray tokenBytes)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    VLOG(LOG_LEVEL) << "JNICALL ObjectClient.updateToken";
    auto client = reinterpret_cast<std::shared_ptr<ObjectClientImpl> *>(handle);
    SensitiveValue token = ToSensitiveValue(env, tokenBytes);
    JNI_CHECK_RESULT(env, (*client)->UpdateToken(token), (void)0);
}

JNIEXPORT void JNICALL Java_com_datasystem_object_ObjectClient_updateAkSkNative(JNIEnv *env, jclass,
    jlong handle, jstring accessKeyJO, jbyteArray secretKeyBytes)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    VLOG(LOG_LEVEL) << "JNICALL ObjectClient.updateAkSk";
    auto client = reinterpret_cast<std::shared_ptr<ObjectClientImpl> *>(handle);
    std::string accessKey = ToString(env, accessKeyJO);
    SensitiveValue secretKey = ToSensitiveValue(env, secretKeyBytes);
    JNI_CHECK_RESULT(env, (*client)->UpdateAkSk(accessKey, secretKey), (void)0);
}
}  // namespace java_api
}  // namespace datasystem
}