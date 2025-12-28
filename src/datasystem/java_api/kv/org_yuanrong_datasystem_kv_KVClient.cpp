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
 * Description: Jni implement for state client.
 */
#include <string>

#include <jni.h>

#include "datasystem/common/log/log.h"
#include "datasystem/client/object_cache/object_client_impl.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/java_api/jni_util.h"
#include "datasystem/java_api/kv/kv_impl.h"

namespace datasystem {
namespace java_api {
extern "C" {

using datasystem::object_cache::ObjectClientImpl;

JNIEXPORT jlong JNICALL Java_org_yuanrong_datasystem_kv_KVClient_init(JNIEnv *env, jclass, jobject jConnectOpts)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    VLOG(LOG_LEVEL) << "JNICALL StateClient.init";
    ConnectOptions connectOpts = ToCppConnectOptions(env, jConnectOpts);
    if (env->ExceptionOccurred()) {
        LOG(ERROR) << "Exception Occurs when Java_org_yuanrong_datasystem_kv_KVClient_init function to call "
                      "ToCppConnectOptions()";
        return 0;
    }
    auto clientSharedPtr = std::make_shared<ObjectClientImpl>(connectOpts);
    bool needRollbackState;
    auto rc = clientSharedPtr->Init(needRollbackState);
    clientSharedPtr->CompleteHandler(rc.IsError(), needRollbackState);
    JNI_CHECK_RESULT(env, rc, 0);
    auto clientUniquePtr = std::make_unique<std::shared_ptr<ObjectClientImpl>>(std::move(clientSharedPtr));
    return reinterpret_cast<jlong>(clientUniquePtr.release());
}

JNIEXPORT void JNICALL Java_org_yuanrong_datasystem_kv_KVClient_setDirectBufferNative(JNIEnv *env, jclass,
                                                                                        jlong handle, jstring keyJO,
                                                                                        jobject valueJO,
                                                                                        jobject paramJO)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    AccessRecorder accessPoint(AccessRecorderKey::DS_KV_CLIENT_SET);
    VLOG(LOG_LEVEL) << "JNICALL StateClient.setDirectBufferNative";
    Status rc = SetDirectBufferNativeImpl(env, handle, keyJO, valueJO, paramJO);
    RequestParam reqParam;
    reqParam.objectKey = ToString(env, keyJO).substr(0, LOG_OBJECT_KEY_SIZE_LIMIT);
    reqParam.writeMode = std::to_string(static_cast<int>(ToCppSetParam(env, paramJO).writeMode));
    reqParam.ttlSecond = std::to_string(ToCppSetParam(env, paramJO).ttlSecond);
    accessPoint.Record(rc.GetCode(), std::to_string(GetByteBufferLimit(env, valueJO)), reqParam, rc.GetMsg());
    JNI_CHECK_RESULT(env, rc, void(0));
}

JNIEXPORT void JNICALL Java_org_yuanrong_datasystem_kv_KVClient_setHeapBufferNative(JNIEnv *env, jclass, jlong handle,
                                                                                      jstring keyJO,
                                                                                      jbyteArray byteArray, jlong size,
                                                                                      jobject paramJO)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    AccessRecorder accessPoint(AccessRecorderKey::DS_KV_CLIENT_SET);
    VLOG(LOG_LEVEL) << "JNICALL StateClient.setDirectBufferNative";
    Status rc = SetHeapBufferNativeImpl(env, handle, keyJO, byteArray, size, paramJO);
    RequestParam reqParam;
    reqParam.objectKey = ToString(env, keyJO).substr(0, LOG_OBJECT_KEY_SIZE_LIMIT);
    reqParam.writeMode = std::to_string(static_cast<int>(ToCppSetParam(env, paramJO).writeMode));
    reqParam.ttlSecond = std::to_string(ToCppSetParam(env, paramJO).ttlSecond);
    accessPoint.Record(rc.GetCode(), std::to_string(size), reqParam, rc.GetMsg());
    JNI_CHECK_RESULT(env, rc, (void)0);
}

JNIEXPORT jobject JNICALL Java_org_yuanrong_datasystem_kv_KVClient_getKeyNative(JNIEnv *env, jclass, jlong handle,
                                                                                  jstring keyJO, jint timeoutMs)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    AccessRecorder accessPoint(AccessRecorderKey::DS_KV_CLIENT_GET);
    VLOG(LOG_LEVEL) << "JNICALL StateClient.getKeyNative";
    int totalSize = 0;
    jobject heapBuffer = nullptr;
    Status rc = GetKeyNativeImpl(env, handle, keyJO, timeoutMs, totalSize, heapBuffer);
    RequestParam reqParam;
    reqParam.objectKey = ToString(env, keyJO).substr(0, LOG_OBJECT_KEY_SIZE_LIMIT);
    reqParam.timeout = std::to_string(timeoutMs);
    StatusCode code = rc.GetCode() == K_NOT_FOUND ? K_OK : rc.GetCode();
    accessPoint.Record(code, std::to_string(totalSize), reqParam, rc.GetMsg());
    JNI_CHECK_RESULT(env, rc, 0);
    return heapBuffer;
}

JNIEXPORT jobject JNICALL Java_org_yuanrong_datasystem_kv_KVClient_getKeysNative(JNIEnv *env, jclass, jlong handle,
                                                                                   jobject keysJO, jint timeoutMs)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    AccessRecorder accessPoint(AccessRecorderKey::DS_KV_CLIENT_GET);
    VLOG(LOG_LEVEL) << "JNICALL StateClient.getKeysNative";
    int totalSize = 0;
    jobject ListJO = nullptr;
    Status rc = GetKeysNativeImpl(env, handle, keysJO, timeoutMs, totalSize, ListJO);
    std::vector<std::string> keys;
    GetJavaStringListVal(env, keysJO, keys);
    RequestParam reqParam;
    reqParam.objectKey = objectKeysToString(keys);
    reqParam.timeout = std::to_string(timeoutMs);
    StatusCode code = rc.GetCode() == K_NOT_FOUND ? K_OK : rc.GetCode();
    accessPoint.Record(code, std::to_string(totalSize), reqParam, rc.GetMsg());
    JNI_CHECK_RESULT(env, rc, 0);
    return ListJO;
}

JNIEXPORT void JNICALL Java_org_yuanrong_datasystem_kv_KVClient_delKeyNative(JNIEnv *env, jclass, jlong handle,
                                                                               jstring keyJO)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    AccessRecorder accessPoint(AccessRecorderKey::DS_KV_CLIENT_DELETE);
    VLOG(LOG_LEVEL) << "JNICALL StateClient.delKeyNative";
    std::vector<std::string> failedKeys;
    Status rc = DelKeyNativeImpl(env, handle, keyJO, failedKeys);
    RequestParam reqParam;
    reqParam.objectKey = ToString(env, keyJO).substr(0, LOG_OBJECT_KEY_SIZE_LIMIT);
    accessPoint.Record(rc.GetCode(), "0", reqParam, rc.GetMsg());
    JNI_CHECK_RESULT(env, rc, (void)0);
}

JNIEXPORT jobject JNICALL Java_org_yuanrong_datasystem_kv_KVClient_delKeysNative(JNIEnv *env, jclass, jlong handle,
                                                                                   jobject keysJO)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    AccessRecorder accessPoint(AccessRecorderKey::DS_KV_CLIENT_DELETE);
    VLOG(LOG_LEVEL) << "JNICALL StateClient.delKeysNative";
    std::vector<std::string> failedKeys;
    Status rc = DelKeysNativeImpl(env, handle, keysJO, failedKeys);
    std::vector<std::string> keys;
    GetJavaStringListVal(env, keysJO, keys);
    RequestParam reqParam;
    reqParam.objectKey = objectKeysToString(keys);
    accessPoint.Record(rc.GetCode(), "0", reqParam, rc.GetMsg());
    JNI_CHECK_RESULT(env, rc, 0);
    return ToJavaStringList(env, failedKeys);
}

JNIEXPORT jstring JNICALL Java_org_yuanrong_datasystem_kv_KVClient_generateKeyNative(JNIEnv *env, jclass,
                                                                                       jlong handle)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    VLOG(LOG_LEVEL) << "JNICALL StateClient.generateKey";
    auto client = reinterpret_cast<std::shared_ptr<ObjectClientImpl> *>(handle);
    std::string key;
    (void)(*client)->GenerateKey(key);
    return env->NewStringUTF(key.data());
}

JNIEXPORT void JNICALL Java_org_yuanrong_datasystem_kv_KVClient_freeKVClientPtr(JNIEnv *, jclass, jlong handle)
{
    auto client = reinterpret_cast<std::shared_ptr<ObjectClientImpl> *>(handle);
    delete client;
}

}  // namespace java_api
}  // namespace datasystem
}