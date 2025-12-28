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
#include "datasystem/java_api/kv/kv_impl.h"

#include <string>
#include "datasystem/common/util/status_helper.h"
#include "datasystem/utils/status.h"
#include "datasystem/client/object_cache/object_client_impl.h"

namespace datasystem {
namespace java_api {

Status SetDirectBufferNativeImpl(JNIEnv *env, jlong handle, jstring keyJO, jobject valueJO, jobject paramJO)
{
    auto client = reinterpret_cast<std::shared_ptr<ObjectClientImpl> *>(handle);
    std::string key = ToString(env, keyJO);
    auto body = env->GetDirectBufferAddress(valueJO);
    if (!body) {
        std::string msg = "cant get data address";
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, msg);
    }
    SetParam param = ToCppSetParam(env, paramJO);
    if (env->ExceptionOccurred()) {
        std::string msg =
            "Exception Occurs when Java_org_yuanrong_datasystem_statecache_StateClient_setDirectBufferNative function "
            "to call ToCppSetParam()";
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, msg);
    } else {
        datasystem::object_cache::FullParam fullParam;
        fullParam.writeMode = param.writeMode;
        fullParam.ttlSecond = param.ttlSecond;
        fullParam.cacheType = param.cacheType;
        fullParam.consistencyType = ConsistencyType::CAUSAL;
        int limit = GetByteBufferLimit(env, valueJO);
        RETURN_IF_NOT_OK(
            (*client)->Put(key, static_cast<const uint8_t *>(body), limit, fullParam, {}, param.ttlSecond));
    }
    body = NULL;
    return Status::OK();
}

Status SetHeapBufferNativeImpl(JNIEnv *env, jlong handle, jstring keyJO, jbyteArray byteArray, jlong size,
                               jobject paramJO)
{
    auto client = reinterpret_cast<std::shared_ptr<ObjectClientImpl> *>(handle);
    std::string key = ToString(env, keyJO);
    SetParam param = ToCppSetParam(env, paramJO);
    datasystem::object_cache::FullParam fullParam;
    if (env->ExceptionOccurred()) {
        std::string msg =
            "Exception Occurs when Java_org_yuanrong_datasystem_statecache_StateClient_setDirectBufferNative function "
            "to call ToCppSetParam()";
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, msg);
    } else {
        fullParam.writeMode = param.writeMode;
        fullParam.consistencyType = ConsistencyType::CAUSAL;
        fullParam.cacheType = param.cacheType;
        fullParam.ttlSecond = param.ttlSecond;
    }
    // When putting HeapByteBuffer data, if the code uses the shared memory logic, the set interface is copy-free.
    if ((*client)->ShmCreateable(size)) {
        std::shared_ptr<Buffer> buffer;
        RETURN_IF_NOT_OK((*client)->Create(key, size, fullParam, buffer));
        // no need call WLatch, the other thread cannot change before publish.
        RETURN_IF_NOT_OK(CopyToBufferNoLatch(env, byteArray, size, *buffer));
        RETURN_IF_NOT_OK(buffer->Publish());
    } else {  // if the code uses the non-shared memory logic, the set interface copies data once more.
        jbyte *bytes = env->GetByteArrayElements(byteArray, 0);
        RETURN_IF_NOT_OK((*client)->Put(key, reinterpret_cast<const uint8_t *>(bytes), size, fullParam, {}));
        env->ReleaseByteArrayElements(byteArray, bytes, 0);
    }
    return Status::OK();
}

Status GetKeyNativeImpl(JNIEnv *env, jlong handle, jstring keyJO, jint timeoutMs, jint &totalSize, jobject &heapBuffer)
{
    auto client = reinterpret_cast<std::shared_ptr<ObjectClientImpl> *>(handle);
    std::string key = ToString(env, keyJO);
    std::vector<Optional<Buffer>> buffers;
    RETURN_IF_NOT_OK((*client)->Get({ key }, timeoutMs, buffers));
    RETURN_IF_NOT_OK(buffers[0]->RLatch());
    heapBuffer = ToHeapBuffer(env, reinterpret_cast<const char *>(buffers[0]->ImmutableData()), buffers[0]->GetSize());
    RETURN_IF_NOT_OK(buffers[0]->UnRLatch());
    if (env->ExceptionOccurred()) {
        std::string msg =
            "Exception Occurs when Java_org_yuanrong_datasystem_statecache_StateClient_get__JLjava_lang_String_2 "
            "function to call ToHeapBuffer()";
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, msg);
    } else {
        totalSize += buffers[0]->GetSize();
    }
    return Status::OK();
}

Status GetKeysNativeImpl(JNIEnv *env, jlong handle, jobject keysJO, jint timeoutMs, jint &totalSize, jobject &ListJO)
{
    auto client = reinterpret_cast<std::shared_ptr<ObjectClientImpl> *>(handle);
    std::vector<std::string> keys;
    GetJavaStringListVal(env, keysJO, keys);
    std::vector<Optional<Buffer>> buffers;
    RETURN_IF_NOT_OK((*client)->Get(keys, timeoutMs, buffers));
    jclass listJC = env->FindClass("java/util/ArrayList");
    jmethodID initId = env->GetMethodID(listJC, "<init>", "()V");
    ListJO = env->NewObject(listJC, initId);
    jmethodID addId = env->GetMethodID(listJC, "add", "(Ljava/lang/Object;)Z");
    jobject byteBuffer = NULL;
    for (auto &buffer : buffers) {
        if (buffer) {
            RETURN_IF_NOT_OK(buffer->RLatch());
            byteBuffer = ToHeapBuffer(env, reinterpret_cast<const char *>(buffer->ImmutableData()), buffer->GetSize());
            RETURN_IF_NOT_OK(buffer->UnRLatch());
            if (env->ExceptionOccurred()) {
                std::string msg =
                    "Exception Occurs when Java_org_yuanrong_datasystem_statecache_StateClient_getKeysNative "
                    "function to call ToHeapBuffer()";
                RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, msg);
            }
            totalSize += buffer->GetSize();
        } else {
            byteBuffer = nullptr;
        }
        env->CallBooleanMethod(ListJO, addId, byteBuffer);
    }
    // Clean up
    env->DeleteLocalRef(listJC);
    env->DeleteLocalRef(byteBuffer);
    return Status::OK();
}

Status DelKeyNativeImpl(JNIEnv *env, jlong handle, jstring keyJO, std::vector<std::string> &failedKeys)
{
    auto client = reinterpret_cast<std::shared_ptr<ObjectClientImpl> *>(handle);
    std::string key = ToString(env, keyJO);
    RETURN_IF_NOT_OK((*client)->Delete({ key }, failedKeys));
    if (!failedKeys.empty()) {
        std::string msg = "The failed key is not empty, delete key failed!";
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, msg);
    }
    return Status::OK();
}

Status DelKeysNativeImpl(JNIEnv *env, jlong handle, jobject keysJO, std::vector<std::string> &failedKeys)
{
    auto client = reinterpret_cast<std::shared_ptr<ObjectClientImpl> *>(handle);
    std::vector<std::string> keys;
    GetJavaStringListVal(env, keysJO, keys);
    RETURN_IF_NOT_OK((*client)->Delete(keys, failedKeys));
    return Status::OK();
}
}  // namespace java_api
}  // namespace datasystem