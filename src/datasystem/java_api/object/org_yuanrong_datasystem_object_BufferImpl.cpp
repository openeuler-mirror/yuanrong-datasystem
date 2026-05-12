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
 * Description: Jni implement for Buffer.
 */

#include <jni.h>

#include "datasystem/client/object_cache/object_client_impl.h"
#include "datasystem/common/log/log.h"
#include "datasystem/java_api/jni_util.h"

using datasystem::Buffer;

namespace datasystem {
namespace java_api {
extern "C" {
JNIEXPORT void JNICALL Java_org_yuanrong_datasystem_object_BufferImpl_publishNative(JNIEnv *env, jclass, jlong handle,
                                                                                jobject nestedObjectKeysJO)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    VLOG(LOG_LEVEL) << "JNICALL BufferImpl.publish";
    auto buffer = reinterpret_cast<std::shared_ptr<Buffer> *>(handle);
    if (nestedObjectKeysJO == nullptr) {
        JNI_CHECK_RESULT(env, (*buffer)->Publish(), (void)0);
    } else {
        std::unordered_set<std::string> nestedObjectKeys;
        GetJavaStringListVal(env, nestedObjectKeysJO, nestedObjectKeys);
        JNI_CHECK_RESULT(env, (*buffer)->Publish(nestedObjectKeys), (void)0);
    }
}

JNIEXPORT void JNICALL Java_org_yuanrong_datasystem_object_BufferImpl_sealNative(JNIEnv *env, jclass, jlong handle,
                                                                             jobject nestedObjectKeysJO)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    VLOG(LOG_LEVEL) << "JNICALL BufferImpl.seal";
    auto buffer = reinterpret_cast<std::shared_ptr<Buffer> *>(handle);
    // get nestedObjectKeys passed from java
    if (nestedObjectKeysJO == nullptr) {
        JNI_CHECK_RESULT(env, (*buffer)->Seal(), (void)0);
    } else {
        std::unordered_set<std::string> nestedObjectKeys;
        GetJavaStringListVal(env, nestedObjectKeysJO, nestedObjectKeys);
        JNI_CHECK_RESULT(env, (*buffer)->Seal(nestedObjectKeys), (void)0);
    }
}

JNIEXPORT void JNICALL Java_org_yuanrong_datasystem_object_BufferImpl_wLatchNative(JNIEnv *env, jclass, jlong handle,
                                                                               jint timeoutSec)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    VLOG(LOG_LEVEL) << "JNICALL BufferImpl.wLatch";
    auto buffer = reinterpret_cast<std::shared_ptr<Buffer> *>(handle);
    JNI_CHECK_RESULT(env, (*buffer)->WLatch(timeoutSec), (void)0);
}

JNIEXPORT void JNICALL Java_org_yuanrong_datasystem_object_BufferImpl_rLatchNative(JNIEnv *env, jclass, jlong handle,
                                                                               jint timeoutSec)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    VLOG(LOG_LEVEL) << "JNICALL BufferImpl.rLatch";
    auto buffer = reinterpret_cast<std::shared_ptr<Buffer> *>(handle);
    JNI_CHECK_RESULT(env, (*buffer)->RLatch(timeoutSec), (void)0);
}

JNIEXPORT void JNICALL Java_org_yuanrong_datasystem_object_BufferImpl_unWLatchNative(JNIEnv *env, jclass, jlong handle)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    VLOG(LOG_LEVEL) << "JNICALL BufferImpl.unWLatch";
    auto buffer = reinterpret_cast<std::shared_ptr<Buffer> *>(handle);
    JNI_CHECK_RESULT(env, (*buffer)->UnWLatch(), (void)0);
}

JNIEXPORT void JNICALL Java_org_yuanrong_datasystem_object_BufferImpl_unRLatchNative(JNIEnv *env, jclass, jlong handle)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    VLOG(LOG_LEVEL) << "JNICALL BufferImpl.unRLatch";
    auto buffer = reinterpret_cast<std::shared_ptr<Buffer> *>(handle);
    JNI_CHECK_RESULT(env, (*buffer)->UnRLatch(), (void)0);
}

JNIEXPORT void JNICALL Java_org_yuanrong_datasystem_object_BufferImpl_memoryCopyDirectBuffer(JNIEnv *env, jclass,
                                                                                         jlong handle, jobject bufferJO)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    VLOG(LOG_LEVEL) << "JNICALL BufferImpl.memoryCopy";
    auto buffer = reinterpret_cast<std::shared_ptr<Buffer> *>(handle);
    auto body = env->GetDirectBufferAddress(bufferJO);
    if (!body) {
        ThrowException(env, -1, "cant get data address");
        return;
    }
    int limit = GetByteBufferLimit(env, bufferJO);
    if (env->ExceptionOccurred()) {
        return;
    }
    JNI_CHECK_RESULT(env, (*buffer)->MemoryCopy(body, limit), (void)0);
    body = NULL;
}

JNIEXPORT void JNICALL Java_org_yuanrong_datasystem_object_BufferImpl_memoryCopyHeapBuffer(JNIEnv *env, jclass,
                                                                                       jlong handle, jbyteArray bytes,
                                                                                       jlong len)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    VLOG(LOG_LEVEL) << "JNICALL BufferImpl.memoryCopy";
    auto buffer = reinterpret_cast<std::shared_ptr<Buffer> *>(handle);
    JNI_CHECK_RESULT(env, CopyToBufferNoLatch(env, bytes, len, **buffer), (void)0);
}

JNIEXPORT jobject JNICALL Java_org_yuanrong_datasystem_object_BufferImpl_mutableDataNative(JNIEnv *env, jclass,
                                                                                       jlong handle)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    VLOG(LOG_LEVEL) << "JNICALL BufferImpl.mutableData";
    auto buffer = reinterpret_cast<std::shared_ptr<Buffer> *>(handle);
    auto dataPtr = static_cast<uint8_t *>((*buffer)->MutableData());
    auto dataSize = (*buffer)->GetSize();
    return env->NewDirectByteBuffer(dataPtr, dataSize);
}

JNIEXPORT jobject JNICALL Java_org_yuanrong_datasystem_object_BufferImpl_immutableDataNative(JNIEnv *env, jobject,
                                                                                         jlong handle)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    VLOG(LOG_LEVEL) << "JNICALL BufferImpl.immutableData";
    auto buffer = reinterpret_cast<std::shared_ptr<Buffer> *>(handle);
    auto dataPtr = const_cast<void *>((*buffer)->ImmutableData());
    auto dataSize = (*buffer)->GetSize();
    return env->NewDirectByteBuffer(dataPtr, dataSize);
}

JNIEXPORT void JNICALL Java_org_yuanrong_datasystem_object_BufferImpl_invalidateBufferNative(JNIEnv *env, jclass,
                                                                                         jlong handle)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    VLOG(LOG_LEVEL) << "JNICALL BufferImpl.invalidateBuffer";
    auto buffer = reinterpret_cast<std::shared_ptr<Buffer> *>(handle);
    JNI_CHECK_RESULT(env, (*buffer)->InvalidateBuffer(), (void)0);
}
JNIEXPORT jlong JNICALL Java_org_yuanrong_datasystem_object_BufferImpl_getSizeNative(JNIEnv *env, jclass, jlong handle)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    (void)env;
    VLOG(LOG_LEVEL) << "JNICALL BufferImpl.getSize";
    auto buffer = reinterpret_cast<std::shared_ptr<Buffer> *>(handle);
    return (*buffer)->GetSize();
}

JNIEXPORT void JNICALL Java_org_yuanrong_datasystem_object_BufferImpl_freeBufferPtr(JNIEnv *, jclass, jlong handle)
{
    auto buffer = reinterpret_cast<std::shared_ptr<Buffer> *>(handle);
    delete buffer;
}
}  // namespace java_api
}  // namespace datasystem
}