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
 * Description: Jni implementation for producer.
 */

#include <jni.h>

#include <string>

#include "datasystem/common/log/log.h"
#include "datasystem/java_api/jni_util.h"
#include "datasystem/stream/consumer.h"
#include "datasystem/stream/producer.h"
#include "datasystem/stream_client.h"

namespace datasystem {
namespace java_api {
extern "C" {

using datasystem::Element;
using datasystem::Producer;

JNIEXPORT void JNICALL Java_org_yuanrong_datasystem_stream_ProducerImpl_sendHeapBufferDefaultTimeout(JNIEnv *env,
                                                                    jclass, jlong handle, jbyteArray bytes, jlong len)
{
    VLOG(LOG_LEVEL) << "JNICALL ProducerImpl.send";
    auto producer = reinterpret_cast<std::shared_ptr<Producer> *>(handle);
    jbyte *bytekey = env->GetByteArrayElements(bytes, 0);
    if (bytekey == nullptr) {
        ThrowException(env, -1, "An empty array is passed in the Java layer.");
        return;
    } else {
        Element element(reinterpret_cast<uint8_t *>(bytekey), len);
        auto rc = (*producer)->Send(element);
        env->ReleaseByteArrayElements(bytes, bytekey, 0);
        if (rc.IsError()) {
            datasystem::java_api::ThrowException((env), rc.GetCode(), rc.GetMsg());
        }
    }
}

JNIEXPORT void JNICALL Java_org_yuanrong_datasystem_stream_ProducerImpl_sendDirectBufferDefaultTimeout(
                                                                    JNIEnv *env, jclass, jlong handle, jobject buf)
{
    VLOG(LOG_LEVEL) << "JNICALL ProducerImpl.send";
    auto producer = reinterpret_cast<std::shared_ptr<Producer> *>(handle);
    auto body = env->GetDirectBufferAddress(buf);
    if (!body) {
        ThrowException(env, -1, "cannot get element address");
        return;
    }
    int limit = GetByteBufferLimit(env, buf);
    Element element(static_cast<uint8_t *>(body), limit);
    JNI_CHECK_RESULT(env, (*producer)->Send(element), (void)0);
    body = NULL;
}

JNIEXPORT void JNICALL Java_org_yuanrong_datasystem_stream_ProducerImpl_sendHeapBuffer(JNIEnv *env,
                                                                                   jclass, jlong handle,
                                                                                   jbyteArray bytes, jlong len,
                                                                                   jint timeoutMs)
{
    VLOG(LOG_LEVEL) << "JNICALL ProducerImpl.send";
    auto producer = reinterpret_cast<std::shared_ptr<Producer> *>(handle);
    jbyte *bytekey = env->GetByteArrayElements(bytes, 0);
    if (bytekey == nullptr) {
        ThrowException(env, -1, "An empty array is passed in the Java layer.");
        return;
    } else {
        Element element(reinterpret_cast<uint8_t *>(bytekey), len);
        auto rc = (*producer)->Send(element, timeoutMs);
        env->ReleaseByteArrayElements(bytes, bytekey, 0);
        if (rc.IsError()) {
            datasystem::java_api::ThrowException((env), rc.GetCode(), rc.GetMsg());
        }
    }
}

JNIEXPORT void JNICALL Java_org_yuanrong_datasystem_stream_ProducerImpl_sendDirectBuffer(JNIEnv *env, jclass,
                                                                                     jlong handle,
                                                                                     jobject buf, jint timeoutMs)
{
    VLOG(LOG_LEVEL) << "JNICALL ProducerImpl.send";
    auto producer = reinterpret_cast<std::shared_ptr<Producer> *>(handle);
    auto body = env->GetDirectBufferAddress(buf);
    if (!body) {
        ThrowException(env, -1, "cannot get element address");
        return;
    }
    int limit = GetByteBufferLimit(env, buf);
    Element element(static_cast<uint8_t *>(body), limit);
    JNI_CHECK_RESULT(env, (*producer)->Send(element, timeoutMs), (void)0);
    body = NULL;
}

JNIEXPORT void JNICALL Java_org_yuanrong_datasystem_stream_ProducerImpl_close(JNIEnv *env, jclass, jlong handle)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    VLOG(LOG_LEVEL) << "JNICALL ProducerImpl.close";
    auto producer = reinterpret_cast<std::shared_ptr<Producer> *>(handle);
    JNI_CHECK_RESULT(env, (*producer)->Close(), (void)0);
    delete (producer);
}

JNIEXPORT void JNICALL Java_org_yuanrong_datasystem_stream_ProducerImpl_freeJNIPtrNative(JNIEnv *, jclass, jlong handle)
{
    VLOG(LOG_LEVEL) << "JNICALL ProducerImpl.freeJNIPtrNative";
    auto producer = reinterpret_cast<std::shared_ptr<Producer> *>(handle);
    delete producer;
}
}  // namespace java_api
}  // namespace datasystem
}