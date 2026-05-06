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
 * Description: Jni implementation for consumer.
 */
#include <memory>

#include <jni.h>

#include "datasystem/common/log/log.h"
#include "datasystem/stream/consumer.h"
#include "datasystem/stream/element.h"
#include "datasystem/stream_client.h"
#include "datasystem/java_api/jni_util.h"

namespace datasystem {
namespace java_api {
extern "C" {

using datasystem::Consumer;
using datasystem::Element;

JNIEXPORT jobject JNICALL Java_org_yuanrong_datasystem_stream_ConsumerImpl_receive(JNIEnv *env, jclass,
                                                                               jlong consumerPtr,
                                                                               jlong expectNum, jint timeoutMs,
                                                                               jboolean hasExpectedNum)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    VLOG(LOG_LEVEL) << "JNICALL ConsumerImpl.receive";
    auto consumer = reinterpret_cast<std::shared_ptr<Consumer> *>(consumerPtr);
    std::vector<Element> elements;
    if (hasExpectedNum == JNI_TRUE) {
        JNI_CHECK_RESULT(env, (*consumer)->Receive(expectNum, timeoutMs, elements), 0);
    } else {
        JNI_CHECK_RESULT(env, (*consumer)->Receive(timeoutMs, elements), 0);
    }

    jclass arrayListClass = env->FindClass("java/util/ArrayList");
    jobject elementList = env->NewObject(arrayListClass, env->GetMethodID(arrayListClass, "<init>", "()V"));
    jmethodID elementAdd = env->GetMethodID(arrayListClass, "add", "(Ljava/lang/Object;)Z");

    jclass elementClass = env->FindClass("org/yuanrong/datasystem/stream/Element");
    jmethodID elementInit = env->GetMethodID(elementClass, "<init>", "(JLjava/nio/ByteBuffer;)V");
    jobject elementBuffer = NULL, elementObject = NULL;
    for (const auto &element : elements) {
        elementBuffer = env->NewDirectByteBuffer(element.ptr, element.size);
        elementObject = env->NewObject(elementClass, elementInit, element.id, elementBuffer);
        env->CallBooleanMethod(elementList, elementAdd, elementObject);
    }
    // Clean up
    env->DeleteLocalRef(arrayListClass);
    env->DeleteLocalRef(elementClass);
    env->DeleteLocalRef(elementBuffer);
    env->DeleteLocalRef(elementObject);
    return elementList;
}

JNIEXPORT void JNICALL Java_org_yuanrong_datasystem_stream_ConsumerImpl_ack(JNIEnv *env, jclass, jlong consumerPtr,
                                                                        jlong elementId)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    VLOG(LOG_LEVEL) << "JNICALL ConsumerImpl.ack";
    auto consumer = reinterpret_cast<std::shared_ptr<Consumer> *>(consumerPtr);
    JNI_CHECK_RESULT(env, (*consumer)->Ack(elementId), (void)0);
}

JNIEXPORT void JNICALL Java_org_yuanrong_datasystem_stream_ConsumerImpl_close(JNIEnv *env, jclass, jlong consumerPtr)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    VLOG(LOG_LEVEL) << "JNICALL ConsumerImpl.close";
    auto consumer = reinterpret_cast<std::shared_ptr<Consumer> *>(consumerPtr);
    JNI_CHECK_RESULT(env, (*consumer)->Close(), (void)0);
    delete consumer;
}

JNIEXPORT void JNICALL Java_org_yuanrong_datasystem_stream_ConsumerImpl_freeJNIPtrNative(JNIEnv *, jclass, jlong handle)
{
    VLOG(LOG_LEVEL) << "JNICALL ConsumerImpl.freeJNIPtrNative";
    auto consumer = reinterpret_cast<std::shared_ptr<Consumer> *>(handle);
    delete consumer;
}

JNIEXPORT void JNICALL Java_org_yuanrong_datasystem_stream_ConsumerImpl_getStatisticsMessage(
    JNIEnv *env, jclass callerClass, jlong consumerPtr)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    VLOG(LOG_LEVEL) << "JNICALL ConsumerImpl.getStatisticsMessage";
    auto consumer = reinterpret_cast<std::shared_ptr<Consumer> *>(consumerPtr);
    uint64_t totalElements = 0;
    uint64_t notProcessedElements = 0;
    (*consumer)->GetStatisticsMessage(totalElements, notProcessedElements);
    jmethodID mid = env->GetStaticMethodID(callerClass, "storeStatisticsMessage", "(JJ)V");
    if (mid == nullptr) {
        LOG(ERROR) << "Cannot get static method ConsumerImpl.getStatisticsMessage";
        return;
    }
    env->CallStaticVoidMethod(callerClass, mid, (jlong)totalElements, (jlong)notProcessedElements);
}
}  // namespace java_api
}  // namespace datasystem
}