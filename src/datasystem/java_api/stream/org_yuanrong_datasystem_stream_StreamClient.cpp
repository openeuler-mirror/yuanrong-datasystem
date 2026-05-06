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
 * Description: Jni implementation for stream client.
 */
#include <memory>
#include <string>

#include <jni.h>

#include "datasystem/common/log/log.h"
#include "datasystem/stream/consumer.h"
#include "datasystem/stream/producer.h"
#include "datasystem/stream/stream_config.h"
#include "datasystem/stream_client.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/java_api/jni_util.h"

namespace datasystem {
namespace java_api {
extern "C" {

using datasystem::Consumer;
using datasystem::Producer;
using datasystem::StreamClient;
using datasystem::SubscriptionType;

JNIEXPORT jlong JNICALL Java_org_yuanrong_datasystem_stream_StreamClient_init(JNIEnv *env, jclass, jobject jConnectOpts,
                                                                          jboolean shouldReportWorkerLost)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    VLOG(LOG_LEVEL) << "JNICALL StreamClient.init";
    ConnectOptions connectOpts = ToCppConnectOptions(env, jConnectOpts);
    if (env->ExceptionOccurred()) {
        LOG(ERROR) << "Exception Occurs when Java_org_yuanrong_datasystem_stream_StreamClient_init function to call "
                      "ToCppConnectOptions()";
        return 0;
    }

    auto client = std::make_unique<StreamClient>(connectOpts);
    if (shouldReportWorkerLost == JNI_TRUE) {
        JNI_CHECK_RESULT(env, client->Init(true), 0);
    } else {
        JNI_CHECK_RESULT(env, client->Init(), 0);
    }
    return reinterpret_cast<jlong>(client.release());
}

JNIEXPORT jlong JNICALL Java_org_yuanrong_datasystem_stream_StreamClient_createProducer(JNIEnv *env, jclass,
                                                                                    jlong handle,
                                                                                    jstring stream, jlong delay,
                                                                                    jlong pageSize, jlong maxStreamSize,
                                                                                    jboolean autoCleanup = false,
                                                                                    jlong retainForNumConsumers = 0,
                                                                                    jboolean encryptStream = false,
                                                                                    jlong reserveSize = 0)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    VLOG(LOG_LEVEL) << "JNICALL StreamClient.createProducer";
    std::string streamName = ToString(env, stream);
    StreamClient *client = reinterpret_cast<StreamClient *>(handle);
    auto producer = std::make_unique<std::shared_ptr<Producer>>();
    ProducerConf producerConf = { .delayFlushTime = delay,
                                  .pageSize = pageSize,
                                  .maxStreamSize = static_cast<uint64_t>(maxStreamSize),
                                  .autoCleanup = static_cast<bool>(autoCleanup),
                                  .retainForNumConsumers = static_cast<uint64_t>(retainForNumConsumers),
                                  .encryptStream = static_cast<bool>(encryptStream),
                                  .reserveSize = static_cast<uint64_t>(reserveSize) };
    JNI_CHECK_RESULT(env, client->CreateProducer(streamName, *producer, producerConf), 0);
    return reinterpret_cast<jlong>(producer.release());
}

JNIEXPORT jlong JNICALL Java_org_yuanrong_datasystem_stream_StreamClient_subscribe(JNIEnv *env, jclass, jlong clientPtr,
                                                                               jstring streamName, jstring subName,
                                                                               jobject subscription,
                                                                               jboolean shouldAutoAck)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    VLOG(LOG_LEVEL) << "JNICALL StreamClient.subscribe";
    // get the value of subscription
    jclass jniSubscription = env->GetObjectClass(subscription);
    jmethodID jniMethoId = env->GetMethodID(jniSubscription, "ordinal", "()I");
    int subscriptionTypeInt = env->CallIntMethod(jniSubscription, jniMethoId);

    SubscriptionType subType = (SubscriptionType)subscriptionTypeInt;
    std::string subNameStr = ToString(env, subName);
    const struct SubscriptionConfig config(subNameStr, subType);

    // Create consumer shared pointer in heap
    auto consumer = std::make_unique<std::shared_ptr<Consumer>>();
    StreamClient *client = reinterpret_cast<StreamClient *>(clientPtr);
    std::string streamNameStr = ToString(env, streamName);
    if (shouldAutoAck == JNI_TRUE) {
        JNI_CHECK_RESULT(env, client->Subscribe(streamNameStr, config, *consumer, true), 0);
    } else {
        JNI_CHECK_RESULT(env, client->Subscribe(streamNameStr, config, *consumer, false), 0);
    }

    // Clean up
    env->DeleteLocalRef(jniSubscription);
    return reinterpret_cast<jlong>(consumer.release());
}

JNIEXPORT jlong JNICALL Java_org_yuanrong_datasystem_stream_StreamClient_queryGlobalProducerNum(JNIEnv *env, jclass,
                                                                                            jlong clientPtr,
                                                                                            jstring streamName)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    VLOG(LOG_LEVEL) << "JNICALL StreamClient.queryGlobalProducerNum";
    // get the structure of the number of global producer stored in producerDistribution
    StreamClient *client = reinterpret_cast<StreamClient *>(clientPtr);
    uint64_t globalProducerNum = 0;
    std::string streamNameStr = ToString(env, streamName);
    JNI_CHECK_RESULT(env, client->QueryGlobalProducersNum(streamNameStr, globalProducerNum), 0);
    // statistic global producer number
    return globalProducerNum;
}

JNIEXPORT jlong JNICALL Java_org_yuanrong_datasystem_stream_StreamClient_queryGlobalConsumerNum(JNIEnv *env, jclass,
                                                                                            jlong clientPtr,
                                                                                            jstring streamName)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    VLOG(LOG_LEVEL) << "JNICALL StreamClient.queryGlobalConsumerNum";
    // get the structure of the number of global consumer stored in consumerDistribution
    StreamClient *client = reinterpret_cast<StreamClient *>(clientPtr);
    uint64_t globalConsumerNum = 0;
    std::string streamNameStr = ToString(env, streamName);
    JNI_CHECK_RESULT(env, client->QueryGlobalConsumersNum(streamNameStr, globalConsumerNum), 0);
    // statistic global consumer number
    return globalConsumerNum;
}

JNIEXPORT void JNICALL Java_org_yuanrong_datasystem_stream_StreamClient_deleteStream(JNIEnv *env, jclass,
                                                                                jlong clientPtr, jstring streamName)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    VLOG(LOG_LEVEL) << "JNICALL StreamClient.deleteStream";
    // convert jni type to c++ type
    StreamClient *client = reinterpret_cast<StreamClient *>(clientPtr);
    std::string streamNameC = ToString(env, streamName);
    // call the DeleteStream method on the c++ side
    JNI_CHECK_RESULT(env, client->DeleteStream(streamNameC), (void)0);
}

JNIEXPORT void JNICALL Java_org_yuanrong_datasystem_stream_StreamClient_freeJNIPtrNative(JNIEnv *, jclass, jlong handle)
{
    VLOG(LOG_LEVEL) << "JNICALL StreamClient.freeJNIPtrNative";
    StreamClient *client = reinterpret_cast<StreamClient *>(handle);
    delete client;
}

JNIEXPORT void JNICALL Java_com_datasystem_stream_StreamClient_updateTokenNative(JNIEnv *env, jclass,
    jlong handle, jbyteArray tokenBytes)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    VLOG(LOG_LEVEL) << "JNICALL StreamClient.updateToken";
    auto client = reinterpret_cast<StreamClient *>(handle);
    SensitiveValue token = ToSensitiveValue(env, tokenBytes);
    JNI_CHECK_RESULT(env, client->UpdateToken(token), (void)0);
}

JNIEXPORT void JNICALL Java_com_datasystem_stream_StreamClient_updateAkSkNative(JNIEnv *env, jclass,
    jlong handle, jstring accessKeyJO, jbyteArray secretKeyBytes)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    VLOG(LOG_LEVEL) << "JNICALL StreamClient.updateAkSk";
    auto client = reinterpret_cast<std::shared_ptr<StreamClient> *>(handle);
    std::string accessKey = ToString(env, accessKeyJO);
    SensitiveValue secretKey = ToSensitiveValue(env, secretKeyBytes);
    JNI_CHECK_RESULT(env, (*client)->UpdateAkSk(accessKey, secretKey), (void)0);
}
}  // namespace java_api
}  // namespace datasystem
}