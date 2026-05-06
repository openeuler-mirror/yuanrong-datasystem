/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: Register function to python.
 */
#include <memory>

#include "datasystem/common/log/log.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/pybind_api/pybind_register.h"
#include "datasystem/stream/consumer.h"
#include "datasystem/stream/element.h"
#include "datasystem/stream/producer.h"
#include "datasystem/stream_client.h"
#include "datasystem/stream/stream_config.h"

using datasystem::Consumer;
using datasystem::Element;
using datasystem::Producer;
using datasystem::StreamClient;
using datasystem::SubscriptionConfig;

namespace datasystem {
PybindDefineRegisterer g_pybind_define_f_StreamClient("StreamClient", PRIORITY_LOW, [](const py::module *m) {
    py::class_<StreamClient, std::shared_ptr<StreamClient>>(*m, "StreamClient")
        .def(py::init([](const std::string &host, int32_t port, const std::string &clientPublicKey,
                         const std::string &clientPrivateKey, const std::string &serverPublicKey,
                         const std::string &accessKey, const std::string &secretKey, const std::string &token,
                         const std::string &tenantId, bool enableExclusiveConnection) {
            ConnectOptions connectOpts{ .host = host, .port = port };
            connectOpts.token = token;
            connectOpts.clientPublicKey = clientPublicKey;
            connectOpts.clientPrivateKey = clientPrivateKey;
            connectOpts.serverPublicKey = serverPublicKey;
            connectOpts.accessKey = accessKey;
            connectOpts.secretKey = secretKey;
            connectOpts.tenantId = tenantId;
            connectOpts.enableExclusiveConnection = enableExclusiveConnection;
            return std::make_unique<StreamClient>(connectOpts);
        }))
        .def("init",
             [](StreamClient &client) {
                 TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                 return client.Init();
             })
        .def("CreateProducer",
             [](StreamClient &client, const std::string &streamName, int64_t delayFlushTime, int64_t pageSize,
                int64_t maxStreamSize, bool autoCleanup, int64_t retainForNumConsumers, bool encryptStream,
                int64_t reserveSize) {
                 TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                 std::shared_ptr<Producer> outProducer;
                 ProducerConf producerConf = { .delayFlushTime = delayFlushTime,
                                               .pageSize = pageSize,
                                               .maxStreamSize = static_cast<uint64_t>(maxStreamSize),
                                               .autoCleanup = autoCleanup,
                                               .retainForNumConsumers = static_cast<uint64_t>(retainForNumConsumers),
                                               .encryptStream = encryptStream,
                                               .reserveSize = static_cast<uint64_t>(reserveSize) };
                 auto status = client.CreateProducer(streamName, outProducer, producerConf);
                 if (status.IsError()) {
                     LOG(ERROR) << FormatString("CreateProducer failed for stream %s with error %s", streamName,
                                                status.ToString());
                 }
                 return std::make_pair(status, outProducer);
             })
        .def("Subscribe",
             [](StreamClient &client, const std::string &streamName, const std::string &subName,
                const int subscriptionType) {
                 TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                 std::shared_ptr<Consumer> outConsumer;
                 const struct SubscriptionConfig config =
                     SubscriptionConfig(subName, (SubscriptionType)subscriptionType);
                 auto status = client.Subscribe(streamName, config, outConsumer);
                 if (status.IsError()) {
                     LOG(ERROR) << FormatString("Subscribe failed for stream %s with error %s", streamName,
                                                status.ToString());
                 }
                 return std::make_pair(status, outConsumer);
             })
        .def("DeleteStream",
             [](StreamClient &client, const std::string &streamName) {
                 TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                 auto status = client.DeleteStream(streamName);
                 if (status.IsError()) {
                     LOG(ERROR) << FormatString("DeleteStream failed for stream %s with error %s", streamName,
                                                status.ToString());
                 }
                 return status;
             })
        .def("QueryGlobalProducersNum",
             [](StreamClient &client, const std::string &streamName) {
                 TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                 uint64_t globalProducerNum = 0;
                 auto status = client.QueryGlobalProducersNum(streamName, globalProducerNum);
                 if (status.IsError()) {
                     LOG(ERROR) << FormatString("QueryGlobalProducersNum failed, stream name is %s", streamName);
                 }
                 return std::make_pair(status, globalProducerNum);
             })
        .def("QueryGlobalConsumersNum", [](StreamClient &client, const std::string &streamName) {
            TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
            uint64_t globalConsumerNum = 0;
            auto status = client.QueryGlobalConsumersNum(streamName, globalConsumerNum);
            if (status.IsError()) {
                LOG(ERROR) << FormatString("QueryGlobalConsumerNum failed, stream name is %s", streamName);
            }
            return std::make_pair(status, globalConsumerNum);
        });
});

PybindDefineRegisterer g_pybind_define_f_Producer("Producer", PRIORITY_LOW, [](const py::module *m) {
    py::class_<Producer, std::shared_ptr<Producer>>(*m, "Producer")
        .def("Send",
             [](Producer &producer, const py::buffer &buf, const int timeoutMs) {
                 TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                 py::buffer_info info = buf.request();
                 Element element(static_cast<uint8_t *>(info.ptr), info.size);
                 return producer.Send(element, timeoutMs);
             })
        .def("Send",
             [](Producer &producer, const py::buffer &buf) {
                 TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                 py::buffer_info info = buf.request();
                 Element element(static_cast<uint8_t *>(info.ptr), info.size);
                 return producer.Send(element);
             })
        .def("Close", [](Producer &producer) {
            TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
            return producer.Close();
        });
});

PybindDefineRegisterer g_pybind_define_f_Consumer("Consumer", PRIORITY_LOW, [](const py::module *m) {
    py::class_<Consumer, std::shared_ptr<Consumer>>(*m, "Consumer")
        .def("Receive",
             [](Consumer &consumer, const int expectNum, const int timeoutMs) {
                 TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                 std::vector<Element> outElement;
                 auto status = consumer.Receive(expectNum, timeoutMs, outElement);
                 return std::make_pair(status, outElement);
             })
        .def("ReceiveAny",
             [](Consumer &consumer, const int timeoutMs) {
                 TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                 std::vector<Element> outElement;
                 auto status = consumer.Receive(timeoutMs, outElement);
                 return std::make_pair(status, outElement);
             })
        .def("Ack",
             [](Consumer &consumer, const int element_id) {
                 TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                 return consumer.Ack(element_id);
             })
        .def("Close", [](Consumer &consumer) {
            TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
            return consumer.Close();
        });
});

PybindDefineRegisterer g_pybind_define_f_element("Element", PRIORITY_LOW, ([](const py::module *m) {
                                                     (void)py::class_<Element>(*m, "Element",
                                                                               pybind11::buffer_protocol())
                                                         .def("get_id",
                                                              [](Element &element) {
                                                                  return element.id != ULONG_MAX ? element.id : -1;
                                                              })
                                                         .def_buffer([](Element &element) {
                                                             return py::buffer_info(element.ptr, element.size);
                                                         });
                                                 }));
}  // namespace datasystem
