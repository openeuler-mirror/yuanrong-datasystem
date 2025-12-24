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

package org.yuanrong.datasystem.example;

import org.yuanrong.datasystem.stream.Consumer;
import org.yuanrong.datasystem.stream.Element;
import org.yuanrong.datasystem.stream.Producer;
import org.yuanrong.datasystem.stream.StreamClient;
import org.yuanrong.datasystem.stream.SubscriptionType;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.logging.Logger;

/**
 * The example of using StreamClient.
 *
 * @since 2022-06-12
 */
public class StreamClientExample {
    public static void main(String[] args) {
        Logger log = Logger.getLogger(StreamClientExample.class.toString());
        log.info("---- Start the stream java api demo");
        String ip = args[0];
        int port = Integer.parseInt(args[1]);
        StreamClient clientSrd = new StreamClient(ip, port);
        try {
            String streamName = "streamName";
            int maxStreamSize = 64 * 1024 * 1024; // 64M
            int pageSize = 1024 * 1024; // 1M
            int delayFlush = 5; // 5ms
            Producer producerSrd = clientSrd.createProducer(streamName, delayFlush, pageSize, maxStreamSize);
            Consumer consumerSrd = clientSrd.subscribe(streamName, "subName", SubscriptionType.STREAM);
            String value = "testData";
            byte[] arr = value.getBytes(StandardCharsets.UTF_8);
            ByteBuffer buffer1 = ByteBuffer.allocateDirect(arr.length);
            buffer1.put(arr);

            log.info("The data sent is : " + value);
            producerSrd.send(buffer1);

            List<Element> elements = consumerSrd.receive(1, 0);
            ByteBuffer buffer2 = elements.get(0).getBuffer();
            byte[] bytes = new byte[buffer2.remaining()];
            buffer2.get(bytes);
            String receive = new String(bytes, StandardCharsets.UTF_8);
            log.info("The received data is : " + receive);

            consumerSrd.ack(elements.get(0).getId());
            producerSrd.close();
            consumerSrd.close();
        } finally {
            clientSrd.close();
        }
    }
}