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

package org.yuanrong.datasystem.stream;

import org.yuanrong.datasystem.Cluster;
import org.yuanrong.datasystem.DataSystemException;
import org.yuanrong.datasystem.TestUtils;
import org.yuanrong.datasystem.stream.Consumer.StatisticsMessage;
import org.yuanrong.datasystem.ConnectOptions;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Test java stream api
 *
 * @since 2022-05-07
 */
public class StreamClientTest {
    private static Cluster cluster;
    private static final Logger logger = Logger.getLogger(StreamClientTest.class.getName());
    private static int workerNums = 2;

    @BeforeClass
    public static void beforeClass() throws IOException {
        cluster = new Cluster(workerNums, 0);
        cluster.init(StreamClientTest.class.getSimpleName());
        cluster.setWorkerGflagParams("-shared_memory_size_mb=1024");
        cluster.startAll();
    }

    @AfterClass
    public static void afterClass() {
        if (cluster != null) {
            cluster.stopAll();
        }
    }

    @Test(expected = DataSystemException.class)
    public void testInvalidAddress() {
        ConnectOptions connectOptions = new ConnectOptions("127.0.0.1", -1);
        new StreamClient(connectOptions);
    }

    /**
     * Get StreamClient.
     *
     * @param number The StreamClient number.(number <= workerNums)
     * @return The stream client.
     */
    public List<StreamClient> getStreamClient(int number) {
        List<StreamClient> clients = new ArrayList<>(number);
        for (int i = 0; i < number; i++) {
            String workerAddr = cluster.getWorkerAddr(i);
            String[] temp = workerAddr.split(":");
            String ip = temp[0];
            int port = Integer.parseInt(temp[1]);
            StreamClient client = new StreamClient(new ConnectOptions(ip, port));
            clients.add(client);
        }
        return clients;
    }

    /**
     * Get StreamClient.
     *
     * @param number The StreamClient number.(number <= workerNums)
     * @param shouldReportWorkerLost Whether to report to the caller when worker had crashed or worker lost the client.
     * @return The stream client.
     */
    public List<StreamClient> getStreamClient(int number, boolean shouldReportWorkerLost) {
        List<StreamClient> clients = new ArrayList<>(number);
        for (int i = 0; i < number; i++) {
            String workerAddr = cluster.getWorkerAddr(i);
            String[] temp = workerAddr.split(":");
            String ip = temp[0];
            int port = Integer.parseInt(temp[1]);
            StreamClient client = new StreamClient(new ConnectOptions(ip, port), shouldReportWorkerLost);
            clients.add(client);
        }
        return clients;
    }

    @Test
    public void testConnect() {
        String[] workerAddr = cluster.getWorkerAddr(0).split(":");
        String ip = workerAddr[0];
        int port = Integer.parseInt(workerAddr[1]);
        StreamClient client = new StreamClient(new ConnectOptions(ip, port));
        client.close();
    }

    @Test
    public void testClientSendReceiveData() {
        logger.info("******************** testClientSendReceiveData **********************");
        StreamClient clientSrd = getStreamClient(1).get(0);
        String streamName = "streamSr";
        Producer producerSrd = clientSrd.createProducer(streamName);
        Consumer consumerSrd = clientSrd.subscribe(streamName, "subNameS", SubscriptionType.STREAM);
        try {
            String value1 = "10101010";
            byte[] arr = value1.getBytes(StandardCharsets.UTF_8);
            ByteBuffer buffer = ByteBuffer.allocateDirect(arr.length);
            buffer.put(arr);

            producerSrd.send(buffer);

            List<Element> elements = consumerSrd.receive(1, 0);
            ByteBuffer buffer2 = elements.get(0).getBuffer();
            buffer.flip();
            Assert.assertEquals(buffer, buffer2);
            consumerSrd.ack(elements.get(0).getId());
        } finally {
            producerSrd.close();
            consumerSrd.close();
            clientSrd.close();
        }
    }

    @Test
    public void testClientSendReceiveDataWithAutoAck() {
        logger.info("******************** testClientSendReceiveDataWithAutoAck **********************");
        StreamClient clientSrd = getStreamClient(1).get(0);
        String streamName = "streamSr";
        Producer producerSrd = clientSrd.createProducer(streamName);
        Consumer consumerSrd = clientSrd.subscribe(streamName, "subNameS", SubscriptionType.STREAM, true);
        try {
            String value = "10101010";
            byte[] arr = value.getBytes(StandardCharsets.UTF_8);
            ByteBuffer buffer = ByteBuffer.allocateDirect(arr.length);
            buffer.put(arr);

            producerSrd.send(buffer);

            List<Element> elements = consumerSrd.receive(1, 0);
            ByteBuffer buffer2 = elements.get(0).getBuffer();
            buffer.flip();
            Assert.assertEquals(buffer, buffer2);

            String value3 = "abcdabcd";
            byte[] arr3 = value3.getBytes(StandardCharsets.UTF_8);
            ByteBuffer buffer3 = ByteBuffer.allocateDirect(arr3.length);
            buffer3.put(arr3);

            producerSrd.send(buffer3);

            List<Element> elements3 = consumerSrd.receive(1, 0);
            ByteBuffer buffer4 = elements3.get(0).getBuffer();
            buffer3.flip();
            Assert.assertEquals(buffer3, buffer4);
        } finally {
            producerSrd.close();
            consumerSrd.close();
            clientSrd.close();
        }
    }

    @Test
    public void testClientBlockingSendReceiveWithoutExpectedNumData() {
        logger.info("******************** testClientBlockingSendReceiveWithoutExpectedNumData **********************");
        StreamClient clientSrd = getStreamClient(1).get(0);
        String streamName = "streamSr";
        Producer producerSrd = clientSrd.createProducer(streamName);
        Consumer consumerSrd = clientSrd.subscribe(streamName, "subNameS", SubscriptionType.STREAM);
        try {
            String value = "10101010";
            byte[] arr = value.getBytes(StandardCharsets.UTF_8);
            ByteBuffer buffer = ByteBuffer.allocateDirect(arr.length);
            buffer.put(arr);
            producerSrd.send(buffer, 5000);

            List<Element> elements = consumerSrd.receive(0);
            ByteBuffer buffer2 = elements.get(0).getBuffer();
            buffer.flip();
            Assert.assertEquals(buffer, buffer2);
            consumerSrd.ack(elements.get(0).getId());
        } finally {
            producerSrd.close();
            consumerSrd.close();
            clientSrd.close();
        }
    }

    @Test
    public void testQueryStreamTopo() {
        logger.info("******************** testQueryStreamTopo **********************");
        String streamName1 = "queryTopoStream1";
        List<StreamClient> clients = getStreamClient(workerNums);
        StreamClient client1 = clients.get(0);
        Producer node1Producer = client1.createProducer(streamName1);
        Consumer node1Consumer = client1.subscribe(streamName1, "subName1", SubscriptionType.STREAM);

        long globalConsumerNum1 = client1.queryGlobalConsumerNum(streamName1);
        Assert.assertEquals(globalConsumerNum1, 1);
        long globalProducerNum1 = client1.queryGlobalProducerNum(streamName1);
        Assert.assertEquals(globalProducerNum1, 1);

        StreamClient client2 = clients.get(1);
        Producer node2Producer = client2.createProducer(streamName1);
        Consumer node2Consumer = client2.subscribe(streamName1, "subName2", SubscriptionType.STREAM);
        try {
            long globalConsumerNum2 = client2.queryGlobalConsumerNum(streamName1);
            Assert.assertEquals(globalConsumerNum2, 2);
            long globalProducerNum2 = client2.queryGlobalProducerNum(streamName1);
            Assert.assertEquals(globalProducerNum2, 2);
        } finally {
            node1Producer.close();
            node1Consumer.close();
            node2Producer.close();
            node2Consumer.close();
            client1.close();
            client2.close();
        }
    }

    @Test
    public void testMultiSendRecv() {
        logger.info("******************** testMultiSendRecv **********************");
        StreamClient clientSr = getStreamClient(1).get(0);
        String streamName = "streamNameMulti0";
        String subName = "subNameMulti0";
        Producer producerTmp = clientSr.createProducer(streamName, 50);
        Consumer consumerTmp = clientSr.subscribe(streamName, subName, SubscriptionType.STREAM);
        try {
            int elementNum = 4000;
            int elementSize = 1000;
            List<ByteBuffer> elementDatas = TestUtils.getRandomDirectBufferList(elementSize, elementNum);
            int num = 5;
            // send bytes directly.
            for (int i = 0; i < num; i++) {
                producerTmp.send(elementDatas.get(i));
            }

            List<Element> elements = consumerTmp.receive(num, 0);
            for (int i = 0; i < elements.size(); i++) {
                ByteBuffer buffer = elements.get(i).getBuffer();
                Assert.assertEquals(buffer, elementDatas.get(i));
            }
        } finally {
            producerTmp.close();
            consumerTmp.close();
            clientSr.close();
        }
    }

    @Test
    public void testStreamAutoSendWithoutDelay() {
        logger.info("******************** testStreamAutoSendWithoutDelay **********************");
        String streamName = "stream_send_without_delay";
        StreamClient clientStreamSwd = getStreamClient(1).get(0);
        Producer producerSwd = clientStreamSwd.createProducer(streamName);
        Consumer consumerSwd = clientStreamSwd.subscribe(streamName, "sub_object_stream1", SubscriptionType.STREAM);
        try {
            String value = "10101010";
            byte[] arr = value.getBytes(StandardCharsets.UTF_8);
            ByteBuffer buffer = ByteBuffer.allocateDirect(arr.length);
            buffer.put(arr);
            producerSwd.send(buffer);
            List<Element> elementList = consumerSwd.receive(1, 10);
            consumerSwd.ack(elementList.get(0).getId());
            ByteBuffer buffer2 = elementList.get(0).getBuffer();
            buffer.flip();
            Assert.assertEquals(buffer, buffer2);
        } finally {
            producerSwd.close();
            consumerSwd.close();
            clientStreamSwd.close();
        }
    }

    @Test
    public void testSendHeapByteBuffer() {
        logger.info("******************** testSendHeapByteBuffer **********************");
        String streamName = "SendHeapByteBuffer";
        StreamClient clientStreamShb = getStreamClient(1).get(0);
        Producer producerShb = clientStreamShb.createProducer(streamName);
        Consumer consumerShb = clientStreamShb.subscribe(streamName, "sub_object_stream1", SubscriptionType.STREAM);
        try {
            String value = "10101010";
            byte[] arr = value.getBytes(StandardCharsets.UTF_8);
            ByteBuffer buffer = ByteBuffer.wrap(arr);
            producerShb.send(buffer);

            List<Element> elementList = consumerShb.receive(1, 40);
            consumerShb.ack(elementList.get(0).getId());
            ByteBuffer buffer2 = elementList.get(0).getBuffer();
            Assert.assertEquals(buffer, buffer2);
        } finally {
            producerShb.close();
            consumerShb.close();
            clientStreamShb.close();
        }
    }

    @Test
    public void testSendDirectByteBuffer() {
        logger.info("******************** testSendDirectByteBuffer **********************");
        String streamName = "testSendDirectByteBuffer";
        StreamClient client = getStreamClient(1).get(0);
        Producer producer = client.createProducer(streamName);
        Consumer consumer = client.subscribe(streamName, "sub", SubscriptionType.STREAM);
        try {
            String value = "10101010";
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length);
            buffer.put(bytes);
            producer.send(buffer);

            List<Element> elements = consumer.receive(1, 500);
            Assert.assertEquals(elements.size(), 1);

            ByteBuffer getBuffer = elements.get(0).getBuffer();
            Assert.assertEquals(getBuffer, buffer.flip());
        } finally {
            producer.close();
            consumer.close();
            client.close();
        }
    }

    @Test
    public void testStreamSametimeSendDemo() throws InterruptedException {
        logger.info("******************** testStreamSametimeSendDemo **********************");
        String streamName = "stream_sametime_send";
        StreamClient clientStreamSsd = getStreamClient(1).get(0);
        Producer producerSsd = clientStreamSsd.createProducer(streamName);
        Consumer consumerSsd = clientStreamSsd.subscribe(streamName, "sub_object_stream2", SubscriptionType.STREAM);
        try {
            String value = "10101010";
            String value1 = "01010101";
            byte[] arr = value.getBytes(StandardCharsets.UTF_8);
            byte[] arr1 = value1.getBytes(StandardCharsets.UTF_8);

            ByteBuffer buffer = ByteBuffer.allocateDirect(arr.length);
            buffer.put(arr);
            producerSsd.send(buffer);

            Thread.sleep(5);
            ByteBuffer buffer1 = ByteBuffer.allocateDirect(arr1.length);
            buffer1.put(arr1);
            producerSsd.send(buffer1);

            List<Element> elements = consumerSsd.receive(2, 500);
            Assert.assertEquals(buffer.flip(), elements.get(0).getBuffer());
            Assert.assertEquals(buffer1.flip(), elements.get(1).getBuffer());
            consumerSsd.ack(elements.get(1).getId());
        } finally {
            producerSsd.close();
            consumerSsd.close();
            clientStreamSsd.close();
        }
    }

    @Test
    public void testStreamContinousSendDemo() {
        logger.info("******************** testStreamContinousSendDemo **********************");
        String streamName = "stream_continue_send";
        StreamClient clientCsd = getStreamClient(1).get(0);
        Producer producerCsd = clientCsd.createProducer(streamName);
        Consumer consumerCsd = clientCsd.subscribe(streamName, "sub_object_stream3", SubscriptionType.STREAM);
        try {
            String value = "1010101010101010";
            List<ByteBuffer> sendByteBufferList = new ArrayList<ByteBuffer>(10);
            for (int i = 1; i < 11; i++) {
                String tmparr = value.substring(0, i);
                byte[] arr = tmparr.getBytes(StandardCharsets.UTF_8);
                ByteBuffer buffer = ByteBuffer.allocateDirect(arr.length);
                buffer.put(arr);
                producerCsd.send(buffer);
                buffer.flip();
                sendByteBufferList.add(buffer);
            }
            List<Element> elementList = consumerCsd.receive(15, 20);

            int elementListCount = 0;
            for (Element element : elementList) {
                consumerCsd.ack(element.getId());
                ByteBuffer buffer2 = element.getBuffer();
                Assert.assertEquals(sendByteBufferList.get(elementListCount), buffer2);
                elementListCount++;
            }
        } finally {
            producerCsd.close();
            consumerCsd.close();
            clientCsd.close();
        }
    }

    @Test
    public void testStreamSametimeFlushDemo() throws InterruptedException {
        logger.info("******************** testStreamSametimeFlushDemo **********************");
        String streamName = "stream_sametime_flush";
        StreamClient clientSfd = getStreamClient(1).get(0);
        Producer producerSfd = clientSfd.createProducer(streamName);
        Consumer consumerSfd = clientSfd.subscribe(streamName, "sub_object_stream4", SubscriptionType.STREAM);
        try {
            String value = "10101010";
            byte[] arr = value.getBytes(StandardCharsets.UTF_8);
            ByteBuffer buffer = ByteBuffer.allocateDirect(arr.length);
            buffer.put(arr);
            producerSfd.send(buffer);

            List<Element> elements = consumerSfd.receive(1, 10);
            ByteBuffer buffer2 = elements.get(0).getBuffer();
            buffer.flip();

            Assert.assertEquals(buffer, buffer2);
            consumerSfd.ack(elements.get(0).getId());
        } finally {
            producerSfd.close();
            consumerSfd.close();
            clientSfd.close();
        }
    }

    @Test
    public void testStreamSetPagesizeDemo() {
        logger.info("******************** testStreamSetPagesizeDemo **********************");
        String streamName = "stream_set_pagesize";
        StreamClient clientSpd = getStreamClient(1).get(0);
        Producer producerSpd = clientSpd.createProducer(streamName, 1, 4 * 1024);
        Consumer consumerSpd = clientSpd.subscribe(streamName, "sub_object_stream5", SubscriptionType.STREAM);
        try {
            String value = "10101010";
            byte[] arr = value.getBytes(StandardCharsets.UTF_8);
            ByteBuffer buffer = ByteBuffer.allocateDirect(arr.length);
            buffer.put(arr);
            producerSpd.send(buffer);
            List<Element> elementList = consumerSpd.receive(1, 10);
            ByteBuffer buffer2 = elementList.get(0).getBuffer();

            Assert.assertEquals(buffer.flip(), buffer2);
            consumerSpd.ack(elementList.get(0).getId());
        } finally {
            producerSpd.close();
            consumerSpd.close();
            clientSpd.close();
        }
    }

    @Test
    public void testStreamSetDefaultSizeDemo() {
        logger.info("******************** testStreamSetDefaultSizeDemo **********************");
        String streamName = "stream_set_default_size";
        StreamClient clientSds = getStreamClient(1).get(0);
        Producer producerSds = clientSds.createProducer(streamName, 2);
        Consumer consumerSds = clientSds.subscribe(streamName, "sub_object_stream6", SubscriptionType.STREAM);
        try {
            String value = "10101010";
            byte[] arr = value.getBytes(StandardCharsets.UTF_8);
            ByteBuffer buffer = ByteBuffer.allocateDirect(arr.length);
            buffer.put(arr);
            producerSds.send(buffer);
            List<Element> elementList = consumerSds.receive(1, 10);
            ByteBuffer buffer2 = elementList.get(0).getBuffer();

            Assert.assertEquals(buffer.flip(), buffer2);

            consumerSds.ack(elementList.get(0).getId());
        } finally {
            producerSds.close();
            consumerSds.close();
            clientSds.close();
        }
    }

    @Test
    public void testMultiProducerSetDemo() {
        logger.info("******************** testMultiProducerSetDemo **********************");
        StreamClient clientStreamPsd = getStreamClient(1).get(0);
        String streamName = "stream_multi_producer";

        int baseSize = 1024 * 4;
        for (int i = 1; i < 6; i++) {
            Producer producerPsd1 = clientStreamPsd.createProducer(streamName, 1 + i / 3, baseSize);
            producerPsd1.close();
        }
        for (int i = 1; i < 6; i++) {
            Producer producerPsd2 = clientStreamPsd.createProducer(streamName, 5, baseSize);
            producerPsd2.close();
        }
        clientStreamPsd.close();
    }

    @Test
    public void testSendRecvOneSmallElement() {
        logger.info("******************** testSendRecvOneSmallElement **********************");
        String streamName = "streamSrose";
        StreamClient clientAsd = getStreamClient(1).get(0);
        Producer producerAsd = clientAsd.createProducer(streamName);
        Consumer consumerAsd = clientAsd.subscribe(streamName, "sub1", SubscriptionType.STREAM);
        try {
            String value = "10101010";
            byte[] arr = value.getBytes(StandardCharsets.UTF_8);
            ByteBuffer buffer = ByteBuffer.allocateDirect(arr.length);
            buffer.put(arr);
            producerAsd.send(buffer);

            List<Element> elements = consumerAsd.receive(1, 10);
            ByteBuffer buffer2 = elements.get(0).getBuffer();
            Assert.assertEquals(buffer.flip(), buffer2);

            consumerAsd.ack(elements.get(0).getId());
        } finally {
            producerAsd.close();
            consumerAsd.close();
            clientAsd.close();
        }
    }

    @Test
    public void testSendRecvOneBigElement() {
        logger.info("******************** tectSendRecvOneBigElement **********************");
        String streamName = "streamSrobe";
        StreamClient clientAsd = getStreamClient(1).get(0);
        Producer producerAsd = clientAsd.createProducer(streamName);
        Consumer consumerAsd = clientAsd.subscribe(streamName, "sub1", SubscriptionType.STREAM);
        try {
            // generate elementData
            int elementSize = 1024 * 1024;
            ByteBuffer buffer = TestUtils.getRandomDirectBuffer(elementSize);
            producerAsd.send(buffer);

            List<Element> elements = consumerAsd.receive(1, 10);
            ByteBuffer buffer2 = elements.get(0).getBuffer();
            consumerAsd.ack(elements.get(0).getId());
            Assert.assertEquals(buffer, buffer2);
        } finally {
            producerAsd.close();
            consumerAsd.close();
            clientAsd.close();
        }
    }

    @Test
    public void testMulSubsAtDifferTime() {
        logger.info("******************** testMulSubsAtDifferTime **********************");
        StreamClient client = getStreamClient(1).get(0);
        String streamName = "stream";
        Producer producer = client.createProducer(streamName, -1L);
        List<String> subNameList = Arrays.asList("sub1", "sub2");
        Consumer consumer1 = client.subscribe(streamName, subNameList.get(0), SubscriptionType.STREAM);
        try {
            // write two page data
            int pageSize = 1024;
            int elementNum = 2 * pageSize;
            int elementSize = 1025; // if we wanna ack, we should let it change page first.

            List<ByteBuffer> elementDatas = TestUtils.getRandomDirectBufferList(elementSize, elementNum);
            for (int i = 0; i < elementNum; i++) {
                producer.send(elementDatas.get(i));
            }

            // consumer1 receives the first half of the elements and ack
            List<Element> elementsList1 = consumer1.receive(pageSize, 10);
            int elementListIndex = 0;

            for (int i = 0; i < elementsList1.size(); i++) {
                ByteBuffer buffer1 = elementsList1.get(i).getBuffer();
                Assert.assertEquals(buffer1, elementDatas.get(elementListIndex));
                elementListIndex++;
            }
            consumer1.ack(elementsList1.get(elementListIndex - 1).getId());
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                logger.warning("Test case failed: testMulSubsAtDifferTime, do something to deal with Exception");
            }

            // Create consumer2 and recv data, it should the second half of the elements
            Consumer consumer2 = client.subscribe(streamName, subNameList.get(1), SubscriptionType.STREAM);
            try {
                List<Element> elementsList2 = consumer2.receive(pageSize, 0);
                for (int i = 0; i < elementsList2.size(); i++) {
                    ByteBuffer buffer2 = elementsList2.get(i).getBuffer();
                    Assert.assertEquals(buffer2, elementDatas.get(elementListIndex));
                    elementListIndex++;
                }
            } finally {
                consumer2.close();
            }
        } finally {
            producer.close();
            consumer1.close();
            client.close();
        }
    }

    @Test
    public void testMulSubsMulConsumers() {
        StreamClient clientOsmc = getStreamClient(1).get(0);
        String streamName = "streamMsmc";
        Producer producerOsmc = clientOsmc.createProducer(streamName);
        List<String> subNameList = Arrays.asList("sub1", "sub2");
        Consumer consumerOsmc1 = clientOsmc.subscribe(streamName, subNameList.get(0), SubscriptionType.STREAM);
        Consumer consumerOsmc2 = clientOsmc.subscribe(streamName, subNameList.get(1), SubscriptionType.STREAM);
        try {
            // write two page data
            int pageSize = 4096;
            int elementNum = 2 * pageSize;
            int elementSize = 8;
            List<ByteBuffer> elementDatas = TestUtils.getRandomDirectBufferList(elementSize, elementNum);
            for (int i = 0; i < elementNum; i++) {
                producerOsmc.send(elementDatas.get(i));
            }

            // consumerOsmc1 receives the first half of the elements and ack
            List<Element> elementsList1 = consumerOsmc1.receive(pageSize, 10);
            int elementListIndex = 0;
            for (int i = 0; i < elementsList1.size(); i++) {
                ByteBuffer buffer1 = elementsList1.get(i).getBuffer();
                Assert.assertEquals(buffer1, elementDatas.get(elementListIndex));
                elementListIndex++;
            }
            consumerOsmc1.ack(elementsList1.get(elementListIndex - 1).getId());

            // consumerOsmc1 receives data,it should receive the second half of the elements
            List<Element> elementsList11 = consumerOsmc1.receive(pageSize, 10);
            for (int i = 0; i < elementsList11.size(); i++) {
                ByteBuffer buffer1 = elementsList11.get(i).getBuffer();
                Assert.assertEquals(buffer1, elementDatas.get(elementListIndex));
                elementListIndex++;
            }

            // consumerOsmc2 receives data,it should receive all the data
            List<Element> elementsList2 = consumerOsmc2.receive(elementNum, 10);
            for (int i = 0; i < elementsList2.size(); i++) {
                ByteBuffer buffer2 = elementsList2.get(i).getBuffer();
                Assert.assertEquals(buffer2, elementDatas.get(i));
            }
        } finally {
            producerOsmc.close();
            consumerOsmc1.close();
            consumerOsmc2.close();
            clientOsmc.close();
        }
    }

    @Test
    public void testAllowReceiveEmpty() {
        logger.info("******************** testAllowReceiveEmpty **********************");
        String streamName = "AllowReceiveEmpty";
        StreamClient client = getStreamClient(1).get(0);
        Producer producer = client.createProducer(streamName);
        Consumer consumer = client.subscribe(streamName, "sub", SubscriptionType.STREAM);
        try {
            String value = "10101010";
            ByteBuffer buffer = ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
            producer.send(buffer);

            List<Element> elements = consumer.receive(1, 500);
            Assert.assertEquals(elements.size(), 1);
            Assert.assertEquals(elements.get(0).getBuffer(), buffer);

            elements = consumer.receive(1, 10);
            Assert.assertEquals(elements.size(), 0);
        } finally {
            producer.close();
            consumer.close();
            client.close();
        }
    }

    @Test
    public void testAck() {
        logger.info("******************** InvalidAck **********************");
        String streamName = "InvalidAck";
        StreamClient clientIak = getStreamClient(1).get(0);
        try {
            Producer producerIak = clientIak.createProducer(streamName);

            int elementNum = 10;
            int elementSize = 524289; // if we wanna ack, we should let it change page first.
            List<ByteBuffer> elements = TestUtils.getRandomDirectBufferList(elementSize, elementNum);
            Consumer consumerIak = clientIak.subscribe(streamName, "sub0", SubscriptionType.STREAM);
            for (int i = 0; i < elementNum; i++) {
                producerIak.send(elements.get(i));
            }
            List<Element> elementList = consumerIak.receive(5, 80);
            for (int i = 0; i < elementList.size(); i++) {
                ByteBuffer buffer = elementList.get(i).getBuffer();
                Assert.assertEquals(buffer, elements.get(i));
            }
            consumerIak.ack(5);
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                logger.warning("Test case failed: testAck, do something to deal with Exception");
            }

            Consumer consumer = clientIak.subscribe(streamName, "sub1", SubscriptionType.STREAM);
            List<Element> elementList1 = consumer.receive(10, 80);
            for (int i = 0; i < elementList1.size(); i++) {
                ByteBuffer buffer = elementList1.get(i).getBuffer();
                Assert.assertEquals(buffer, elements.get(i + 5));
            }
            consumer.ack(10);
            consumerIak.close();
            consumer.close();
            producerIak.close();
        } finally {
            clientIak.close();
        }
    }

    @Test(expected = DataSystemException.class)
    public void testSendAfterCloseProducer() {
        logger.info("******************** testSendAfterCloseProducer **********************");
        String streamName = "SendAfterCloseProducer";
        StreamClient clientSac = getStreamClient(1).get(0);
        Producer producerSac = clientSac.createProducer(streamName);
        Consumer consumerSac = clientSac.subscribe(streamName, "sub_object_stream", SubscriptionType.STREAM);
        try {
            String value = "10101010";
            byte[] arr = value.getBytes(StandardCharsets.UTF_8);
            ByteBuffer buffer = ByteBuffer.allocateDirect(arr.length);
            buffer.put(arr);
            producerSac.close();
            producerSac.send(buffer);
        } finally {
            consumerSac.close();
            clientSac.close();
        }
    }

    @Test(expected = DataSystemException.class)
    public void testReceiveAfterCloseConsumer() {
        logger.info("******************** testReceiveAfterCloseConsumer **********************");
        String streamName = "testReceiveAfterCloseConsumer";
        StreamClient clientRac = getStreamClient(1).get(0);
        Producer producerRac = clientRac.createProducer(streamName);
        Consumer consumerRac = clientRac.subscribe(streamName, "sub_object_stream", SubscriptionType.STREAM);
        try {
            String value = "10101010";
            byte[] arr = value.getBytes(StandardCharsets.UTF_8);
            ByteBuffer buffer = ByteBuffer.allocateDirect(arr.length);
            buffer.put(arr);
            producerRac.send(buffer);
            consumerRac.close();
            List<Element> elementList = consumerRac.receive(1, 10);
            ByteBuffer bufferRec = elementList.get(0).getBuffer();
            Assert.assertEquals(bufferRec, buffer.flip());
        } finally {
            producerRac.close();
            clientRac.close();
        }
    }

    @Test(expected = DataSystemException.class)
    public void testAckAfterCloseConsume() {
        logger.info("******************** testAckAfterCloseConsume **********************");
        String streamName = "testAckAfterCloseConsume";
        StreamClient clientAac = getStreamClient(1).get(0);
        Producer producerAac = clientAac.createProducer(streamName);
        Consumer consumerAac = clientAac.subscribe(streamName, "sub_object_stream", SubscriptionType.STREAM);
        try {
            String value = "10101010";
            byte[] arr = value.getBytes(StandardCharsets.UTF_8);
            ByteBuffer buffer = ByteBuffer.allocateDirect(arr.length);
            buffer.put(arr);
            producerAac.send(buffer);
            List<Element> elementList = consumerAac.receive(1, 10);
            consumerAac.close();
            consumerAac.ack(elementList.get(0).getId());
        } finally {
            producerAac.close();
            clientAac.close();
        }
    }

    /**
     * Input stream cache information.
     */
    static class InputStreamInfo {
        String streamName;
        int producerNum;
        int subNum;

        public InputStreamInfo(String streamName, int producerNum, int subNum) {
            this.streamName = streamName;
            this.producerNum = producerNum;
            this.subNum = subNum;
        }
    }

    /**
     * Create producers and consumers with multiple threads.
     *
     * @param clientMSSPSC    The StreamClient instance.
     * @param inputStreamInfo Input stream cache information.
     * @param producerList    Producer list.
     * @param consumerList    Consumer list.
     */
    private static void createProducersAndConsumers(StreamClient clientMSSPSC, InputStreamInfo inputStreamInfo,
            Collection<Producer> producerList, Collection<Consumer> consumerList) {
        ThreadPoolExecutor pool = new ThreadPoolExecutor(5, 20, 60L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(50),
                new ThreadPoolExecutor.CallerRunsPolicy());
        // Create producers.
        List<Future<Producer>> producerThread = new ArrayList<>(inputStreamInfo.producerNum);
        for (int i = 0; i < inputStreamInfo.producerNum; i++) {
            Future<Producer> future = pool.submit(() -> {
                return clientMSSPSC.createProducer(inputStreamInfo.streamName);
            });
            producerThread.add(future);
        }
        // Create consumers.
        List<Future<Consumer>> consumerThread = new ArrayList<>(inputStreamInfo.subNum);
        for (int i = 0; i < inputStreamInfo.subNum; i++) {
            int finalI = i;
            Future<Consumer> future = pool.submit(() -> {
                return clientMSSPSC.subscribe(inputStreamInfo.streamName, "subs" + finalI, SubscriptionType.STREAM);
            });
            consumerThread.add(future);
        }
        // Waiting for all threads currently to end and input the result.
        try {
            for (Future<Producer> future : producerThread) {
                producerList.add(future.get());
            }
            for (Future<Consumer> future : consumerThread) {
                consumerList.add(future.get());
            }
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
        pool.shutdown();
    }

    /**
     * Send and receive data through multiple threads.
     *
     * @param producerList    Producer list.
     * @param consumerList    Consumer list.
     */
    private static void multiSendRecv(List<Producer> producerList, List<Consumer> consumerList) {
        ThreadPoolExecutor pool = new ThreadPoolExecutor(5, 20, 60L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(50),
                new ThreadPoolExecutor.CallerRunsPolicy());
        // The producers to send data.
        List<Future<?>> producerThread = new ArrayList<>(producerList.size());
        for (int i = 0; i < producerList.size(); i++) {
            int finalI = i;
            Runnable producerSendT = new Runnable() {
                @Override
                public void run() {
                    Producer producer = producerList.get(finalI);
                    String value = "101010";
                    byte[] arr = value.getBytes(StandardCharsets.UTF_8);
                    ByteBuffer buffer = ByteBuffer.allocateDirect(arr.length);
                    buffer.put(arr);
                    producer.send(buffer);
                    producer.close();
                }
            };
            Future<?> future = pool.submit(producerSendT);
            producerThread.add(future);
        }
        // The consumers to receive data.
        List<Future<?>> consumerThread = new ArrayList<>(consumerList.size());
        for (int i = 0; i < consumerList.size(); i++) {
            int finalI = i;
            Runnable consumerReceiveT = new Runnable() {
                @Override
                public void run() {
                    Consumer consumer = consumerList.get(finalI);
                    List<Element> elements = consumer.receive(producerList.size(), 30);
                    Assert.assertEquals(producerList.size(), elements.size());
                    consumer.close();
                }
            };
            Future<?> future = pool.submit(consumerReceiveT);
            consumerThread.add(future);
        }
        // Waiting for all threads currently to end and input the result.
        try {
            for (Future<?> future : producerThread) {
                future.get();
            }
            for (Future<?> future : consumerThread) {
                future.get();
            }
        } catch (ExecutionException | InterruptedException e) {
            logger.warning("Test case failed: multiSendRecv, do something to deal with DataSystemException");
        }
    }

    @Test
    public void testMPMC() {
        logger.info("******************** testMPMC **********************");
        StreamClient clientMSSPSC = getStreamClient(1).get(0);
        try {
            List<Producer> producerList = new ArrayList<>();
            List<Consumer> consumerList = new ArrayList<>();
            InputStreamInfo inputStreamInfo = new InputStreamInfo("streamMPMC", 10, 10);
            createProducersAndConsumers(clientMSSPSC, inputStreamInfo, producerList, consumerList);
            multiSendRecv(producerList, consumerList);
        } finally {
            clientMSSPSC.close();
        }
    }

    @Test
    public void testMPSC() {
        logger.info("******************** testMPSC **********************");
        StreamClient clientMSSPSC = getStreamClient(1).get(0);
        try {
            List<Producer> producerList = new ArrayList<>();
            List<Consumer> consumerList = new ArrayList<>();
            InputStreamInfo inputStreamInfo = new InputStreamInfo("streamMPSC", 10, 1);
            createProducersAndConsumers(clientMSSPSC, inputStreamInfo, producerList, consumerList);
            multiSendRecv(producerList, consumerList);
        } finally {
            clientMSSPSC.close();
        }
    }

    @Test
    public void testSPMC() {
        logger.info("******************** testSPMC **********************");
        StreamClient clientMSSPSC = getStreamClient(1).get(0);
        try {
            List<Producer> producerList = new ArrayList<>();
            List<Consumer> consumerList = new ArrayList<>();
            InputStreamInfo inputStreamInfo = new InputStreamInfo("streamSPMC", 1, 10);
            createProducersAndConsumers(clientMSSPSC, inputStreamInfo, producerList, consumerList);
            multiSendRecv(producerList, consumerList);
        } finally {
            clientMSSPSC.close();
        }
    }

    @Test
    public void testClientDeleteStream() {
        logger.info("******************** testClientDeleteStream **********************");
        String streamName = "StreamName";
        StreamClient client = getStreamClient(1).get(0);
        final long pageSize = 4 * 1024;
        final long maxStreamSize = 1024 * 1024;
        Producer producer = client.createProducer(streamName, -1, pageSize, maxStreamSize);
        Consumer consumer = client.subscribe(streamName, "subName", SubscriptionType.STREAM);
        producer.close();
        consumer.close();
        client.deleteStream(streamName);
        client.close();
    }

    @Test
    public void testFinalizeToFreeJNIPtr() {
        userBehavior();
        System.gc();
    }

    private void userBehavior() {
        StreamClient client = getStreamClient(1).get(0);
        String streamName = "StreamNameUB";
        Producer producer = client.createProducer(streamName);
        Consumer consumer = client.subscribe(streamName, "subName", SubscriptionType.STREAM);
        String value = "abc";
        byte[] arr = value.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocateDirect(arr.length);
        buffer.put(arr);
        producer.send(buffer);
        List<Element> elements = consumer.receive(1, 0);
        ByteBuffer buffer2 = elements.get(0).getBuffer();
        Assert.assertEquals(buffer.flip(), buffer2);
    }

    @Test
    public void testEqualsToStringAndHashCodeMethod() {
        String streamName = "testEqualsToStringAndHashCodeMethod";
        StreamClient client = getStreamClient(1).get(0);
        Producer producer1 = client.createProducer(streamName);
        Producer producer2 = client.createProducer(streamName);
        Consumer consumer1 = client.subscribe(streamName, "subName1", SubscriptionType.STREAM);
        Consumer consumer2 = client.subscribe(streamName, "subName2", SubscriptionType.STREAM);
        try {
            String str1 = producer1.toString();
            String str2 = producer2.toString();
            Assert.assertFalse(str1.equals(str2));
            Assert.assertFalse(producer1.equals(producer2));
            Assert.assertFalse(producer1.hashCode() == producer2.hashCode());

            String str3 = consumer1.toString();
            String str4 = consumer2.toString();
            Assert.assertFalse(str3.equals(str4));
            Assert.assertFalse(consumer1.equals(consumer2));
            Assert.assertFalse(consumer1.hashCode() == consumer2.hashCode());
        } finally {
            producer1.close();
            producer2.close();
            consumer1.close();
            consumer2.close();
            client.close();
        }
    }

    @Test
    public void testSendDirectBufferZeroToLimit() {
        logger.info("******************** testSendDirectBufferZeroToLimit **********************");
        String streamName = "testSendDirectBufferZeroToLimit";
        StreamClient clientAac = getStreamClient(1).get(0);
        Producer producerAac = clientAac.createProducer(streamName);
        Consumer consumerAac = clientAac.subscribe(streamName, "sub_object_stream", SubscriptionType.STREAM);
        try {
            int size = 10;
            ByteBuffer data = TestUtils.getRandomDirectBuffer(size);
            // The pos and limit positions of the ByteBuffer are modified.
            int limit = 5;
            data.limit(limit);
            producerAac.send(data);
            List<Element> elementList = consumerAac.receive(1, 10);
            ByteBuffer buffer = elementList.get(0).getBuffer();
            Assert.assertEquals(buffer.capacity(), limit);
            Assert.assertEquals(data.slice(), buffer);
            consumerAac.close();
        } finally {
            producerAac.close();
            clientAac.close();
        }
    }

    @Test
    public void testSendHeapBufferZeroToLimit() {
        logger.info("******************** testSendHeapBufferZeroToLimit **********************");
        String streamName = "testAckAfterCloseConsume";
        StreamClient clientAac = getStreamClient(1).get(0);
        Producer producerAac = clientAac.createProducer(streamName);
        Consumer consumerAac = clientAac.subscribe(streamName, "sub_object_stream", SubscriptionType.STREAM);
        try {
            int size = 10;
            ByteBuffer data = TestUtils.getRandomHeapBuffer(size);
            // The pos and limit positions of the ByteBuffer are modified.
            int position = 3;
            int limit = 6;
            data.position(position);
            data.limit(limit);
            // java.nio.HeapByteBuffer[pos=3 lim=6 cap=10]
            producerAac.send(data);
            List<Element> elementList = consumerAac.receive(1, 10);
            ByteBuffer buffer = elementList.get(0).getBuffer();
            Assert.assertEquals(buffer.capacity(), limit);
            data.position(0);
            Assert.assertEquals(data.slice(), buffer);
            consumerAac.close();
        } finally {
            producerAac.close();
            clientAac.close();
        }
    }

    @Test
    public void testGetStatisticsMessage() {
        logger.info("******************** testGetStatisticsMessage **********************");
        StreamClient client = getStreamClient(1).get(0);
        String streamName = "testGetStatisticsMessage";
        Producer producer = client.createProducer(streamName);
        Consumer consumer1 = client.subscribe(streamName, "subName1", SubscriptionType.STREAM);
        Consumer consumer2 = client.subscribe(streamName, "subName2", SubscriptionType.STREAM);
        try {
            String value = "10101010";
            byte[] arr = value.getBytes(StandardCharsets.UTF_8);
            ByteBuffer buffer = ByteBuffer.allocateDirect(arr.length);
            buffer.put(arr);

            producer.send(buffer);
            producer.send(buffer);

            List<Element> elements = consumer1.receive(2, 0);
            StatisticsMessage statistics = new StatisticsMessage();
            consumer1.getStatisticsMessage(statistics);
            Assert.assertEquals(elements.size(), 2);
            Assert.assertEquals(elements.size(), statistics.getTotalElements().longValue());
            Assert.assertEquals(elements.size(), statistics.getNotProcessedElements().longValue());

            consumer1.ack(elements.get(0).getId());
            statistics = new StatisticsMessage();
            consumer1.getStatisticsMessage(statistics);
            Assert.assertEquals(elements.size(), statistics.getTotalElements().longValue());
            Assert.assertEquals(1, statistics.getNotProcessedElements().longValue());

            consumer1.ack(elements.get(1).getId());
            statistics = new StatisticsMessage();
            consumer1.getStatisticsMessage(statistics);
            Assert.assertEquals(elements.size(), statistics.getTotalElements().longValue());
            Assert.assertEquals(0, statistics.getNotProcessedElements().longValue());

            elements = consumer2.receive(2, 0);
            consumer2.ack(elements.get(1).getId());
            statistics = new StatisticsMessage();
            consumer2.getStatisticsMessage(statistics);
            Assert.assertEquals(elements.size(), statistics.getTotalElements().longValue());
            Assert.assertEquals(0, statistics.getNotProcessedElements().longValue());
        } finally {
            producer.close();
            consumer1.close();
            consumer2.close();
            client.close();
        }
    }

    @Test
    public void testRetainForNumConsumers() {
        logger.info("******************** testRetainForNumConsumers **********************");
        String streamName = "stream_retain";
        StreamClient client = getStreamClient(1).get(0);
        final long pageSize = 4 * 1024;
        final long maxStreamSize = 1024 * 1024;
        Producer producer = client.createProducer(streamName, 0, pageSize, maxStreamSize, false, 1);
        String value = "10101010";
        byte[] arr = value.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocateDirect(arr.length);
        buffer.put(arr);
        producer.send(buffer);

        // late consumer should receive
        Consumer consumer = client.subscribe(streamName, "sub1", SubscriptionType.STREAM);
        List<Element> elementList = consumer.receive(1, 20);
        Assert.assertEquals(elementList.size(), 1);
        consumer.ack(elementList.get(0).getId());

        // second late consumer should not
        Consumer consumer2 = client.subscribe(streamName, "sub2", SubscriptionType.STREAM);
        List<Element> elementList2 = consumer2.receive(10);
        Assert.assertEquals(elementList2.size(), 0);
        producer.close();
        consumer.close();
        consumer2.close();
        client.close();
    }

    @Test
    public void testCreateProducerReserveSize() {
        logger.info("******************** testCreateProducerReserveSize **********************");
        // This testcase intends to test that invalid reserve size will lead to CreateProducer failure.
        String streamName = "stream_reserveSize";
        StreamClient client = getStreamClient(1).get(0);
        final long pageSize = 8 * 1024;
        final long maxStreamSize = 64 * 1024 * 1024;

        // Valid reserveSize should be less than or equal to max stream size.
        long reserveSize1 = pageSize + maxStreamSize;
        Assert.assertThrows(DataSystemException.class, () -> {
            Producer producer1 = client.createProducer(streamName, 0, pageSize, maxStreamSize, false, 0,
                    false, reserveSize1);
        });

        // Valid reserveSize should be a multiple of page size.
        long reserveSize2 = 12 * 1024;
        Assert.assertThrows(DataSystemException.class, () -> {
            Producer producer1 = client.createProducer(streamName, 0, pageSize, maxStreamSize, false, 0,
                    false, reserveSize2);
        });

        // 0 is an acceptable input for reserveSize, the default reserveSize will then be page size.
        long reserveSize3 = 0L;
        Producer producer3 = client.createProducer(streamName, 0, pageSize, maxStreamSize, false, 0,
                false, reserveSize3);

        producer3.close();
        client.close();
    }
}