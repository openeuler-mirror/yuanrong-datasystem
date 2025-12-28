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

package org.yuanrong.datasystem.object;

import org.yuanrong.datasystem.Cluster;
import org.yuanrong.datasystem.ConnectOptions;
import org.yuanrong.datasystem.ConsistencyType;
import org.yuanrong.datasystem.DataSystemException;
import org.yuanrong.datasystem.TestUtils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Test java object api
 *
 * @since 2022-08-05
 */
public class ObjectClientTest {
    private static Cluster cluster;
    private static int workerNums = 2;

    @BeforeClass
    public static void beforeClass() throws IOException {
        cluster = new Cluster(workerNums, 0);
        cluster.init(ObjectClientTest.class.getSimpleName());
        cluster.startAll();
    }

    @AfterClass
    public static void afterClass() {
        if (cluster != null) {
            cluster.stopAll();
        }
    }

    /**
     * Get Cross-Node ObjectClient list.
     *
     * @param number The ObjectClient number.(number <= workerNums)
     * @return The ObjectClient list.
     */
    private List<ObjectClient> getObjectClient(int number) {
        if (number > workerNums) {
            String message = "Invalid Argument, starting " + workerNums + " workers, but expecting to create "
                    + number + " cross-node Object Clients.";
            throw new DataSystemException(message);
        }
        List<ObjectClient> clients = new ArrayList<>(number);
        for (int i = 0; i < number; i++) {
            String workerAddr = cluster.getWorkerAddr(i);
            String[] temp = workerAddr.split(":");
            String ip = temp[0];
            int port = Integer.parseInt(temp[1]);
            ConnectOptions connectOpts = new ConnectOptions(ip, port);
            ObjectClient client = new ObjectClient(connectOpts);
            clients.add(client);
        }
        return clients;
    }

    /**
     * To init CreateParam.
     *
     * @return CreateParam.
     */
    private CreateParam initCreateParam() {
        CreateParam createParam = new CreateParam();
        createParam.setConsistencyType(ConsistencyType.PRAM);
        return createParam;
    }

    @Test
    public void testConnect() {
        ObjectClient client = getObjectClient(1).get(0);
        client.close();
    }

    @Test
    public void testClientGet() {
        ObjectClient client = getObjectClient(1).get(0);
        List<String> objectKeys = TestUtils.getUUIDList(3);
        CreateParam createParam = initCreateParam();
        int size1 = 50;
        int size2 = 100;
        try {
            Buffer buffer1 = client.create(objectKeys.get(0), size1, createParam);
            Buffer buffer2 = client.create(objectKeys.get(1), size2, createParam);
            try {
                ByteBuffer data1 = TestUtils.getRandomDirectBuffer(size1);
                buffer1.wLatch();
                buffer1.memoryCopy(data1);
                buffer1.seal();
                buffer1.unWLatch();
                ByteBuffer data2 = TestUtils.getRandomDirectBuffer(size2);
                buffer2.wLatch();
                buffer2.memoryCopy(data2);
                buffer2.seal();
                buffer2.unWLatch();

                List<Buffer> buffers = client.get(objectKeys, 5);
                try {
                    Assert.assertEquals(data1, buffers.get(0).immutableData());
                    Assert.assertEquals(data2, buffers.get(1).immutableData());
                    Assert.assertNull(buffers.get(2));
                } finally {
                    buffers.get(0).close();
                    buffers.get(1).close();
                }
            } finally {
                buffer1.close();
                buffer2.close();
            }
        } finally {
            client.close();
        }
    }

    @Test
    public void testClientCreateAndPublishAndDelete() {
        ObjectClient client = getObjectClient(1).get(0);
        try {
            List<String> objectKeys = TestUtils.getUUIDList(1);
            CreateParam createParam = initCreateParam();
            int size = 50;
            Buffer buffer = client.create(objectKeys.get(0), size, createParam);
            try {
                Assert.assertEquals(buffer.getSize(), size);
                ByteBuffer data = TestUtils.getRandomDirectBuffer(size);
                buffer.wLatch();
                buffer.memoryCopy(data);
                Assert.assertEquals(client.gIncreaseRef(objectKeys).size(), 0);
                buffer.publish();
                buffer.unWLatch();
                Assert.assertEquals(client.gDecreaseRef(objectKeys).size(), 0);
            } finally {
                buffer.close();
            }
        } finally {
            client.close();
        }
    }

    @Test
    public void testClientPutAndGet() {
        ObjectClient client = getObjectClient(1).get(0);
        try {
            List<String> objectKeys = TestUtils.getUUIDList(1);
            int size = 1024;
            ByteBuffer data = TestUtils.getRandomDirectBuffer(size);
            CreateParam createParam = initCreateParam();
            client.put(objectKeys.get(0), data, createParam);

            List<Buffer> buffers = client.get(objectKeys, 5);
            try {
                Assert.assertEquals(data, buffers.get(0).immutableData());
            } finally {
                for (Buffer buf : buffers) {
                    buf.close();
                }
            }
        } finally {
            client.close();
        }
    }

    @Test
    public void testClientCreatAndGetBigBytes() {
        ObjectClient client = getObjectClient(1).get(0);
        try {
            List<String> objectKeys = TestUtils.getUUIDList(1);
            CreateParam createParam = initCreateParam();
            int size = 5 * 1024 * 1024;
            Buffer buffer = client.create(objectKeys.get(0), size, createParam);
            try {
                Assert.assertEquals(buffer.getSize(), size);
                buffer.wLatch();
                ByteBuffer data = TestUtils.getRandomDirectBuffer(size);
                buffer.memoryCopy(data);
                buffer.seal();
                buffer.unWLatch();

                List<Buffer> buffers = client.get(objectKeys, 5);
                try {
                    Assert.assertEquals(data, buffers.get(0).immutableData());
                } finally {
                    for (Buffer buf1 : buffers) {
                        buf1.close();
                    }
                }
            } finally {
                buffer.close();
            }
        } finally {
            client.close();
        }
    }

    @Test
    public void testClientIncreaseAndDecreaseGlobalRef() {
        ObjectClient client = getObjectClient(1).get(0);
        try {
            List<String> objectKeys = TestUtils.getUUIDList(1);
            Assert.assertEquals(client.gIncreaseRef(objectKeys).size(), 0);
            Assert.assertEquals(client.gIncreaseRef(objectKeys).size(), 0);
            Assert.assertEquals(client.gIncreaseRef(objectKeys).size(), 0);
            Assert.assertEquals(client.gIncreaseRef(objectKeys).size(), 0);
            Assert.assertEquals(client.queryGlobalRefNum(objectKeys.get(0)), 1);

            Assert.assertEquals(client.gDecreaseRef(objectKeys).size(), 0);
            Assert.assertEquals(client.gDecreaseRef(objectKeys).size(), 0);
            Assert.assertEquals(client.gDecreaseRef(objectKeys).size(), 0);
            Assert.assertEquals(client.gDecreaseRef(objectKeys).size(), 0);
            Assert.assertEquals(client.queryGlobalRefNum(objectKeys.get(0)), 0);
        } finally {
            client.close();
        }
    }

    @Test
    public void testInvalidateBuffer() {
        ObjectClient client = getObjectClient(1).get(0);
        try {
            List<String> objectKeys = TestUtils.getUUIDList(1);
            CreateParam createParam = initCreateParam();
            int size = 50;
            Buffer buffer = client.create(objectKeys.get(0), size, createParam);
            try {
                Assert.assertEquals(buffer.getSize(), size);
                buffer.wLatch();
                ByteBuffer data = TestUtils.getRandomDirectBuffer(size);
                buffer.memoryCopy(data);
                buffer.publish();
                buffer.unWLatch();
                buffer.invalidateBuffer();
            } finally {
                buffer.close();
            }
        } finally {
            client.close();
        }
    }

    @Test
    public void testBuffer() {
        ObjectClient client = getObjectClient(1).get(0);
        try {
            List<String> objectKeys = TestUtils.getUUIDList(2);
            CreateParam createParam = initCreateParam();
            String value = "abc";
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            ByteBuffer data = ByteBuffer.allocateDirect(bytes.length);
            int size = value.length();
            Buffer buffer1 = client.create(objectKeys.get(0), size, createParam);
            try {
                // buffer1 to memoryCopy and public
                data.put(bytes);
                buffer1.wLatch();
                buffer1.memoryCopy(data);
                buffer1.publish();
                buffer1.unWLatch();
                buffer1.rLatch();
                ByteBuffer bufferGet1 = buffer1.immutableData();
                Assert.assertEquals(data.flip(), bufferGet1);
                buffer1.unRLatch();
                buffer1.invalidateBuffer();
            } finally {
                buffer1.close();
            }
            Buffer buffer2 = client.create(objectKeys.get(1), size, createParam);
            try {
                // buffer2 to memoryCopy and seal
                buffer2.wLatch();
                buffer2.memoryCopy(data);
                buffer2.seal();
                buffer2.unWLatch();
                buffer2.rLatch();
                ByteBuffer bufferGet2 = buffer2.mutableData();
                Assert.assertEquals(data, bufferGet2);
                buffer2.unRLatch();
                buffer2.invalidateBuffer();
            } finally {
                buffer2.close();
            }
        } finally {
            client.close();
        }
    }

    @Test
    public void testMemoryCopyHeapBuffer() {
        ObjectClient objClient = getObjectClient(1).get(0);
        try {
            List<String> objectKeys = TestUtils.getUUIDList(1);
            CreateParam createParam = initCreateParam();
            String value = "10101010";
            byte[] arr = value.getBytes(StandardCharsets.UTF_8);
            ByteBuffer data = ByteBuffer.allocate(arr.length);
            data.put(arr);
            int size = value.length();
            Buffer buffer = objClient.create(objectKeys.get(0), size, createParam);
            try {
                Assert.assertEquals(buffer.getSize(), size);
                buffer.wLatch();
                data.flip();
                buffer.memoryCopy(data);
                buffer.seal();
                buffer.unWLatch();
            } finally {
                buffer.close();
            }
            List<Buffer> buffers = objClient.get(objectKeys, 5);
            try {
                ByteBuffer bufferGet = buffers.get(0).immutableData();
                Assert.assertEquals(data, bufferGet);
            } finally {
                for (Buffer buf : buffers) {
                    buf.close();
                }
            }
        } finally {
            objClient.close();
        }
    }

    @Test
    public void testMemoryCopyDirectBuffer() {
        ObjectClient client = getObjectClient(1).get(0);
        try {
            List<String> objectKeys = TestUtils.getUUIDList(1);
            CreateParam createParam = initCreateParam();
            String value = "10101010";
            byte[] arr = value.getBytes(StandardCharsets.UTF_8);
            ByteBuffer buffer = ByteBuffer.allocateDirect(arr.length);
            buffer.put(arr);
            int size = value.length();
            Buffer buffer1 = client.create(objectKeys.get(0), size, createParam);
            try {
                Assert.assertEquals(buffer1.getSize(), size);

                buffer1.wLatch();
                buffer1.memoryCopy(buffer);
                buffer1.seal();
                buffer1.unWLatch();
            } finally {
                buffer1.close();
            }
            List<Buffer> buffers = client.get(objectKeys, 5);
            try {
                ByteBuffer bufferGet = buffers.get(0).immutableData();
                Assert.assertEquals(buffer.flip(), bufferGet);
            } finally {
                for (Buffer buff : buffers) {
                    buff.close();
                }
            }
        } finally {
            client.close();
        }
    }

    @Test
    public void testPutHeapBuffer() {
        ObjectClient client = getObjectClient(1).get(0);
        try {
            List<String> objectKeys = TestUtils.getUUIDList(1);
            CreateParam createParam = initCreateParam();
            // non-shared memory
            int size = 1 * 1024;
            ByteBuffer data1 = TestUtils.getRandomHeapBuffer(size);
            client.put(objectKeys.get(0), data1, createParam);
            List<Buffer> buffers1 = client.get(objectKeys, 5);
            try {
                Assert.assertEquals(data1, buffers1.get(0).immutableData());
            } finally {
                for (Buffer buf : buffers1) {
                    buf.close();
                }
            }
            // shared memory
            size = 500 * 1024;
            ByteBuffer data2 = TestUtils.getRandomHeapBuffer(size);
            client.put(objectKeys.get(0), data2, createParam);
            List<Buffer> buffers2 = client.get(objectKeys, 5);
            try {
                Assert.assertEquals(data2, buffers2.get(0).immutableData());
            } finally {
                for (Buffer buf : buffers1) {
                    buf.close();
                }
            }
        } finally {
            client.close();
        }
    }

    @Test
    public void testFinalizeToFreeJNIPtr() {
        userBehavior();
        System.gc();
    }

    private void userBehavior() {
        ObjectClient client = getObjectClient(1).get(0);
        List<String> objectKeys = TestUtils.getUUIDList(1);
        ByteBuffer data = TestUtils.getRandomDirectBuffer(10);
        CreateParam createParam = initCreateParam();
        client.put(objectKeys.get(0), data, createParam);
        List<Buffer> buffers = client.get(objectKeys, 5);
        Buffer buffer = buffers.get(0);
        Assert.assertEquals(data, buffer.immutableData());
    }

    @Test
    public void testEqualsToStringAndHashCodeMethod() {
        ObjectClient client = getObjectClient(1).get(0);
        try {
            List<String> objectKeys = TestUtils.getUUIDList(2);
            CreateParam createParam = initCreateParam();
            int size = 1024;
            Buffer buffer1 = client.create(objectKeys.get(0), size, createParam);
            Buffer buffer2 = client.create(objectKeys.get(1), size, createParam);
            try {
                String str1 = buffer1.toString();
                String str2 = buffer2.toString();
                Assert.assertFalse(str1.equals(str2));
                Assert.assertFalse(buffer1.equals(buffer2));
                Assert.assertFalse(buffer1.hashCode() == buffer2.hashCode());
            } finally {
                buffer1.close();
                buffer2.close();
            }
        } finally {
            client.close();
        }
    }

    @Test(expected = DataSystemException.class)
    public void testClientPublishNested() {
        ObjectClient client = getObjectClient(1).get(0);
        try {
            List<String> objectKeys = TestUtils.getUUIDList(3);
            Assert.assertEquals(client.gIncreaseRef(objectKeys).size(), 0);
            int size = 1024;
            CreateParam createParam = initCreateParam();
            ByteBuffer data1 = TestUtils.getRandomDirectBuffer(size);
            ByteBuffer data2 = TestUtils.getRandomDirectBuffer(size);
            client.put(objectKeys.get(1), data1, createParam);
            client.put(objectKeys.get(2), data2, createParam);

            List<String> nestedKeys = new ArrayList<>();
            nestedKeys.add(objectKeys.get(1));
            nestedKeys.add(objectKeys.get(2));

            Buffer buffer = client.create(objectKeys.get(0), size, createParam);
            Assert.assertEquals(buffer.getSize(), size);
            buffer.wLatch();
            ByteBuffer data0 = TestUtils.getRandomDirectBuffer(size);
            buffer.memoryCopy(data0);
            buffer.publish(nestedKeys);
            buffer.unWLatch();

            List<Buffer> buffers = client.get(objectKeys, 0);
            Assert.assertEquals(data0, buffers.get(0).immutableData());
            Assert.assertEquals(data1, buffers.get(1).immutableData());
            Assert.assertEquals(data2, buffers.get(2).immutableData());

            Assert.assertEquals(client.gDecreaseRef(nestedKeys).size(), 0);
            buffers = client.get(objectKeys, 0);
            Assert.assertEquals(data0, buffers.get(0).immutableData());
            Assert.assertEquals(data1, buffers.get(1).immutableData());
            Assert.assertEquals(data2, buffers.get(2).immutableData());

            List<String> parentId = new ArrayList<>();
            parentId.add(objectKeys.get(0));
            Assert.assertEquals(client.gDecreaseRef(parentId).size(), 0);

            client.get(objectKeys, 0);
        } finally {
            client.close();
        }
    }

    @Test
    public void testMemoryCopyDirectBufferZeroToLimit() {
        ObjectClient client = getObjectClient(1).get(0);
        try {
            List<String> objectKeys = TestUtils.getUUIDList(1);
            CreateParam createParam = initCreateParam();
            int size = 10;
            Buffer buffer = client.create(objectKeys.get(0), size, createParam);
            try {
                ByteBuffer data = TestUtils.getRandomDirectBuffer(size);
                // The limit positions of the ByteBuffer are modified.
                int limit = 5;
                data.limit(limit);
                // java.nio.HeapByteBuffer[pos=0 lim=5 cap=10]
                buffer.memoryCopy(data);
                buffer.seal();
                List<Buffer> getBuffers = client.get(objectKeys, 5);
                try {
                    ByteBuffer getBuffer = getBuffers.get(0).mutableData();
                    Assert.assertEquals(getBuffer.capacity(), size);
                    getBuffer.position(0);
                    getBuffer.limit(limit);
                    data.position(0);
                    data.limit(limit);
                    Assert.assertEquals(getBuffer.slice(), data.slice());

                    getBuffer.position(limit);
                    getBuffer.limit(size);
                    data.position(limit);
                    data.limit(size);
                    Assert.assertNotEquals(getBuffer.slice(), data.slice());
                } finally {
                    for (Buffer buf2 : getBuffers) {
                        buf2.close();
                    }
                }
            } finally {
                buffer.close();
            }
        } finally {
            client.close();
        }
    }

    @Test
    public void testMemoryCopyHeapBufferZeroToLimit() {
        ObjectClient client = getObjectClient(1).get(0);
        List<String> objectKeys = TestUtils.getUUIDList(1);
        CreateParam createParam = initCreateParam();
        int sizeByte = 10;
        try {
            Buffer buffer = client.create(objectKeys.get(0), sizeByte, createParam);
            try {
                ByteBuffer data = TestUtils.getRandomHeapBuffer(sizeByte);
                // The pos and limit positions of the ByteBuffer are modified.
                int position = 3;
                int limit = 6;
                data.position(position);
                data.limit(limit);
                // java.nio.HeapByteBuffer[pos=3 lim=6 cap=10]
                buffer.memoryCopy(data);
                buffer.seal();

                List<Buffer> getBuffers = client.get(objectKeys, 5);
                try {
                    ByteBuffer getBuffer = getBuffers.get(0).mutableData();
                    getBuffer.position(0);
                    getBuffer.limit(limit);

                    data.position(0);
                    data.limit(limit);
                    Assert.assertEquals(getBuffer.capacity(), sizeByte);
                    Assert.assertEquals(getBuffer.slice(), data.slice());
                } finally {
                    for (Buffer buf3 : getBuffers) {
                        buf3.close();
                    }
                }
            } finally {
                buffer.close();
            }
        } finally {
            client.close();
        }
    }

    @Test
    public void testPutHeapBufferZeroToLimit() {
        ObjectClient client = getObjectClient(1).get(0);
        List<String> objectKeys = TestUtils.getUUIDList(1);
        CreateParam createParam = initCreateParam();
        int size = 10;
        try {
            Buffer buffer = client.create(objectKeys.get(0), size, createParam);
            try {
                ByteBuffer data = TestUtils.getRandomHeapBuffer(size);
                // The pos and limit positions of the ByteBuffer are modified.
                int position = 3;
                int limit = 6;
                data.position(position);
                data.limit(limit);
                // java.nio.HeapByteBuffer[pos=3 lim=6 cap=10]
                client.put(objectKeys.get(0), data, createParam, new ArrayList<>());

                List<Buffer> getBuffers = client.get(objectKeys, 5);
                try {
                    ByteBuffer getBuffer = getBuffers.get(0).mutableData();
                    Assert.assertEquals(buffer.getSize(), size);
                    Assert.assertEquals(getBuffer.capacity(), limit);

                    data.position(0);
                    Assert.assertEquals(data.slice(), getBuffer);
                } finally {
                    for (Buffer buf4 : getBuffers) {
                        buf4.close();
                    }
                }
            } finally {
                buffer.close();
            }
        } finally {
            client.close();
        }
    }

    @Test
    public void testPutDirectBufferZeroToLimit() {
        ObjectClient client = getObjectClient(1).get(0);
        List<String> objectKeys = TestUtils.getUUIDList(1);
        CreateParam createParam = initCreateParam();
        try {
            int size = 10;
            ByteBuffer data = TestUtils.getRandomDirectBuffer(size);
            // The limit positions of the ByteBuffer are modified.
            int limit = 5;
            data.limit(limit);

            client.put(objectKeys.get(0), data, createParam);
            List<Buffer> buffers = client.get(objectKeys, 5);

            ByteBuffer getBuffer = buffers.get(0).immutableData();
            Assert.assertEquals(getBuffer.capacity(), limit);
            Assert.assertEquals(getBuffer, data.slice());
        } finally {
            client.close();
        }
    }
}