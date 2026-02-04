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

package org.yuanrong.datasystem.kv;

import org.yuanrong.datasystem.Cluster;
import org.yuanrong.datasystem.ConnectOptions;
import org.yuanrong.datasystem.Context;
import org.yuanrong.datasystem.DataSystemException;
import org.yuanrong.datasystem.TestUtils;
import org.yuanrong.datasystem.WriteMode;

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
 * @since 2022-08-25
 */
public class KVClientTest {
    private static Cluster cluster;
    private static int workerNums = 2;
    private static int redisNums = 0;
    private static String systemAccessKey = "QTWAOYTTINDUT2QVKYUC";
    private static String systemSecretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";

    @BeforeClass
    public static void beforeClass() throws IOException {
        cluster = new Cluster(workerNums, redisNums);
        cluster.init(KVClientTest.class.getSimpleName());
        String paras = " -authorization_enable=true -system_access_key=" + systemAccessKey + " -system_secret_key="
                + systemSecretKey; // + " -iam_kit=token_oauth" + " -tenant_token_url=https://auth.server.com";
        cluster.setWorkerGflagParams(paras);
        cluster.startAll();
    }

    @AfterClass
    public static void afterClass() {
        if (cluster != null) {
            cluster.stopAll();
        }
    }

    /**
     * Get Cross-Node KVClient list.
     *
     * @param number The KVClient number.(number <= workerNums)
     * @return The KVClient list.
     */
    private List<KVClient> getKVClient(int number) {
        return getKVClient(number, systemAccessKey, systemSecretKey, "", "");
    }

    /**
     * Get Cross-Node KVClient list.
     *
     * @param number   The KVClient number.(number <= workerNums)
     * @param ak       The access key for AK/SK authorize.
     * @param sk       The secret key for AK/SK authorize.
     * @param tenantId The tenant id.
     * @param token    The token.
     * @return The KVClient list.
     */
    private List<KVClient> getKVClient(int number, String ak, String sk, String tenantId, String token) {
        if (number > workerNums) {
            String message = "Invalid Argument, starting " + workerNums + " workers, but expecting to create "
                    + number + " cross-node Object Clients.";
            throw new DataSystemException(message);
        }
        List<KVClient> clients = new ArrayList<>(number);
        for (int i = 0; i < number; i++) {
            String workerAddr = cluster.getWorkerAddr(i);
            String[] temp = workerAddr.split(":");
            String ip = temp[0];
            int port = Integer.parseInt(temp[1]);
            ConnectOptions connectOpts = new ConnectOptions(ip, port);
            if (ak != null && !ak.isEmpty()) {
                connectOpts.setAkSkAuth(ak, sk.getBytes(StandardCharsets.UTF_8), tenantId);
            }
            KVClient client = new KVClient(connectOpts);
            clients.add(client);
        }
        return clients;
    }

    @Test
    public void testConnect() {
        KVClient client = getKVClient(workerNums).get(0);
        client.close();
    }

    // environment variable is null, throw Exception.
    @Test(expected = DataSystemException.class)
    public void testStateCacheClientInitByEnv() {
        KVClient client = new KVClient();
        client.close();
    }

    @Test
    public void testSetHeapByteBuffer() {
        KVClient client = getKVClient(1).get(0);
        try {
            String key = TestUtils.getUUID();
            ByteBuffer value = TestUtils.getRandomHeapBuffer(10);
            client.set(key, value);
            client.del(key);
        } finally {
            client.close();
        }
    }

    @Test
    public void testSetDirectByteBuffer() {
        KVClient client = getKVClient(1).get(0);
        try {
            String key = TestUtils.getUUID();
            ByteBuffer value = TestUtils.getRandomDirectBuffer(10);
            client.set(key, value);
        } finally {
            client.close();
        }
    }

    @Test
    public void testSetWriteMode() {
        KVClient client = getKVClient(1).get(0);
        try {
            List<String> keys = TestUtils.getUUIDList(4);
            ByteBuffer value = TestUtils.getRandomDirectBuffer(10);
            SetParam param = new SetParam();
            param.setWriteMode(WriteMode.NONE_L2_CACHE);
            client.set(keys.get(0), value, param);
            ByteBuffer valueGet1 = client.get(keys.get(0));
            Assert.assertEquals(value, valueGet1);
            client.del(keys.get(0));

            param.setWriteMode(WriteMode.NONE_L2_CACHE_EVICT);
            client.set(keys.get(3), value, param);
            ByteBuffer valueGet4 = client.get(keys.get(3));
            Assert.assertEquals(value, valueGet4);
            client.del(keys.get(3));
        } finally {
            client.close();
        }
    }

    @Test
    public void testSetAndGetKey() {
        KVClient client = getKVClient(1).get(0);
        try {
            // non-shared memory
            String key = TestUtils.getUUID();
            ByteBuffer value = TestUtils.getRandomDirectBuffer(10);
            client.set(key, value);
            ByteBuffer valueGet = client.get(key);
            Assert.assertEquals(value, valueGet);
            client.del(key);
            // shared memory
            String key2 = TestUtils.getUUID();
            ByteBuffer value2 = TestUtils.getRandomDirectBuffer(500 * 1024);
            client.set(key2, value2);
            ByteBuffer valueGet2 = client.get(key2);
            Assert.assertEquals(value2, valueGet2);
            client.del(key2);
        } finally {
            client.close();
        }
    }

    @Test
    public void testSetAndGetAndDelKeys() {
        KVClient client = getKVClient(1).get(0);
        try {
            int num = 3;
            // non-shared memory
            List<String> keys = TestUtils.getUUIDList(num);
            List<ByteBuffer> values = TestUtils.getRandomDirectBufferList(10, num);
            for (int i = 0; i < num; i++) {
                client.set(keys.get(i), values.get(i));
            }
            List<ByteBuffer> valuesGet = client.get(keys);
            Assert.assertEquals(values, valuesGet);

            // shared memory
            List<String> keys2 = TestUtils.getUUIDList(num);
            List<ByteBuffer> values2 = TestUtils.getRandomDirectBufferList(500 * 1024, num);
            for (int i = 0; i < num; i++) {
                client.set(keys2.get(i), values2.get(i));
            }
            List<ByteBuffer> valuesGet2 = client.get(keys2);
            Assert.assertEquals(values2, valuesGet2);

            client.del(keys);
            // expected DataSystemException
            Assert.assertThrows(DataSystemException.class, () -> client.get(keys));
        } finally {
            client.close();
        }
    }

    @Test(expected = DataSystemException.class)
    public void testSetWithTTL() throws InterruptedException {
        List<KVClient> clients = getKVClient(2);
        try {
            String key = TestUtils.getUUID();
            ByteBuffer value = TestUtils.getRandomDirectBuffer(10);
            SetParam param = new SetParam();
            param.setWriteMode(WriteMode.WRITE_THROUGH_L2_CACHE);
            param.setTtlSecond(1L);

            KVClient client1 = clients.get(0);
            KVClient client2 = clients.get(1);

            client1.set(key, value, param);
            ByteBuffer valueGet1 = client2.get(key);
            Assert.assertEquals(value, valueGet1);
            Thread.sleep(1500);
            client1.get(key);
        } finally {
            for (KVClient client : clients) {
                client.close();
            }
        }
    }

    @Test(expected = DataSystemException.class)
    public void testDelSingleKey() {
        Context.setTraceId("testDelSingleKey");
        KVClient client = getKVClient(1).get(0);
        try {
            String key = TestUtils.getUUID();
            ByteBuffer value = TestUtils.getRandomDirectBuffer(10);
            client.set(key, value);
            ByteBuffer valueGet = client.get(key);
            Assert.assertEquals(value, valueGet);
            client.del(key);

            // expected DataSystemException
            client.get(key, 5);
        } finally {
            client.close();
        }
    }

    @Test
    public void testSetDirectBufferZeroToLimit() {
        KVClient client = getKVClient(1).get(0);
        try {
            int size = 10;
            List<String> keys = TestUtils.getUUIDList(1);
            ByteBuffer data = TestUtils.getRandomDirectBuffer(size);
            // The limit positions of the ByteBuffer are modified.
            int limit = 5;
            data.limit(limit);

            SetParam param = new SetParam();
            param.setWriteMode(WriteMode.NONE_L2_CACHE);
            client.set(keys.get(0), data, param);
            ByteBuffer getBuffer = client.get(keys.get(0));

            Assert.assertEquals(getBuffer.capacity(), limit);
            Assert.assertEquals(getBuffer, data.slice());
        } finally {
            client.close();
        }
    }

    @Test
    public void testSetHeapBufferZeroToLimit() {
        KVClient client = getKVClient(1).get(0);
        int size = 10;
        try {
            List<String> keys = TestUtils.getUUIDList(1);
            ByteBuffer data = TestUtils.getRandomHeapBuffer(size);
            // The pos and limit positions of the ByteBuffer are modified.
            int position = 3;
            int limit = 6;
            data.position(position);
            data.limit(limit);
            // java.nio.HeapByteBuffer[pos=3 lim=6 cap=10]
            SetParam param = new SetParam();
            param.setWriteMode(WriteMode.NONE_L2_CACHE);
            client.set(keys.get(0), data, param);

            ByteBuffer getBuffer = client.get(keys.get(0));

            Assert.assertEquals(getBuffer.capacity(), limit);
            data.position(0);
            Assert.assertEquals(data.slice(), getBuffer);
        } finally {
            client.close();
        }
    }

    @Test(expected = DataSystemException.class)
    public void testAuthAkSkFailed() {
        getKVClient(1, systemAccessKey, "fakeSk", "", "");
    }

    @Test(expected = DataSystemException.class)
    public void testAuthTokenFailed() {
        getKVClient(1, "", "", "", "fakeToken");
    }

    @Test
    public void testAuthSuccess() {
        KVClient client1 = null;
        KVClient client2 = null;
        try {
            client1 = getKVClient(1, systemAccessKey, systemSecretKey, "akskTenant1", "").get(0);
            client2 = getKVClient(1, systemAccessKey, systemSecretKey, "akskTenant2", "").get(0);
            ByteBuffer value1 = TestUtils.getRandomDirectBuffer(1024);
            ByteBuffer value2 = TestUtils.getRandomDirectBuffer(2048);
            client1.set("key", value1);
            client2.set("key", value2);

            ByteBuffer getVal1 = client1.get("key");
            ByteBuffer getVal2 = client2.get("key");
            Assert.assertEquals(value1, getVal1);
            Assert.assertEquals(value2, getVal2);

            client1.del("key");
            client2.del("key");
        } finally {
            if (client1 != null) {
                client1.close();
            }
            if (client2 != null) {
                client2.close();
            }
        }
    }

    @Test
    public void testGenerateKey() {
        KVClient client = getKVClient(1).get(0);
        try {
            String key = client.generateKey();
            int len = 73;
            Assert.assertEquals(key.length(), len);
            Assert.assertEquals(key.contains(";"), true);
        } finally {
            client.close();
        }
    }

    @Test(expected = DataSystemException.class)
    public void testInvalidTraceId() {
        Context.setTraceId("a|b");
    }
}