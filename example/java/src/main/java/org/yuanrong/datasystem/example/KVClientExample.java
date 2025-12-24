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

import org.yuanrong.datasystem.ConnectOptions;
import org.yuanrong.datasystem.kv.KVClient;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

/**
 * The example of using KVClient.
 *
 * @since 2022-09-01
 */
public class KVClientExample {
    private static final Logger log = Logger.getLogger(KVClientExample.class.toString());

    /**
     * Print ByteBuffer.
     *
     * @param buffer The ByteBuffer.
     */
    public static void printByteBuffer(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        String value = new String(bytes, StandardCharsets.UTF_8);
        String message = "The value is : " + value;
        log.info(message);
    }

    public static void main(String[] args) {
        log.info("---- Start the KV java api demo");
        String ip = args[0];
        int port = Integer.parseInt(args[1]);
        ConnectOptions connectOpts = new ConnectOptions(ip, port);
        KVClient client = new KVClient(connectOpts);
        try {
            List<String> keys = Arrays.asList("key1", "key2", "key3");
            String tmp = "abc";
            byte[] bytes = tmp.getBytes(StandardCharsets.UTF_8);
            ByteBuffer value = ByteBuffer.allocateDirect(bytes.length);
            value.put(bytes);
            // client set and get
            for (int i = 0; i < 3; i++) {
                client.set(keys.get(i), value);
            }
            ByteBuffer getValue = client.get(keys.get(0));
            List<ByteBuffer> getValues = client.get(keys);
            client.del(keys);
            String message = "The size of value list is : " + getValues.size();
            log.info(message);
            printByteBuffer(getValue);
        } finally {
            client.close();
        }
    }
}