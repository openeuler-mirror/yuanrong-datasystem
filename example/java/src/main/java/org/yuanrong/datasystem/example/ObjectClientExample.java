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
import org.yuanrong.datasystem.ConsistencyType;
import org.yuanrong.datasystem.object.Buffer;
import org.yuanrong.datasystem.object.CreateParam;
import org.yuanrong.datasystem.object.ObjectClient;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

/**
 * The example of using ObjectClient.
 *
 * @since 2022-08-17
 */
public class ObjectClientExample {
    public static void main(String[] args) {
        Logger log = Logger.getLogger(ObjectClientExample.class.toString());
        log.info("---- Start the object java api demo");
        String ip = args[0];
        int port = Integer.parseInt(args[1]);
        // initialize ConnectOptions
        ConnectOptions connectOpts = new ConnectOptions(ip, port);
        // get ObjectClient
        ObjectClient client = new ObjectClient(connectOpts);
        try {
            // initialize CreateParam
            CreateParam createParam = new CreateParam();
            createParam.setConsistencyType(ConsistencyType.PRAM);
            // client to create buffer
            List<String> objectIds = Arrays.asList("ObjectId");
            int size = 5 * 1024 * 1024;
            client.gIncreaseRef(objectIds);
            Buffer buffer = client.create(objectIds.get(0), size, createParam);
            // buffer to memoryCopy and seal
            String data = "abc";
            byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
            ByteBuffer buffer1 = ByteBuffer.allocateDirect(bytes.length);
            buffer1.put(bytes);
            buffer.wLatch();
            buffer.memoryCopy(buffer1);
            buffer.seal();
            buffer.unWLatch();
            buffer.close();
            // client to get buffer
            List<Buffer> buffers = client.get(objectIds, 5);
            ByteBuffer bufferGet = buffers.get(0).immutableData();
            byte[] bytes1 = new byte[bufferGet.remaining()];
            bufferGet.get(bytes1);
            String data1 = new String(bytes1, StandardCharsets.UTF_8);
            // client to delete object
            client.gDecreaseRef(objectIds);
            for (Buffer buf : buffers) {
                buf.close();
            }
        } finally {
            client.close();
        }
    }
}