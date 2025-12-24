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

package org.yuanrong.datasystem;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.UUID;

/**
 * Test utils.
 *
 * @since 2022-08-25
 */
public class TestUtils {
    private static Random random = new Random();

    /**
     * Generate random UUID32 list.
     *
     * @param number The number of UUID32.
     * @return 32-bit uuid list.
     */
    public static List<String> getUUIDList(int number) {
        if (number <= 0) {
            return Collections.emptyList();
        }
        List<String> list = new ArrayList<>(number);
        int num = number;
        while (num > 0) {
            list.add(getUUID());
            num--;
        }
        return list;
    }

    /**
     * Generate random UUID32.
     *
     * @return 32-bit uuid.
     */
    public static String getUUID() {
        return UUID.randomUUID().toString().replace("-", "").toLowerCase(Locale.ENGLISH);
    }

    /**
     * Generate random HeapByteBuffer.
     *
     * @param length The size of ByteBuffer.
     * @return ByteBuffer.
     */
    public static ByteBuffer getRandomHeapBuffer(int length) {
        byte[] bytes = new byte[length];
        random.nextBytes(bytes);
        return ByteBuffer.wrap(bytes);
    }

    /**
     * Generate random DirectByteBuffer.
     *
     * @param length The size of ByteBuffer.
     * @return ByteBuffer.
     */
    public static ByteBuffer getRandomDirectBuffer(int length) {
        byte[] bytes = new byte[length];
        random.nextBytes(bytes);
        ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length);
        buffer.put(bytes);
        buffer.flip();
        return buffer;
    }

    /**
     * Generate random DirectByteBuffer list.
     *
     * @param length The size of ByteBuffer.
     * @param number The number of ByteBuffer.
     * @return ByteBuffer list.
     */
    public static List<ByteBuffer> getRandomDirectBufferList(int length, int number) {
        if (number <= 0) {
            return Collections.emptyList();
        }
        List<ByteBuffer> dirBufferList = new ArrayList<>(number);
        int num = number;
        for (int i = 0; i < num; i++) {
            dirBufferList.add(getRandomDirectBuffer(length));
        }
        return dirBufferList;
    }
}