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

import java.math.BigInteger;
import java.util.List;

/**
 * the consumer interface of stream in client.
 *
 * @since 2022-05-07
 */
public interface Consumer {
    /**
     * The container of statistics message.
     * Provide getters and setters of totalElements and notProcessedElements.
     */
    public class StatisticsMessage {
        private BigInteger totalElements = null;
        private BigInteger notProcessedElements = null;

        public BigInteger getTotalElements() {
            return this.totalElements;
        }

        public BigInteger getNotProcessedElements() {
            return this.notProcessedElements;
        }

        public void setTotalElements(BigInteger uint64) {
            this.totalElements = uint64;
        }

        public void setNotProcessedElements(BigInteger uint64) {
            this.notProcessedElements = uint64;
        }
    }

    /**
     * Receive elements meta, where worker handles big and small element lookup and parsing
     *
     * @param expectNum The number of elements to be read.
     * @param timeoutMs The timeout millisecond of elements to be Received.
     * @return The element has been received.
     */
    List<Element> receive(long expectNum, int timeoutMs);

    /**
     * Receive elements meta, where worker handles big and small element lookup and parsing
     *
     * @param timeoutMs The timeout millisecond of elements to be Received.
     * @return The element has been received.
     */
    List<Element> receive(int timeoutMs);


    /**
     * Acknowledge elements that had been read by this consumer
     *
     * @param elementId The element id that to be acknowledged.
     */
    void ack(long elementId);

    /**
     * Close the consumer, after close it will not allow Receive and Ack Elements.
     */
    void close();

    /**
     * Get statistics message.
     *
     * @param statistics An empty instance of StatisticsMessage to hold returned metrics.
     */
    void getStatisticsMessage(StatisticsMessage statistics);
}