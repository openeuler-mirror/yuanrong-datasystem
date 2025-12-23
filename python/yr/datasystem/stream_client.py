# Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Stream cache client python interface.
"""

from enum import Enum
import yr.datasystem.lib.libds_client_py as ds
from yr.datasystem.util import Validator as validator


class SubconfigType(Enum):
    """The type of stream"""
    STREAM = 0
    ROUND_ROBIN = 1
    KEY_PARTITIONS = 2


class StreamClient:
    """the client of stream"""

    def __init__(self,
                 host: str,
                 port: int,
                 client_public_key: str = "",
                 client_private_key: str = "",
                 server_public_key: str = "",
                 access_key="",
                 secret_key="",
                 token: str = '',
                 tenant_id="",
                 enable_exclusive_connection=False):
        """ Constructor of the StreamClient class

        Args:
            host(str): The worker address host.
            port(str): The worker address port.
            client_public_key(str): The client's public key, for curve authentication.
            client_private_key(str): The client's private key, for curve authentication.
            server_public_key(str): The worker server's public key, for curve authentication.
            access_key(str): The access key used by AK/SK authorize.
            secret_key(str): The secret key for AK/SK authorize.
            token(str): A string used for authentication.
            tenant_id(str): The tenant ID.
            enable_exclusive_connection(bool): Experimental feature: improves IPC performance between client and
            datasystem_worker.
        """

        if isinstance(token, str):
            token = str.encode(token)
        if isinstance(client_private_key, str):
            client_private_key = str.encode(client_private_key)
        if isinstance(secret_key, str):
            secret_key = str.encode(secret_key)
        self._client = ds.StreamClient(host, port, client_public_key, client_private_key, server_public_key, access_key,
                                       secret_key, token, tenant_id, enable_exclusive_connection)

    def init(self):
        """ Init a stream client to connect to a worker.

        Raises:
            RuntimeError: Raise a runtime error if the client fails to connect to the worker.
        """
        init_status = self._client.init()
        if init_status.is_error():
            raise RuntimeError(init_status.to_string())

    def create_producer(self,
                        stream_name,
                        delay_flush_time_ms=5,
                        page_size_byte=1024 * 1024,
                        max_stream_size_byte=1024 * 1024 * 1024,
                        auto_cleanup=False,
                        retain_for_num_consumers=0,
                        encrypt_stream=False,
                        reserve_size=0):
        """ Create one Producer to send element.

        Args:
            stream_name: The name of the stream.
            delay_flush_time_ms: The time used in automatic flush after send and default is 5ms.
            page_size_byte: The size used in allocate page and default is 1MB.
                must be a multiple of 4KB.
            max_stream_size_byte: The max stream size in worker and default is 1GB.
                must between greater then 64KB and less than the shared memory size.
            auto_cleanup: Should auto delete when the last producer/consumer exit.
            retain_for_num_consumers: The number of consumers to retain data for, default to 0.
            encrypt_stream: Enable stream data encryption between workers, default to false.
            reserve_size: default reserve size to page size, must be a multiple of page size.
        Return:
            outProducer: The output Producer that user can use it to send element.

        Raise:
            TypeError: Raise a type error if the input parameter is invalid.
            RutimeError: Raise a runtime error if creating a producer fails.
        """
        if not isinstance(stream_name, str):
            raise TypeError("The input of stream_name should be string.")
        if not isinstance(delay_flush_time_ms, int):
            raise TypeError("The input of delay_flush_time_ms should be int.")
        validator.check_param_range("delay_flush_time_ms", delay_flush_time_ms, 0, validator.INT32_MAX_SIZE)
        if not isinstance(page_size_byte, int):
            raise TypeError("The input of page_size_byte should be int.")
        status, out_producer = self._client.CreateProducer(stream_name, delay_flush_time_ms, page_size_byte,
                                                           max_stream_size_byte, auto_cleanup, retain_for_num_consumers,
                                                           encrypt_stream, reserve_size)
        if status.is_error():
            raise RuntimeError(status.to_string())
        return Producer(out_producer)

    def subscribe(self, stream_name, sub_name, subscription_type):
        """ Subscribe a new consumer onto master request

        Args:
            stream_name: The name of the stream.
            sub_name: The name of subscription
            subscription_type: The type of subscription.

        Return:
            outConsumer: The output Consumer that user can use it to receive element.

        Raise:
            TypeError: Raise a type error if the input parameter is invalid.
            RuntimeError: Raise a runtime error if subscribing a new consumer fails.
        """
        if not isinstance(stream_name, str):
            raise TypeError("The input of stream_name should be string.")
        if not isinstance(subscription_type, int):
            raise TypeError("The input of type should be int.")
        status, out_consumer = self._client.Subscribe(stream_name, sub_name, subscription_type)
        if status.is_error():
            raise RuntimeError(status.to_string())
        return Consumer(out_consumer)

    def delete_stream(self, stream_name):
        """ Delete one stream

        Args:
            stream_name: The name of the stream.

        Raise:
            TypeError: Raise a type error if the input parameter is invalid.
            RutimeError: Raise a runtime error if deleting one stream fails.
        """
        if not isinstance(stream_name, str):
            raise TypeError("The input of stream_name should be string.")
        status = self._client.DeleteStream(stream_name)
        if status.is_error():
            raise RuntimeError(status.to_string())

    def query_global_producer_num(self, stream_name):
        """ Query number of producer in global worker node

        Args:
            stream_name: The name of the target stream.

        Returns:
            global_producer_num: Query result.

        Raise:
            TypeError: Raise a type error if the input parameter is invalid.
            RutimeError: Raise a runtime error if querying global producer number fails.
        """
        if not isinstance(stream_name, str):
            raise TypeError("The input of stream_name should be string.")
        status, global_producer_num = self._client.QueryGlobalProducersNum(stream_name)
        if status.is_error():
            raise RuntimeError(status.to_string())
        return global_producer_num

    def query_global_consumer_num(self, stream_name):
        """ Query number of consumer in global worker node

        Args:
            stream_name: The name of the target stream.

        Returns:
            global_consumer_num: Query result.

        Raise:
            TypeError: Raise a type error if the input parameter is invalid.
            RutimeError: Raise a runtime error if querying global consumer number fails.
        """
        if not isinstance(stream_name, str):
            raise TypeError("The input of stream_name should be string.")
        status, global_consumer_num = self._client.QueryGlobalConsumersNum(stream_name)
        if status.is_error():
            raise RuntimeError(status.to_string())
        return global_consumer_num


class Producer:
    """the producer of stream in client"""

    def __init__(self, producer):
        if not isinstance(producer, ds.Producer):
            raise TypeError("The input of parament should be Producer.")
        self._producer = producer

    def send(self, element_bytes, timeout_ms=None):
        """ Produce send one element of the stream each time

        Args:
            element: The element that to be written.
            timeout_ms: The amount of time in milliseconds to wait for the send to complete in the range of
                [0, INT32_MAX]. A value of 0 means that it will immediately return the error reason without waiting if
                the send cannot be completed right away. A value greater than 0 makes this a possible blocking call
                where it will wait for the operation to complete if needed. If the wait time exceeds the value then
                the function will stop waiting and return the error reason.

        Raise:
            TypeError: Raise a type error if the input parameter is invalid.
            RutimeError: Raise a runtime error if sending one element fails.
        """
        if not isinstance(element_bytes, memoryview) and not isinstance(element_bytes, bytes) and not isinstance(
                element_bytes, bytearray):
            raise TypeError("The input of parament should be memoryview or bytes or bytearray.")
        if timeout_ms is None:
            status = self._producer.Send(element_bytes)
            if status.is_error():
                raise RuntimeError(status.to_string())
        else:
            if not isinstance(timeout_ms, int):
                raise TypeError("The input of timeout_ms should be int.")
            validator.check_param_range("timeout_ms", timeout_ms, 0, validator.INT32_MAX_SIZE)
            status = self._producer.Send(element_bytes, timeout_ms)
            if status.is_error():
                raise RuntimeError(status.to_string())

    def close(self):
        """ Close a producer, register a publisher to a stream.

        Raise:
            RuntimeError: Raise a runtime error if closing a producer fails.
        """
        status = self._producer.Close()
        if status.is_error():
            raise RuntimeError(status.to_string())


class Consumer:
    """the consumer of stream in client"""

    def __init__(self, consumer):
        if not isinstance(consumer, ds.Consumer):
            raise TypeError("The input of parament should be Consumer.")
        self._consumer = consumer

    def receive(self, expect_num, timeout_ms):
        """ Receive an expected number of elements.

        Args:
            expect_num: The number of elements to be read.
            timeout_ms: The timeout in milliseconds to wait or until number of expected elements has been received.

        Return:
            values: element has been received

        Raise:
            TypeError: Raise a type error if the input parameter is invalid.
            RutimeError: Raise a runtime error if receiving elements meta falis.
        """
        if not isinstance(expect_num, int):
            raise TypeError("The input of expect_num should be int.")
        validator.check_param_range("expect_num", expect_num, 1, validator.INT32_MAX_SIZE)
        if not isinstance(timeout_ms, int):
            raise TypeError("The input of timeout_ms should be int.")
        validator.check_param_range("timeout_ms", timeout_ms, 0, validator.INT32_MAX_SIZE)
        status, element = self._consumer.Receive(expect_num, timeout_ms)
        if status.is_error():
            raise RuntimeError(status.to_string())
        return element

    def receive_any(self, timeout_ms):
        """ Receive any number of elements that is available.

        Args:
            timeout_ms: The timeout in milliseconds to wait or until any number of elements has been received.

        Return:
            values: element has been received

        Raise:
            TypeError: Raise a type error if the input parameter is invalid.
            RutimeError: Raise a runtime error if receiving elements meta falis.
        """
        if not isinstance(timeout_ms, int):
            raise TypeError("The input of timeout_ms should be int.")
        validator.check_param_range("timeout_ms", timeout_ms, 0, validator.INT32_MAX_SIZE)
        status, element = self._consumer.ReceiveAny(timeout_ms)
        if status.is_error():
            raise RuntimeError(status.to_string())
        return element

    def ack(self, element_id):
        """ Acknowledge elements that had been read by this consumer.

        Args:
            element_id: The element id that to be acknowledged.

        Raise:
            TypeError: Raise a type error if the input parameter is invalid.
            RutimeError: Raise a runtime error if acknowledging elements falis.
        """
        if not isinstance(element_id, int):
            raise TypeError("The input of element_id should be int.")
        status = self._consumer.Ack(element_id)
        if status.is_error():
            raise RuntimeError(status.to_string())

    def close(self):
        """ Close the consumer, after close it will not allow Receive and Ack Elements.

        Raise:
            RuntimeError: Raise a runtime error if closing the consumer falis.
        """
        status = self._consumer.Close()
        if status.is_error():
            raise RuntimeError(status.to_string())
