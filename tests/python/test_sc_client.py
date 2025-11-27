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
Stream cache test case.
"""
from __future__ import absolute_import
import json
import logging
import os
import random
import time
import unittest

from datasystem.object_client import ObjectClient
from datasystem.stream_client import StreamClient, SubconfigType


def wait_proc(proc):
    """wait_proc"""
    proc.wait()
    stdout, stderr = proc.communicate()
    logging.info(stdout)
    logging.info(stderr)


class TestScClientMethods(unittest.TestCase):
    """
    Features: Stream cache client python interface test.
    """

    @classmethod
    def setUpClass(cls):
        logging.info("********************sc_client test start*********************")
        time.sleep(3)
        root_dir = os.path.dirname(os.path.abspath('..'))
        worker_env_path = os.path.join(root_dir, "output", "datasystem", "service", "worker_config.json")
        with open(worker_env_path, "r") as f:
            config = json.load(f)

        work_address = config.get("worker_address", {})
        TestScClientMethods.work_addr = work_address.get("value")
        logging.info("TestScClientMethods.work_addr: %s", TestScClientMethods.work_addr)

    @staticmethod
    def multi_producer_and_consumer(client, element_datas):
        """mutil producer and consumer test case"""
        length = len(element_datas)
        stream_name = "stream_name_multi_" + str(0)
        sub_name = "sub_name_multi_" + str(0)
        producer_tmp = client.create_producer(stream_name, 50)

        consumer_tmp = client.subscribe(stream_name, sub_name, SubconfigType.STREAM.value)

        before_send = time.perf_counter()
        for i in range(length):
            # send bytes directly.
            producer_tmp.send(element_datas[i])

        before_flsuh = time.perf_counter()
        before_recv = time.perf_counter()
        _ = consumer_tmp.receive(length, 0)
        after_recv = time.perf_counter()

        time_lst = [before_send, before_flsuh, before_recv, after_recv]
        annotation_lst = ['send', 'flush', 'recv']
        for i in range(len(time_lst) - 1):
            logging.info('%s: %s ms', annotation_lst[i], 1000 * (time_lst[i + 1] - time_lst[i]))
        producer_tmp.close()
        consumer_tmp.close()
        return True

    @staticmethod
    def test_client_delete_stream_success():
        """delete stream test"""
        stream_name = "stream_ds"
        ip = TestScClientMethods.work_addr.split(":")
        client_ds = StreamClient(ip[0], int(ip[1]))
        client_ds.init()
        producer_ds = client_ds.create_producer(stream_name)

        consumer_ds = client_ds.subscribe(stream_name, "sub_name_d", SubconfigType.STREAM.value)

        producer_ds.close()
        consumer_ds.close()
        client_ds.delete_stream(stream_name)

    @staticmethod
    def test_stream_set_default_size_success():
        """stream set default size test"""
        ip = TestScClientMethods.work_addr.split(":")
        client_stream_sds = StreamClient(ip[0], int(ip[1]))
        client_stream_sds.init()
        stream_name = "stream_set_default_size"

        client_stream_sds.create_producer(stream_name, 2)

    @staticmethod
    def test_multi_producer_set_success():
        """mutil producer set test"""
        ip = TestScClientMethods.work_addr.split(":")
        client_stream_psd = StreamClient(ip[0], int(ip[1]))
        client_stream_psd.init()
        stream_name = "stream_multi_producer"

        base_size = 1024 * 4
        for i in range(1, 6):
            client_stream_psd.create_producer(stream_name, 1 + i // 3, base_size)
        for _ in range(1, 6):
            client_stream_psd.create_producer(stream_name, 5, base_size)

    @staticmethod
    def test_client_send_receive_data_success():
        """client send and receive data test"""
        arr = b'10101010'
        stream_name = "stream_sr"
        ip = TestScClientMethods.work_addr.split(":")
        client_srd = StreamClient(ip[0], int(ip[1]))
        client_srd.init()
        producer_srd = client_srd.create_producer(stream_name)

        consumer_srd = client_srd.subscribe(stream_name, "sub_name_s", SubconfigType.STREAM.value)
        producer_srd.send(arr)

        element_list = consumer_srd.receive(1, 0)

        data_element = memoryview(element_list[-1])
        logging.info("element type: %s", type(element_list))
        logging.info("element size: %s", len(data_element) * data_element.itemsize)
        logging.info("element context: %s", data_element.tobytes())
        consumer_srd.ack(element_list[-1].get_id())

        producer_srd.close()
        consumer_srd.close()

    @staticmethod
    def test_client_send_with_blocking_support_receive_without_expected_num_data():
        """client send and receive data test"""
        arr = b'10101010'
        stream_name = "stream_sr"
        ip = TestScClientMethods.work_addr.split(":")
        client_srd = StreamClient(ip[0], int(ip[1]))
        client_srd.init()
        producer_srd = client_srd.create_producer(stream_name)

        consumer_srd = client_srd.subscribe(stream_name, "sub_name_s", SubconfigType.STREAM.value)
        producer_srd.send(arr, 1000)

        element_list = consumer_srd.receive_any(0)

        data_element = memoryview(element_list[-1])
        logging.info("element type: %s", type(element_list))
        logging.info("element size: %s", len(data_element) * data_element.itemsize)
        logging.info("element context: %s", data_element.tobytes())
        consumer_srd.ack(element_list[-1].get_id())

        producer_srd.close()
        consumer_srd.close()

    @staticmethod
    def test_stream_set_pagesize_sucess():
        """stream set pagesize test"""
        ip = TestScClientMethods.work_addr.split(":")
        client_stream_spd = StreamClient(ip[0], int(ip[1]))
        client_stream_spd.init()
        stream_name = "stream_set_pagesize"

        client_stream_spd.create_producer(stream_name, 1, 1024 * 4)

    @staticmethod
    def test_stream_client_work_success():
        """stream case test"""
        ip = TestScClientMethods.work_addr.split(":")
        client_stream_os = StreamClient(ip[0], int(ip[1]))
        client_stream_os.init()

        stream_name = "stream_object"
        producer_os = client_stream_os.create_producer(stream_name)

        consumer_os = client_stream_os.subscribe(stream_name, "sub_object_stream", SubconfigType.STREAM.value)
        arr = b'0001'
        producer_os.send(arr)
        element_list = consumer_os.receive(1, 0)

        consumer_os.ack(element_list[-1].get_id())

        producer_os.close()
        consumer_os.close()

    def random_str(self, slen=10):
        """random string"""
        seed = "1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
        sa = []
        for _ in range(slen):
            sa.append(random.choice(seed))
        return ''.join(sa)

    def generate_elements(self, element_num, element_size):
        """generate elements"""
        elements = []
        seed = "1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
        tmp = []
        for _ in range(element_num):
            for _ in range(element_size):
                tmp.append(random.choice(seed))
            rus = ''.join(tmp)
            elements.append(bytes(rus, encoding='utf8'))
            tmp = []
        return elements

    def test_multi_send_recv_success(self):
        """mutil producer and consumer test"""
        element_num = 4000
        element_size = 1000
        element_datas = self.generate_elements(element_num, element_size)
        ip = TestScClientMethods.work_addr.split(":")
        client_sr = StreamClient(ip[0], int(ip[1]))
        client_sr.init()
        self.multi_producer_and_consumer(client_sr, element_datas)

    def test_query_stream_topo_success(self):
        """query stream topo test"""
        stream_name1 = "query_topo_stream1"
        ip = TestScClientMethods.work_addr.split(":")
        client_qst1 = StreamClient(ip[0], int(ip[1]))
        client_qst1.init()
        node1_producer_qst1 = client_qst1.create_producer(stream_name1)

        node1_consumer_qst = client_qst1.subscribe(stream_name1, "sub_name1", SubconfigType.STREAM.value)

        global_consumer_num = client_qst1.query_global_consumer_num(stream_name1)
        self.assertEqual(global_consumer_num, 1)

        global_producer_num = client_qst1.query_global_producer_num(stream_name1)
        self.assertEqual(global_producer_num, 1)

        ip = TestScClientMethods.work_addr.split(":")
        client_qst2 = StreamClient(ip[0], int(ip[1]))
        client_qst2.init()

        node2_producer_qst = client_qst2.create_producer(stream_name1)

        node2_consumer_qst = client_qst2.subscribe(stream_name1, "sub_name2", SubconfigType.STREAM.value)
        global_consumer_num = client_qst2.query_global_consumer_num(stream_name1)
        self.assertEqual(global_consumer_num, 2)

        global_producer_num = client_qst1.query_global_producer_num(stream_name1)
        self.assertEqual(global_producer_num, 1)

        node1_producer_qst1.close()
        node1_consumer_qst.close()
        node2_producer_qst.close()
        node2_consumer_qst.close()

    def test_object_client_work_success(self):
        """object case test"""
        host, port = TestScClientMethods.work_addr.split(":")
        port = int(port)
        client_os = ObjectClient(host, port)
        client_os.init()
        object_key = self.random_str(10)
        value = bytearray(self.random_str(50), encoding='utf8')
        buffer = client_os.create(object_key, len(value))
        buffer.wlatch()
        buffer.memory_copy(value)
        buffer.seal()
        buffer.unwlatch()

        object_key2 = self.random_str(10)
        value2 = bytearray(self.random_str(100), encoding='utf8')
        buffer2 = client_os.create(object_key2, len(value2))
        buffer2.wlatch()
        buffer2.memory_copy(value2)
        buffer2.seal()
        buffer2.unwlatch()

        buffer_list = client_os.get([object_key, object_key2], 5)

        self.assertEqual(value, buffer_list[0].immutable_data())
        self.assertEqual(value2, buffer_list[1].immutable_data())

    def test_stream_auto_send_success(self):
        """stream auto send case test"""
        ip = TestScClientMethods.work_addr.split(":")
        client_stream_asd = StreamClient(ip[0], int(ip[1]))
        client_stream_asd.init()

        stream_name = "stream_send_auto"
        producer_asd = client_stream_asd.create_producer(stream_name)
        consumer_asd = client_stream_asd.subscribe(stream_name, "sub_object_stream", SubconfigType.STREAM.value)

        arr = b'10101010'
        producer_asd.send(arr)

        element_list = consumer_asd.receive(1, 10)

        consumer_asd.ack(element_list[-1].get_id())
        self.assertEqual(memoryview(element_list[-1]).tobytes(), arr)

        producer_asd.close()
        consumer_asd.close()

    def test_stream_auto_send_without_delay_success(self):
        """stream auto send without delay case test"""
        ip = TestScClientMethods.work_addr.split(":")
        client_stream_swd = StreamClient(ip[0], int(ip[1]))
        client_stream_swd.init()
        stream_name = "stream_send_without_delay"
        producer_swd = client_stream_swd.create_producer(stream_name, 0)
        consumer_swd = client_stream_swd.subscribe(stream_name, "sub_object_stream", SubconfigType.STREAM.value)

        arr = b'10101010'
        producer_swd.send(arr)

        element_list = consumer_swd.receive(1, 10)

        consumer_swd.ack(element_list[-1].get_id())
        self.assertEqual(memoryview(element_list[-1]).tobytes(), arr)

        producer_swd.close()
        consumer_swd.close()

    def test_stream_sametime_send_success(self):
        """stream send sametime test"""
        ip = TestScClientMethods.work_addr.split(":")
        client_stream_ssd = StreamClient(ip[0], int(ip[1]))
        client_stream_ssd.init()
        stream_name = "stream_sametime_send"
        producer_ssd = client_stream_ssd.create_producer(stream_name)
        consumer_ssd = client_stream_ssd.subscribe(stream_name, "sub_object_stream", SubconfigType.STREAM.value)

        arr = b'10101010'
        arr2 = b'01010101'
        producer_ssd.send(arr)

        time.sleep(0.005)
        producer_ssd.send(arr2)

        element_list = consumer_ssd.receive(2, 20)

        for _, element in enumerate(element_list):
            consumer_ssd.ack(element.get_id())

        self.assertEqual(memoryview(element_list[0]).tobytes(), arr)
        self.assertEqual(memoryview(element_list[-1]).tobytes(), arr2)

        producer_ssd.close()
        consumer_ssd.close()

    def test_stream_continuous_send_success(self):
        """stream send continue test"""
        ip = TestScClientMethods.work_addr.split(":")
        client_stream_csd = StreamClient(ip[0], int(ip[1]))
        client_stream_csd.init()
        stream_name = "stream_continuous_send"
        producer_csd = client_stream_csd.create_producer(stream_name)
        consumer_csd = client_stream_csd.subscribe(stream_name, "sub_object_stream", SubconfigType.STREAM.value)

        arr = b'1010101010101010'
        for i in range(1, 11):
            tmparr = arr[0:i]
            producer_csd.send(tmparr)

        element_list = consumer_csd.receive(15, 20)

        for i, element in enumerate(element_list):
            consumer_csd.ack(element.get_id())
            self.assertEqual(memoryview(element).tobytes(), arr[0:(i + 1)])

        producer_csd.close()
        consumer_csd.close()

    def test_stream_producer_without_consumer(self):
        """stream send test without consumer"""
        ip = TestScClientMethods.work_addr.split(":")
        for i in range(5):
            client = StreamClient(ip[0], int(ip[1]))
            client.init()
            stream_name = 'test_dfx_streamcache_node_scale_004'
            producer = client.create_producer(stream_name, delay_flush_time_ms=5, page_size_byte=1024 * 1024,
                                            max_stream_size_byte=10 * 1024 * 1024, auto_cleanup=False)
            for j in range(100000):
                data = ('test' + str(i) + str(j)).encode()
                producer.send(data)
            producer.close()
            client.delete_stream(stream_name)

    def test_stream_sametime_flush_success(self):
        """stream flush sametime test"""
        ip = TestScClientMethods.work_addr.split(":")
        client_stream_sfd = StreamClient(ip[0], int(ip[1]))
        client_stream_sfd.init()
        stream_name = "stream_sametime_flush"
        producer_sfd = client_stream_sfd.create_producer(stream_name)
        consumer_sfd = client_stream_sfd.subscribe(stream_name, "sub_object_stream", SubconfigType.STREAM.value)

        arr = b'10101010'
        producer_sfd.send(arr)
        time.sleep(0.005)
        element_list = consumer_sfd.receive(1, 100)
        self.assertEqual(len(element_list), 1)
        consumer_sfd.ack(element_list[-1].get_id())
        self.assertEqual(memoryview(element_list[-1]).tobytes(), arr)

        producer_sfd.close()
        consumer_sfd.close()

    def test_long_max_limit(self):
        """Test case in which the value of except_num exceeds the upper limit"""
        ip = TestScClientMethods.work_addr.split(":")
        client = StreamClient(ip[0], int(ip[1]))
        client.init()
        stream_name = "test_long_max_limit"
        consumer = client.subscribe(stream_name, "sub_object_stream", SubconfigType.STREAM.value)

        int32_max_size = int("0x7FFFFFFF", 16)

        # test except_num
        except_num = int32_max_size + 1
        self.assertRaises(RuntimeError, consumer.receive, except_num, 10)

        # test timeout_ms
        timeout_ms = int32_max_size + 1
        self.assertRaises(RuntimeError, consumer.receive, 10, timeout_ms)
        consumer.close()

    def test_param_max_limit(self):
        """Test case in which the value size exceeds the upper limit"""
        ip = TestScClientMethods.work_addr.split(":")
        client = StreamClient(ip[0], int(ip[1]))
        client.init()
        stream_name = "test_send_max_limit"
        delay_flush_time_ms = int("0x7FFFFFFFFFFFFFFF", 16) + 1
        self.assertRaises(RuntimeError, client.create_producer, stream_name, delay_flush_time_ms)

    def test_stream_producer_retain_for_num_consumer(self):
        """stream test retain for consumer"""
        ip = TestScClientMethods.work_addr.split(":")
        client = StreamClient(ip[0], int(ip[1]))
        client.init()
        stream_name = 'test_retain'
        producer = client.create_producer(stream_name, 5, 1024 * 1024, 10 * 1024 * 1024, False, 1)

        arr = b'10101010'
        producer.send(arr)

        consumer = client.subscribe(stream_name, "test_retain_sub1", SubconfigType.STREAM.value)
        element_list = consumer.receive(1, 0)
        self.assertEqual(len(element_list), 1)
        consumer.close()

        # We only retain data for 1 consumer. Second consumer should not get any
        consumer2 = client.subscribe(stream_name, "test_retain_sub2", SubconfigType.STREAM.value)
        element_list2 = consumer2.receive_any(0)
        self.assertEqual(len(element_list2), 0)
        consumer2.close()

        producer.close()
        client.delete_stream(stream_name)

    def test_create_producer_reserve_size(self):
        """test create producer reserve size"""
        # This testcase intends to test that invalid reserve size will lead to CreateProducer failure.
        ip = TestScClientMethods.work_addr.split(":")
        client = StreamClient(ip[0], int(ip[1]))
        client.init()
        stream_name = 'test_reserve_size'
        delay_flush_time_ms = 5
        page_size_byte = 8 * 1024
        max_stream_size_byte = 64 * 1024 * 1024
        auto_cleanup = False
        retain_for_num_consumers = 0
        encrypt_stream = False

        # Valid reserve size should be less than or equal to max stream size.
        reserve_size = max_stream_size_byte + page_size_byte
        self.assertRaises(RuntimeError, client.create_producer, stream_name, delay_flush_time_ms,
            page_size_byte, max_stream_size_byte, auto_cleanup, retain_for_num_consumers, encrypt_stream,
            reserve_size)

        # Valid reserve size should be a multiple of page size.
        reserve_size = 12 * 1024
        self.assertRaises(RuntimeError, client.create_producer, stream_name, delay_flush_time_ms,
            page_size_byte, max_stream_size_byte, auto_cleanup, retain_for_num_consumers, encrypt_stream,
            reserve_size)

        # 0 is an acceptable input for reserve size, the default reserve size will then be the page size.
        reserve_size = 0
        producer = client.create_producer(stream_name, delay_flush_time_ms, page_size_byte,
            max_stream_size_byte, auto_cleanup, retain_for_num_consumers, encrypt_stream, reserve_size)

        global_producer_num = client.query_global_producer_num(stream_name)
        self.assertEqual(global_producer_num, 1)

        producer.close()
        client.delete_stream(stream_name)
