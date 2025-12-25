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

// Testing stream package
package stream

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"
	"unsafe"

	"clients/common"
)

// A helper function so that the testcase can get the worker host and port from the environment
func getWorkerForTest() (common.ConnectArguments, common.Status) {
	workerAddr := os.Getenv("WORKER_ADDR")
	workerPortStr := os.Getenv("WORKER_PORT")
	var connectParameters = common.ConnectArguments{Host: "", Port: -1}
	if len(workerAddr) == 0 || len(workerPortStr) == 0 {
		return connectParameters, common.CreateStatus(common.InvalidParam,
			"WORKER_ADDR or WORKER_PORT env variables need to be set!")
	}
	workerPort, err := strconv.Atoi(workerPortStr)
	if err != nil {
		return connectParameters, common.CreateStatus(common.InvalidParam,
			"WORKER_PORT env variable was set but had invalid value!")
	}

	connectParameters.Host = workerAddr
	connectParameters.Port = workerPort
	return connectParameters, common.CreateStatus(common.Ok, "")
}

// A helper function so that the testcase can create stream cache client.
func CreateClientForTest() (*StreamClient, common.Status) {
	var connectParameters common.ConnectArguments
	var s common.Status
	const UnknowError int = 10 // same as C++ K_UNKNOWN_ERROR=10
	if connectParameters, s = getWorkerForTest(); s.IsError() {
		return nil, common.CreateStatus(UnknowError,
			"Get the worker host and port failed!")
	}
	workerAddr := connectParameters.Host
	workerPort := connectParameters.Port
	fmt.Printf("Testcase using worker address %s on port %d\n", workerAddr, workerPort)

	theClient := CreateClient(connectParameters)
	s = theClient.Init(false)
	if s.IsError() {
		theClient.DestroyClient()
		return nil, common.CreateStatus(UnknowError,
			"Init function connects the client to the worker failed!")
	}
	return &theClient, common.CreateStatus(common.Ok, "")
}

func get2DSlice(rows, cols uint) [][]uint8 {
	var rowLimit uint = 1000
	var colLimit uint = 1024 * 1024
	if rows > rowLimit {
		return nil
	}
	if cols > colLimit {
		return nil
	}
	limit := 256
	rand.Seed(time.Now().UnixNano())
	slice := make([][]uint8, rows, rows)
	for i := range slice {
		slice[i] = make([]uint8, cols, cols)
		for j := range slice[i] {
			slice[i][j] = uint8(rand.Intn(limit))
		}
	}
	return slice
}

func sendElements(t *testing.T, producer *StreamProducer, slices [][]uint8) {
	var id uint64 = 10000
	var s common.Status
	for i := range slices {
		var element Element
		element.Ptr = &slices[i][0]
		element.Size = uint64(len(slices[i]))
		element.Id = id
		s = producer.Send(element)
		if s.IsError() {
			t.Fatalf(s.ToString())
		}
	}
	// s = producer.Flush()
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
}

func receiveAndCheckElements(t *testing.T, consumer *StreamConsumer, slices [][]uint8) {
	var timeoutMs uint32 = 10
	var eles []Element
	var s common.Status
	eles, s = consumer.Receive(timeoutMs)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	if len(eles) != len(slices) {
		t.Fatalf(s.ToString())
	}
	for i := range eles {
		if eles[i].Size != uint64(len(slices[i])) {
			t.Fatalf(s.ToString())
		}
		ele := unsafe.Slice((*uint8)(unsafe.Pointer(eles[i].Ptr)), eles[i].Size)
		for j := 0; j < int(eles[i].Size); j++ {
			if ele[j] != slices[i][j] {
				t.Fatalf(s.ToString())
			}
		}
	}
	if len(eles) == 0 {
		return
	}
	s = consumer.Ack(eles[len(eles)-1].Id)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
}

func receiveAndCheckElementsExpect(t *testing.T, consumer *StreamConsumer, slices [][]uint8) {
	var timeoutMs uint32 = 10
	var eles []Element
	var s common.Status
	eles, s = consumer.ReceiveExpectNum(uint32(len(slices)), timeoutMs)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	if len(eles) != len(slices) {
		t.Fatalf(s.ToString())
	}
	for i := range eles {
		if eles[i].Size != uint64(len(slices[i])) {
			t.Fatalf(s.ToString())
		}
		ele := unsafe.Slice((*uint8)(unsafe.Pointer(eles[i].Ptr)), eles[i].Size)
		for j := 0; j < int(eles[i].Size); j++ {
			if ele[j] != slices[i][j] {
				t.Fatalf(s.ToString())
			}
		}
	}
	if len(eles) == 0 {
		return
	}
	s = consumer.Ack(eles[len(eles)-1].Id)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
}

func TestSubscribe(t *testing.T) {
	var theClient *StreamClient
	var s common.Status
	var consumer *StreamConsumer

	if theClient, s = CreateClientForTest(); theClient == nil {
		t.Fatalf(s.ToString())
	}
	defer theClient.DestroyClient()

	var streamName string = "TestSubscribeStr"
	var subName string = "TestSubscribe"

	var subConfig SubscriptionConfig
	subConfig.SubName = subName
	subConfig.SubType = Stream

	consumer, s = theClient.Subscribe(streamName, subConfig, false)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	_ = consumer
}

func TestReceiveNothing(t *testing.T) {
	var theClient *StreamClient
	var s common.Status
	var consumer *StreamConsumer

	if theClient, s = CreateClientForTest(); theClient == nil {
		t.Fatalf(s.ToString())
	}
	defer theClient.DestroyClient()

	var streamName string = "TestReceiveNothingStr"
	var subName string = "TestReceiveNothing"
	var subConfig SubscriptionConfig
	subConfig.SubName = subName
	subConfig.SubType = Stream
	var cacheCapacity uint32 = 32768

	consumer, s = theClient.Subscribe(streamName, subConfig, false, cacheCapacity)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}

	var expectNum uint32 = 1
	var timeoutMs uint32 = 10
	var eles []Element
	eles, s = consumer.ReceiveExpectNum(expectNum, timeoutMs)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	if len(eles) != 0 {
		t.Fatalf(s.ToString())
	}
}

func TestReceiveSomething1(t *testing.T) {
	var theClient *StreamClient
	var s common.Status
	var consumer *StreamConsumer
	var producer *StreamProducer

	if theClient, s = CreateClientForTest(); theClient == nil {
		t.Fatalf(s.ToString())
	}
	defer theClient.DestroyClient()

	var streamName string = "TestReceiveSomething1Str"
	var subName string = "TestReceiveSomething1"
	var subConfig SubscriptionConfig
	subConfig.SubName = subName
	subConfig.SubType = Stream

	consumer, s = theClient.Subscribe(streamName, subConfig, false)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}

	var delayFlushTime int64 = 5
	var pageSize int64 = 4096
	var maxStreamSize uint64 = 1024 * 1024 * 1024
	producer, s = theClient.CreateProducer(streamName, delayFlushTime, pageSize,
									maxStreamSize, false)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	// get elements
	var numEle uint = 2
	var sz uint = 5
	slices := get2DSlice(numEle, sz)
	// send, receive
	sendElements(t, producer, slices)
	receiveAndCheckElements(t, consumer, slices)
}

func TestReceiveSomething2(t *testing.T) {
	fmt.Printf("TestReceiveSomething2 \n")
	var theClient *StreamClient
	var s common.Status
	var consumer *StreamConsumer
	var producer *StreamProducer

	if theClient, s = CreateClientForTest(); theClient == nil {
		t.Fatalf(s.ToString())
	}
	defer theClient.DestroyClient()

	var streamName string = "TestReceiveSomething2Str"
	var subName string = "TestReceiveSomething2"

	var subConfig SubscriptionConfig
	subConfig.SubName = subName
	subConfig.SubType = Stream

	consumer, s = theClient.Subscribe(streamName, subConfig, false)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}

	var delayFlushTime int64 = 5
	var pageSize int64 = 1024 * 1024
	var maxStreamSize uint64 = 1024 * 1024 * 1024
	producer, s = theClient.CreateProducer(streamName, delayFlushTime, pageSize,
								maxStreamSize, false)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	numIter := 100
	for i := 0; i < numIter; i++ {
		// get elements
		var numEle uint = 1
		var sz uint = 5
		slices := get2DSlice(numEle, sz)
		// send, receive
		sendElements(t, producer, slices)
		receiveAndCheckElements(t, consumer, slices)
	}
	s = consumer.Close()
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	s = producer.Close()
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
}

func TestReceiveSomething3(t *testing.T) {
	var theClient *StreamClient
	var s common.Status
	var consumer *StreamConsumer
	var producer *StreamProducer

	if theClient, s = CreateClientForTest(); theClient == nil {
		t.Fatalf(s.ToString())
	}
	defer theClient.DestroyClient()

	var streamName string = "TestReceiveSomething3Str"
	var subName string = "TestReceiveSomething3"
	var subConfig SubscriptionConfig
	subConfig.SubName = subName
	subConfig.SubType = Stream

	consumer, s = theClient.Subscribe(streamName, subConfig, false)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}

	var delayFlushTime int64 = 5
	var pageSize int64 = 1024 * 1024
	var maxStreamSize uint64 = 1024 * 1024 * 1024
	producer, s = theClient.CreateProducer(streamName, delayFlushTime, pageSize,
									maxStreamSize, false)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	// get elements
	var numEle uint = 100
	var sz uint = 5
	slices := get2DSlice(numEle, sz)
	// send, receive
	sendElements(t, producer, slices)
	receiveAndCheckElementsExpect(t, consumer, slices)
	fmt.Printf("TestReceiveSomething3 success\n")
}

func TestQueryConsumerAndProducer(t *testing.T) {
	fmt.Printf("TestQueryConsumerAndProducer \n")
	var theClient *StreamClient
	var s common.Status
	var consumer *StreamConsumer
	var producer *StreamProducer

	if theClient, s = CreateClientForTest(); theClient == nil {
		t.Fatalf(s.ToString())
	}
	defer theClient.DestroyClient()

	var streamName string = "TestQueryConsumerAndProducerStr"
	var subName string = "TestQueryConsumerAndProducer"

	var subConfig SubscriptionConfig
	subConfig.SubName = subName
	subConfig.SubType = Stream

	consumer, s = theClient.Subscribe(streamName, subConfig, false)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}

	var delayFlushTime int64 = 5
	var pageSize int64 = 1024 * 1024
	var maxStreamSize uint64 = 1024 * 1024 * 1024
	producer, s = theClient.CreateProducer(streamName, delayFlushTime, pageSize,
									maxStreamSize, false)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	// get elements
	var numEle uint = 2
	var sz uint = 5
	slices := get2DSlice(numEle, sz)
	// send, receive
	sendElements(t, producer, slices)
	receiveAndCheckElements(t, consumer, slices)
	var numProducer uint64
	var numConsumer uint64
	numProducer, s = theClient.QueryGlobalProducersNum(streamName)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	numConsumer, s = theClient.QueryGlobalProducersNum(streamName)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}

	if numConsumer != 1 {
		t.Fatalf("consumer num is not 1")
	}
	if numProducer != 1 {
		t.Fatalf("consumer num is not 1")
	}
	fmt.Printf("TestQueryConsumerAndProducer success\n")
}

func TestCloseAndDeleteStreams(t *testing.T) {
	fmt.Printf("TestCloseAndDeleteStreams \n")
	var theClient *StreamClient
	var s common.Status
	var consumer *StreamConsumer
	var producer *StreamProducer

	if theClient, s = CreateClientForTest(); theClient == nil {
		t.Fatalf(s.ToString())
	}
	defer theClient.DestroyClient()

	var streamName string = "TestCloseAndDeleteStreams"
	var subName string = "CloseAndDeleteStreams"

	var subConfig SubscriptionConfig
	subConfig.SubName = subName
	subConfig.SubType = Stream

	consumer, s = theClient.Subscribe(streamName, subConfig, false)

	if s.IsError() {
		t.Fatalf(s.ToString())
	}

	var delayFlushTime int64 = 5
	var pageSize int64 = 1024 * 1024
	var maxStreamSize uint64 = 1024 * 1024 * 1024
	producer, s = theClient.CreateProducer(streamName, delayFlushTime, pageSize,
									maxStreamSize, false)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}

	s = consumer.Close()
	if s.IsError() {
		t.Fatalf(s.ToString())
	}

	s = producer.Close()
	if s.IsError() {
		t.Fatalf(s.ToString())
	}

	s = theClient.DeleteStream(streamName)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	fmt.Printf("TestCloseAndDeleteStreams success\n")
}

func TestGetStatisticsMessage(t *testing.T) {
	fmt.Printf("TestGetStatisticsMessage \n")
	var theClient *StreamClient
	var s common.Status
	var consumer *StreamConsumer
	var producer *StreamProducer

	if theClient, s = CreateClientForTest(); theClient == nil {
		t.Fatalf(s.ToString())
	}
	defer theClient.DestroyClient()

	var streamName string = "TestGetStatisticsMessage"
	var subName string = "GetStatisticsMessage"

	var subConfig SubscriptionConfig
	subConfig.SubName = subName
	subConfig.SubType = Stream

	consumer, s = theClient.Subscribe(streamName, subConfig, false)

	if s.IsError() {
		t.Fatalf(s.ToString())
	}

	var delayFlushTime int64 = 5
	var pageSize int64 = 1024 * 1024
	var maxStreamSize uint64 = 1024 * 1024 * 1024
	producer, s = theClient.CreateProducer(streamName, delayFlushTime, pageSize,
									maxStreamSize, false)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}

	var numEle uint = 2
	var sz uint = 5
	slices := get2DSlice(numEle, sz)
	// send, receive
	sendElements(t, producer, slices)
	receiveAndCheckElements(t, consumer, slices)

	var totalElements uint64
	var notProcessedElements uint64
	totalElements, notProcessedElements, s = consumer.GetStatisticsMessage()
	if s.IsError() {
		t.Fatalf(s.ToString())
	}

	if totalElements != uint64(numEle) {
		t.Fatalf("totalElements != numelements")
	}
	if notProcessedElements != 0 {
		t.Fatalf("notProcessedElements != 0 after ack")
	}

	fmt.Printf("ResetSingleStreams success\n")
}

func TestRetainForNumConsumers(t *testing.T) {
	var theClient *StreamClient
	var s common.Status
	var producer *StreamProducer
	var consumer1, consumer2 *StreamConsumer
	
	if theClient, s = CreateClientForTest(); theClient == nil {
		t.Fatalf(s.ToString())
	}
	defer theClient.DestroyClient()

	var streamName string = "TestRetainData"
	var delayFlushTime int64 = 5
	var pageSize int64 = 1024 * 1024
	var maxStreamSize uint64 = 1024 * 1024 * 1024
	prodConfig := ProducerConfig{
        RetainForNumConsumers: 1,
    }
	producer, s = theClient.CreateProducerWithConfig(streamName, delayFlushTime, pageSize,
									maxStreamSize, false, prodConfig)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}

	// get elements, send
	var numEle uint = 100
	var sz uint = 5
	slices := get2DSlice(numEle, sz)
	sendElements(t, producer, slices)

	// first late consumer
	var subConfig SubscriptionConfig
	var subName string = "Test1LateConsumer"
	subConfig.SubType = Stream
	subConfig.SubName = subName
	consumer1, s = theClient.Subscribe(streamName, subConfig, false)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}

	// receive, first late consumer should receive
	receiveAndCheckElementsExpect(t, consumer1, slices)

	// second late consumer
	subName = "Test2LateConsumer"
	subConfig.SubName = subName
	consumer2, s = theClient.Subscribe(streamName, subConfig, false)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}

	// receive, second late consumer should not receive
	var expectNum uint32 = 1
	var timeoutMs uint32 = 10
	var eles []Element
	eles, s = consumer2.ReceiveExpectNum(expectNum, timeoutMs)
	if (s.IsError() || len(eles) != 0) {
		t.Fatalf(s.ToString())
	}
	fmt.Printf("TestRetainForNumConsumers success\n")
}

func TestCreateConsumerReserveSize(t *testing.T) {
	// This testcase intends to test that invalid reserve size will lead to CreateProducer failure.
	var theClient *StreamClient
	var s common.Status
	var producer *StreamProducer

	if theClient, s = CreateClientForTest(); theClient == nil {
		t.Fatalf(s.ToString())
	}
	defer theClient.DestroyClient()

	var tempVar uint64= 12
	var streamName string = "TestCreateConsumerReserveSize"
	var delayFlushTime int64 = 5
	var pageSize int64 = 1024 * 1024
	var maxStreamSize uint64  = 1024 * 1024 * 1024
	var notMultipleOfPageSize uint64 = tempVar * 1024
	var invalidReserveSize uint64 = maxStreamSize + uint64(pageSize)
	prodConfig2 := ProducerConfig{
		RetainForNumConsumers: 1,
        ReserveSize: 0,
    }

	// Valid reserve size should be less than or equal to max stream size.
	prodConfig2.ReserveSize = invalidReserveSize
	producer, s = theClient.CreateProducerWithConfig(streamName, delayFlushTime, pageSize,
									maxStreamSize, false, prodConfig2)
	if s.IsError() {
		t.Logf(s.ToString())
	}

	// Valid reserve size should be a multiple of page size.
	prodConfig2.ReserveSize = notMultipleOfPageSize
	producer, s = theClient.CreateProducerWithConfig(streamName, delayFlushTime, pageSize,
									maxStreamSize, false, prodConfig2)
	if s.IsError() {
		t.Logf(s.ToString())
	}
	// 0 is an acceptable input for reserve size, the default reserve size will then be the page size.
	prodConfig2.ReserveSize = 0
	producer, s = theClient.CreateProducerWithConfig(streamName, delayFlushTime, pageSize,
									maxStreamSize, false, prodConfig2)
	if s.IsError() {
		t.Errorf(s.ToString())
	}

	// get elements
	var numEle uint = 2
	var sz uint = 5
	slices := get2DSlice(numEle, sz)
	// send, receive
	sendElements(t, producer, slices)

	fmt.Printf("TestCreateConsumerReserveSize success\n")
}