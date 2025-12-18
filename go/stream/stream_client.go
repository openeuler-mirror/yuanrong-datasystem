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

// Package stream is the package of the stream client
package stream

/*
#cgo CFLAGS: -I../include/
#cgo LDFLAGS: -L../lib -ldatasystem_c
#include <stdlib.h>
#include <string.h>
#include "datasystem/c_api/stream_client_c_wrapper.h"
#include "datasystem/c_api/utilC.h"
*/
import "C"

import (
	"fmt"
	"strconv"
	"sync"
	"unsafe"

	"clients/common"
)

// StreamClient is the main handle object that allows you to make calls into the client API's.
// Create it with the call to CreateClient(), and then all subsequent api calls will flow through this handle.
type StreamClient struct {
	client C.StreamClient_p
	// lock Protect the StreamClient
	mutex *sync.RWMutex
}

// StreamConsumer is the main handle object that allows you to make calls associated with a consumer.
// Create it with the call to Subscribe(), and then all subsequent api calls will flow through this handle.
type StreamConsumer struct {
	consumer C.Consumer_p
	// lock Protect the StreamConsumer
	mutex *sync.RWMutex
}

// SubscriptionType is the new type defined.
// Use const Stream, RoundRobin, KeyPartitions and Unknown together to simulate the enumeration type of C++.
type SubscriptionType int

// The constant Stream, RoundRobin, KeyPartitions and Unknown are of the SubscriptionType type.
// Use it when calling Subscribe()
const (
	Stream SubscriptionType = iota
	RoundRobin
	KeyPartitions
	Unknown
)

// Element holds the pointer to an element. Size and element ID are given together.
type Element struct {
	Ptr  *uint8
	Size uint64
	Id   uint64
}

// SubscriptionConfig Consisting of subscription name and type. Optionally,
// Should the consumer receive notification about the fault of a producer. Default is false. the cache
// prefetch low water mark can be enabled (non-zero value will turn prefetching on).
type SubscriptionConfig struct {
	SubName             string
	SubType             SubscriptionType
	CachePrefetchLWM    uint16
}

// StreamProducer for test
type StreamProducer struct {
	producer C.Producer_p
	mutex    *sync.RWMutex
}

// ProducerConfig used to to create producer with optional params
type ProducerConfig struct {
    RetainForNumConsumers	uint64
	EncryptStream 			bool
	ReserveSize				uint64
}

func clearAndFree(pointer *C.char, size C.ulong) {
	bytes := unsafe.Slice(pointer, size)
	for i := C.ulong(0); i < size; i++ {
		bytes[i] = 0
	}
	C.free(unsafe.Pointer(pointer))
}

func checkNullClientPtr(ptr *StreamClient) common.Status {
	if ptr == nil {
		return common.CreateStatus(common.UnexpectedError, "The ptr of StreamClient is nil")
	}
	if ptr.client == nil {
		return common.CreateStatus(common.UnexpectedError, "The StreamClient.client is nil")
	}
	if ptr.mutex == nil {
		return common.CreateStatus(common.UnexpectedError, "The StreamClient.mutex is nil")
	}
	return common.CreateStatus(common.Ok, "")
}

func checkNullConsumerPtr(ptr *StreamConsumer) common.Status {
	if ptr == nil {
		return common.CreateStatus(common.UnexpectedError, "The ptr of StreamConsumer is nil")
	}
	if ptr.consumer == nil {
		return common.CreateStatus(common.UnexpectedError, "The StreamConsumer.consumer is nil")
	}
	if ptr.mutex == nil {
		return common.CreateStatus(common.UnexpectedError, "The StreamConsumer.mutex is nil")
	}
	return common.CreateStatus(common.Ok, "")
}

func checkNullProducerPtr(ptr *StreamProducer) common.Status {
	if ptr == nil {
		return common.CreateStatus(common.UnexpectedError, "The ptr of streamProducer is nil")
	}
	if ptr.producer == nil {
		return common.CreateStatus(common.UnexpectedError, "The streamProducer.consumer is nil")
	}
	if ptr.mutex == nil {
		return common.CreateStatus(common.UnexpectedError, "The streamProducer.mutex is nil")
	}
	return common.CreateStatus(common.Ok, "")
}

// CreateClient function creates the StreamClient and associates it with a given worker host and port numbers.
// After creation, a connection to the worker is NOT established yet.  To connect, see the Connect() call.
// To disconnect and/or release the resources from this client, use the Free() call.
func CreateClient(param common.ConnectArguments) StreamClient {
	var ret StreamClient
	cWorkerHost := C.CString(param.Host)
	defer C.free(unsafe.Pointer(cWorkerHost))
	cWorkerPort := C.int(param.Port)
	cWorkerTimeout := C.int(param.TimeoutMs)
	// ClientPublicKey
	cClientPublicKey := C.CString(param.ClientPublicKey)
	cClientPublicKeyLen := C.ulong(len(param.ClientPublicKey))
	defer C.free(unsafe.Pointer(cClientPublicKey))
	// ClientPrivateKey
	cClientPrivateKeyLen := C.ulong(len(param.ClientPrivateKey))
	cClientPrivateKey := (*C.char)(C.CBytes(param.ClientPrivateKey))
	defer clearAndFree(cClientPrivateKey, cClientPrivateKeyLen)
	// ServerPublicKey
	cServerPublicKey := C.CString(param.ServerPublicKey)
	cServerPublicKeyLen := C.ulong(len(param.ServerPublicKey))
	defer C.free(unsafe.Pointer(cServerPublicKey))
	// AccessKey
	cAccessKey := C.CString(param.AccessKey)
	cAccessKeyLen := C.ulong(len(param.AccessKey))
	defer C.free(unsafe.Pointer(cAccessKey))
	// SecretKey
	cSecretKeyLen := C.ulong(len(param.SecretKey))
	cSecretKey := (*C.char)(C.CBytes(param.SecretKey))
	defer clearAndFree(cSecretKey, cSecretKeyLen)
	// TenantID
	cTenantID := C.CString(param.TenantID)
	cTenantIDLen := C.ulong(len(param.TenantID))
	defer C.free(unsafe.Pointer(cTenantID))
	// EnableCrossNodeConnection
	cEnableCrossNodeConnection := C.CString(strconv.FormatBool(param.EnableCrossNodeConnection))
	defer C.free(unsafe.Pointer(cEnableCrossNodeConnection))

	ret.client = C.StreamCreateClient(cWorkerHost, cWorkerPort, cWorkerTimeout, 
		cClientPublicKey, cClientPublicKeyLen, cClientPrivateKey, cClientPrivateKeyLen, cServerPublicKey, 
		cServerPublicKeyLen, cAccessKey, cAccessKeyLen, cSecretKey, cSecretKeyLen, cTenantID, cTenantIDLen, 
		cEnableCrossNodeConnection)
	ret.mutex = new(sync.RWMutex)
	return ret
}

// Init function connects the client to the worker.
func (t *StreamClient) Init(reportWorkerLost bool) common.Status {
	rc := checkNullClientPtr(t)
	if int(rc.Code) != common.Ok {
		return rc
	}
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	statusC := C.StreamConnectWorker(t.client, C.bool(reportWorkerLost))
	if int(statusC.code) != common.Ok {
		return common.CreateStatus(int(statusC.code),
			fmt.Sprintf("Stream cache client failed to connect. msg: %s", C.GoString(&statusC.errMsg[0])))
	}
	return common.CreateStatus(common.Ok, "")
}

// DestroyClient function frees all resources of the client and terminates a connection if it was connected
func (t *StreamClient) DestroyClient() {
	rc := checkNullClientPtr(t)
	if int(rc.Code) == common.Ok {
		t.mutex.Lock()
		defer t.mutex.Unlock()
		C.StreamFreeClient(t.client)
		t.client = nil
	}
}

// Subscribe subscribes a stream through a client and gets a consumer.
func (t *StreamClient) Subscribe(streamName string, config SubscriptionConfig, autoAck bool,
	cacheCapacity ...uint32) (*StreamConsumer, common.Status) {
	rc := checkNullClientPtr(t)
	if int(rc.Code) != common.Ok {
		return nil, rc
	}
	cStrName := C.CString(streamName)
	defer C.free(unsafe.Pointer(cStrName))
	strNameLen := C.size_t(len(streamName))
	cSubName := C.CString(config.SubName)
	defer C.free(unsafe.Pointer(cSubName))
	subNameLen := C.size_t(len(config.SubName))
	var consumer StreamConsumer

	const scCacheCapacity uint32 = 32768

	cCacheCapacity := C.uint32_t(scCacheCapacity)
	if len(cacheCapacity) > 0 {
		cCacheCapacity = C.uint32_t(cacheCapacity[0])
	}

	t.mutex.Lock()
	defer t.mutex.Unlock()
	statusC := C.StreamSubscribe(t.client, cStrName, strNameLen, cSubName, subNameLen, C.SubType(config.SubType), 
		C.bool(autoAck), cCacheCapacity, C.uint16_t(config.CachePrefetchLWM),&consumer.consumer)
	if int(statusC.code) != common.Ok {
		return nil, common.CreateStatus(int(statusC.code),
			fmt.Sprintf("Stream cache client failed to subscribe. msg: %s", C.GoString(&statusC.errMsg[0])))
	}
	consumer.mutex = new(sync.RWMutex)
	return &consumer, common.CreateStatus(common.Ok, "")
}

func createElements(pEles *C.StreamElement, count C.uint64_t) ([]Element, common.Status) {
	defer C.free(unsafe.Pointer(pEles))
	if count > C.UINT32_MAX {
		return nil, common.CreateStatus(common.UnexpectedError, fmt.Sprintf("The element count %d out of range", count))
	}
	cEles := unsafe.Slice((*Element)(unsafe.Pointer(pEles)), count)
	eles := make([]Element, count)
	for i := C.uint64_t(0); i < count; i++ {
		eles[i].Ptr = cEles[i].Ptr
		eles[i].Size = cEles[i].Size
		eles[i].Id = cEles[i].Id
	}
	return eles, common.CreateStatus(common.Ok, "")
}

// ReceiveExpectNum receives an expected number of elements from the stream the consumer associated with.
func (t *StreamConsumer) ReceiveExpectNum(expectNum uint32, timeoutMs uint32) ([]Element, common.Status) {
	rc := checkNullConsumerPtr(t)
	if int(rc.Code) != common.Ok {
		return nil, rc
	}

	var count C.uint64_t
	var pEles *C.StreamElement
	t.mutex.Lock()
	defer t.mutex.Unlock()
	statusC := C.StreamConsumerReceiveExpect(t.consumer, C.uint32_t(expectNum), C.uint32_t(timeoutMs), &pEles, &count)
	if int(statusC.code) != common.Ok {
		return nil, common.CreateStatus(int(statusC.code),
			fmt.Sprintf("Stream cache consumer failed to receive. msg: %s", C.GoString(&statusC.errMsg[0])))
	}
	return createElements(pEles, count)
}

// Receive receives elements from the stream the consumer associated with.
func (t *StreamConsumer) Receive(timeoutMs uint32) ([]Element, common.Status) {
	rc := checkNullConsumerPtr(t)
	if int(rc.Code) != common.Ok {
		return nil, rc
	}

	var count C.uint64_t
	var pEles *C.StreamElement
	t.mutex.Lock()
	defer t.mutex.Unlock()
	statusC := C.StreamConsumerReceive(t.consumer, C.uint32_t(timeoutMs), &pEles, &count)
	if int(statusC.code) != common.Ok {
		return nil, common.CreateStatus(int(statusC.code),
			fmt.Sprintf("Stream cache consumer failed to receive. msg: %s", C.GoString(&statusC.errMsg[0])))
	}
	return createElements(pEles, count)
}

// CloseConsumer Close the consumer, after close it will not allow Receive and Ack Elements.
func (t *StreamConsumer) Close() common.Status {
	rc := checkNullConsumerPtr(t)
	if int(rc.Code) != common.Ok {
		return rc
	}

	t.mutex.Lock()
	defer t.mutex.Unlock()
	statusC := C.CloseConsumer(t.consumer)
	if int(statusC.code) != common.Ok {
		return common.CreateStatus(int(statusC.code),
			fmt.Sprintf("Stream cache consumer failed to close. msg: %s", C.GoString(&statusC.errMsg[0])))
	}
	t.consumer = nil
	return common.CreateStatus(common.Ok, "")
}

//  Close the producer, after close it will not allow Send new Elements, and it will trigger flush operations
func (t *StreamProducer) Close() common.Status {
	rc := checkNullProducerPtr(t)
	if int(rc.Code) != common.Ok {
		return rc
	}

	t.mutex.Lock()
	defer t.mutex.Unlock()
	statusC := C.CloseProducer(t.producer)
	if int(statusC.code) != common.Ok {
		return common.CreateStatus(int(statusC.code),
			fmt.Sprintf("Stream cache producer failed to close. msg: %s", C.GoString(&statusC.errMsg[0])))
	}
	t.producer = nil
	return common.CreateStatus(common.Ok, "")
}

// GetStatisticsMessage Get the amount of received elements since this consumer construct, and the amount of elements
// not processed.
func (t *StreamConsumer) GetStatisticsMessage() (uint64, uint64, common.Status) {
	var totalElements uint64 = 0
	var notProcessedElements uint64 = 0
	rc := checkNullConsumerPtr(t)
	if int(rc.Code) != common.Ok {
		return totalElements, notProcessedElements, rc
	}

	var cTotalElements C.uint64_t
	var cNotProcessedElements C.uint64_t

	t.mutex.Lock()
	defer t.mutex.Unlock()
	statusC := C.GetStatisticsMessage(t.consumer, &cTotalElements, &cNotProcessedElements)
	if int(statusC.code) != common.Ok {
		return totalElements, notProcessedElements, common.CreateStatus(int(statusC.code),
			fmt.Sprintf("Stream cache consumer failed to SetInactive. msg: %s", C.GoString(&statusC.errMsg[0])))
	}

	totalElements = uint64(cTotalElements)
	notProcessedElements = uint64(cNotProcessedElements)

	return totalElements, notProcessedElements, common.CreateStatus(common.Ok, "")
}

// Ack acknowledges the elements marked by elementId.
func (t *StreamConsumer) Ack(elementId uint64) common.Status {
	rc := checkNullConsumerPtr(t)
	if int(rc.Code) != common.Ok {
		return rc
	}
	t.mutex.Lock()
	defer t.mutex.Unlock()
	statusC := C.StreamConsumerAck(t.consumer, C.uint64_t(elementId))
	if int(statusC.code) != common.Ok {
		return common.CreateStatus(int(statusC.code),
			fmt.Sprintf("Stream cache consumer failed to ack. msg: %s", C.GoString(&statusC.errMsg[0])))
	}
	return common.CreateStatus(common.Ok, "")
}

// CreateProducer create one Producer to send element.
func (t *StreamClient) CreateProducer(streamName string, delayFlushTimeMs int64, pageSize int64,
	maxStreamSize uint64, autoCleanup bool) (*StreamProducer, common.Status) {
	rc := checkNullClientPtr(t)
	if int(rc.Code) != common.Ok {
		return nil, rc
	}
	cStrName := C.CString(streamName)
	defer C.free(unsafe.Pointer(cStrName))
	strNameLen := C.size_t(len(streamName))
	var producer StreamProducer
	t.mutex.Lock()
	defer t.mutex.Unlock()
	statusC := C.StreamCreateProducer(t.client, cStrName, strNameLen, C.int64_t(delayFlushTimeMs), C.int64_t(pageSize),
		C.uint64_t(maxStreamSize), C.bool(autoCleanup), &producer.producer)
	if int(statusC.code) != common.Ok {
		return nil, common.CreateStatus(int(statusC.code),
			fmt.Sprintf("Stream cache client failed to create producer. msg: %s", C.GoString(&statusC.errMsg[0])))
	}
	producer.mutex = new(sync.RWMutex)
	return &producer, common.CreateStatus(common.Ok, "")
}

// CreateProducerWithConfig create one Producer to send element with config. 
func (t *StreamClient) CreateProducerWithConfig(streamName string, delayFlushTimeMs int64, pageSize int64,
	maxStreamSize uint64, autoCleanup bool, prodConfig ProducerConfig) (*StreamProducer, common.Status) {
	rc := checkNullClientPtr(t)
	if int(rc.Code) != common.Ok {
		return nil, rc
	}
	cStrName := C.CString(streamName)
	defer C.free(unsafe.Pointer(cStrName))
	strNameLen := C.size_t(len(streamName))
	var producer StreamProducer
	t.mutex.Lock()
	defer t.mutex.Unlock()
	statusC := C.StreamCreateProducerWithConfig(t.client, cStrName, strNameLen, C.int64_t(delayFlushTimeMs),
	C.int64_t(pageSize), C.uint64_t(maxStreamSize), C.bool(autoCleanup), C.uint64_t(prodConfig.RetainForNumConsumers),
	C.bool(prodConfig.EncryptStream), C.uint64_t(prodConfig.ReserveSize), &producer.producer)
	if int(statusC.code) != common.Ok {
		return nil, common.CreateStatus(int(statusC.code),
			fmt.Sprintf("Stream cache client failed to create producer. msg: %s", C.GoString(&statusC.errMsg[0])))
	}
	producer.mutex = new(sync.RWMutex)
	return &producer, common.CreateStatus(common.Ok, "")
}

// QueryGlobalProducersNum Query the number of global producers.
func (t *StreamClient) QueryGlobalProducersNum(streamName string) (uint64, common.Status) {
	var producerNum uint64 = 0
	rc := checkNullClientPtr(t)
	if int(rc.Code) != common.Ok {
		return producerNum, rc
	}

	cStrName := C.CString(streamName)
	defer C.free(unsafe.Pointer(cStrName))
	strNameLen := C.size_t(len(streamName))

	var count C.uint64_t

	t.mutex.Lock()
	defer t.mutex.Unlock()
	statusC := C.QueryGlobalProducersNum(t.client, cStrName, strNameLen, &count)
	if int(statusC.code) != common.Ok {
		return producerNum, common.CreateStatus(int(statusC.code),
			fmt.Sprintf("Stream cache client failed to query producer num. msg: %s", C.GoString(&statusC.errMsg[0])))
	}

	producerNum = uint64(count)
	return producerNum, common.CreateStatus(common.Ok, "")
}

// QueryGlobalConsumersNum Query the number of global consumers.
func (t *StreamClient) QueryGlobalConsumersNum(streamName string) (uint64, common.Status) {
	var consumerNum uint64 = 0
	rc := checkNullClientPtr(t)
	if int(rc.Code) != common.Ok {
		return consumerNum, rc
	}

	cStrName := C.CString(streamName)
	defer C.free(unsafe.Pointer(cStrName))
	strNameLen := C.size_t(len(streamName))

	var count C.uint64_t

	t.mutex.Lock()
	defer t.mutex.Unlock()
	statusC := C.QueryGlobalConsumersNum(t.client, cStrName, strNameLen, &count)
	if int(statusC.code) != common.Ok {
		return consumerNum, common.CreateStatus(int(statusC.code),
			fmt.Sprintf("Stream cache client failed to query consumer num. msg: %s", C.GoString(&statusC.errMsg[0])))
	}

	consumerNum = uint64(count)
	return consumerNum, common.CreateStatus(common.Ok, "")
}

// Send Send one element of the stream.
func (t *StreamProducer) Send(element Element) common.Status {
	rc := checkNullProducerPtr(t)
	if int(rc.Code) != common.Ok {
		return rc
	}

	t.mutex.Lock()
	defer t.mutex.Unlock()
	cPtr := (*C.uint8_t)(unsafe.Pointer(element.Ptr))
	statusC := C.StreamProducerSend(t.producer, cPtr, C.uint64_t(element.Size), C.uint64_t(element.Id))
	if int(statusC.code) != common.Ok {
		return common.CreateStatus(int(statusC.code),
			fmt.Sprintf("Stream cache producer failed to send. msg: %s", C.GoString(&statusC.errMsg[0])))
	}
	return common.CreateStatus(common.Ok, "")
}

// DeleteStream  Delete one stream.
func (t *StreamClient) DeleteStream(streamName string) common.Status {
	rc := checkNullClientPtr(t)
	if int(rc.Code) != common.Ok {
		return rc
	}

	cStrName := C.CString(streamName)
	defer C.free(unsafe.Pointer(cStrName))
	strNameLen := C.size_t(len(streamName))

	t.mutex.Lock()
	defer t.mutex.Unlock()
	statusC := C.DeleteStream(t.client, cStrName, strNameLen)
	if int(statusC.code) != common.Ok {
		return common.CreateStatus(int(statusC.code),
			fmt.Sprintf("Stream cache client failed to delete stream. msg: %s", C.GoString(&statusC.errMsg[0])))
	}

	return common.CreateStatus(common.Ok, "")
}
