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

// Package object is the package of the object client
package object

/*
#cgo CFLAGS: -I../include/
#cgo LDFLAGS: -L../lib -ldatasystem_c
#include <stdlib.h>
#include <string.h>
#include "datasystem/c_api/object_client_c_wrapper.h"
#include "datasystem/c_api/utilC.h"
*/
import "C"

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"unsafe"

	"clients/common"
)

// ObjectClient is the main handle object that allows you to make calls into the client API's.
// Create it with the call to OCCreateClient(), and then all subsequent api calls will flow through this handle.
type ObjectClient struct {
	client C.ObjectClient_p
	// lock Protect the ObjectClient
	mutex *sync.RWMutex
}

// WriteModeEnum is the new type defined.
// Use const NoneL2Cache and WriteThroughL2Cache and WriteBackL2Cache together to simulate the enumeration type of C++.
type WriteModeEnum int

// The constant NoneL2Cache and WriteThroughL2Cache and WriteBackL2Cache is of the WriteModeEnum type.
// They is used as two enumeration constants of WriteModeEnum to simulates the enumeration type of C++.
const (
	NoneL2Cache WriteModeEnum = iota
	WriteThroughL2Cache
	WriteBackL2Cache
	NoneL2CacheEvict
)

// ConsistencyTypeEnum is the new type defined.
// Use const Pram and Causal together to simulate the enumeration type of C++.
type ConsistencyTypeEnum int

// The constant Pram and Causal is of the ConsistencyTypeEnum type.
// They is used as two enumeration constants of ConsistencyTypeEnum to simulates the enumeration type of C++.
const (
	Pram ConsistencyTypeEnum = iota
	Causal
)

// PutParam structure is used to transfer parameters during the Put operation.
// It takes parameters including WriteMode and ConsistencyType.
// The WriteMode is a WriteModeEnum "enumeration" type,
// The ConsistencyType is a ConsistencyTypeEnum "enumeration" type,
type PutParam struct {
	ConsistencyType ConsistencyTypeEnum
}

// ObjMetaInfo structure is used to transfer parameters during the GetObjMetaInfo operation.
// It takes parameters including ObjSize and Locations.
// The ObjSize is the size of object data.
// The Locations is the workerIds of the locations.
type ObjMetaInfo struct {
	ObjSize   uint64
	Locations []string
}

func clearAndFree(pointer *C.char, size C.ulong) {
	bytes := unsafe.Slice(pointer, size)
	for i := C.ulong(0); i < size; i++ {
		bytes[i] = 0
	}
	C.free(unsafe.Pointer(pointer))
}

func checkNullPtr(ptr *ObjectClient) common.Status {
	if ptr == nil {
		return common.CreateStatus(common.UnexpectedError, "The ptr of ObjectClient is nil")
	}
	if ptr.client == nil {
		return common.CreateStatus(common.UnexpectedError, "The ObjectClient.client is nil")
	}
	if ptr.mutex == nil {
		return common.CreateStatus(common.UnexpectedError, "The ObjectClient.mutex is nil")
	}
	return common.CreateStatus(common.Ok, "")
}

// CreateClient function creates the ObjectClient and associates it with a given worker host and port numbers.
// After creation, a connection to the worker is NOT established yet.  To connect, see the OCConnectWorker() call.
// To disconnect and/or release the resources from this client, use the OCFreeClient() call.
func CreateClient(param common.ConnectArguments) ObjectClient {
	cWorkerHost := C.CString(param.Host)
	defer C.free(unsafe.Pointer(cWorkerHost))
	cWorkerPort := C.int(param.Port)
	cWorkerTimeout := C.int(param.TimeoutMs)
	// Token
	cWorkerTokenLen := C.ulong(len(param.Token))
	cWorkerToken := (*C.char)(C.CBytes(param.Token))
	defer clearAndFree(cWorkerToken, cWorkerTokenLen)
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

	var ret ObjectClient
	ret.client = C.OCCreateClient(cWorkerHost, cWorkerPort, cWorkerTimeout, cWorkerToken, cWorkerTokenLen,
		cClientPublicKey, cClientPublicKeyLen, cClientPrivateKey, cClientPrivateKeyLen, cServerPublicKey, cServerPublicKeyLen,
		cAccessKey, cAccessKeyLen, cSecretKey, cSecretKeyLen, cTenantID, cTenantIDLen, cEnableCrossNodeConnection)
	ret.mutex = new(sync.RWMutex)
	return ret
}

// Init function connects the client to the worker.
func (t *ObjectClient) Init() common.Status {
	rc := checkNullPtr(t)
	if int(rc.Code) != common.Ok {
		return rc
	}
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	statusC := C.OCConnectWorker(t.client)
	if int(statusC.code) != common.Ok {
		return common.CreateStatus(int(statusC.code),
			fmt.Sprintf("Object cache client failed to connect. msg: %s", C.GoString(&statusC.errMsg[0])))
	}
	return common.CreateStatus(common.Ok, "")
}

// UpdateAkSk function update aksk for the object client with given accessKey and secretKey, return error if failed.
func (t *ObjectClient) UpdateAkSk(accessKey string, secretKey []byte) common.Status {
	cAccessKey := C.CString(accessKey)
	cAccessKeyLen := C.ulong(len(accessKey))
	defer C.free(unsafe.Pointer(cAccessKey))

	cSecretKey := (*C.char)(C.CBytes(secretKey))
	cSecretKeyLen := C.ulong(len(secretKey))
	defer clearAndFree(cSecretKey, cSecretKeyLen)

	statusC := C.OCUpdateAkSk(t.client, cAccessKey, cAccessKeyLen, cSecretKey, cSecretKeyLen)
	if int(statusC.code) != common.Ok {
		return common.CreateStatus(int(statusC.code),
			fmt.Sprintf("Object cache client failed to update aksk. msg: %s", C.GoString(&statusC.errMsg[0])))
	}
	return common.CreateStatus(common.Ok, "")
}

// DestroyClient function frees all resources of the client and terminates a connection if it was connected
func (t *ObjectClient) DestroyClient() {
	rc := checkNullPtr(t)
	if int(rc.Code) == common.Ok {
		t.mutex.Lock()
		defer t.mutex.Unlock()
		C.OCFreeClient(t.client)
		t.client = nil
	}
}

// getConsistencyType method is used to convert the WriteMode type of Go to the string type of C.
func getWriteMode(writeMode WriteModeEnum) (*C.char, error) {
	switch writeMode {
	case NoneL2Cache:
		return C.CString("NONE_L2_CACHE"), nil
	case WriteThroughL2Cache:
		return C.CString("WRITE_THROUGH_L2_CACHE"), nil
	case WriteBackL2Cache:
		return C.CString("WRITE_BACK_L2_CACHE"), nil
	case NoneL2CacheEvict:
		return C.CString("NONE_L2_CACHE_EVICT"), nil
	default:
		return nil, errors.New("invalidParam: The value provided to the PutParam.WriteMode parameter should be" +
			"NoneL2Cache, WriteThroughL2Cache, WriteBackL2Cache or NoneL2CacheEvict")
	}
}

// getConsistencyType method is used to convert the ConsistencyType type of Go to the string type of C.
func getConsistencyType(consistencyType ConsistencyTypeEnum) (*C.char, error) {
	switch consistencyType {
	case Pram:
		return C.CString("PRAM"), nil
	case Causal:
		return C.CString("CAUSAL"), nil
	default:
		return nil, errors.New("invalidParam: The value provided to the PutParam.ConsistencyType parameter should be" +
			"Pram or CAUSAL")
	}
}

// Put an object into the worker.
// A status code is returned, indicating that the Put operation is successful or failed.
func (t *ObjectClient) Put(objectKey string, value string, param PutParam, nestedObjectKeys ...[]string) common.Status {
	// objectKey
	cObjKey := C.CString(objectKey)
	defer C.free(unsafe.Pointer(cObjKey))
	objKeyLen := C.size_t(len(objectKey))
	// value
	type MyString struct {
		Str *C.char
		Len int
	}
	myString := (*MyString)(unsafe.Pointer(&value))
	cValue := myString.Str
	valueLen := C.size_t(len(value))
	// ConsistencyType
	cConsistencyType, err := getConsistencyType(param.ConsistencyType)
	defer C.free(unsafe.Pointer(cConsistencyType))
	if err != nil {
		return common.CreateStatus(common.InvalidParam, err.Error())
	}
	// nestedObjectKeys
	var cNestedObjKeysArray **C.char
	var cNestedObjKeyLenArray []C.size_t
	var cNestedObjKeysNum C.size_t
	if len(nestedObjectKeys) > 0 {
		cNestedObjKeysArray = C.MakeCharsArray(C.int(len(nestedObjectKeys[0])))
		defer C.FreeCharsArray(cNestedObjKeysArray, C.int(len(nestedObjectKeys[0])))
		cNestedObjKeyLenArray = make([]C.size_t, len(nestedObjectKeys[0]))
		for idx, str := range nestedObjectKeys[0] {
			C.SetCharsAtIdx(cNestedObjKeysArray, C.CString(str), C.int(idx))
			cNestedObjKeyLenArray[idx] = C.size_t(len(str))
		}
		cNestedObjKeysNum = C.size_t(len(nestedObjectKeys[0]))
	}
	// to Put
	var statusC C.struct_StatusC
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	if len(nestedObjectKeys) > 0 {
		statusC = C.OCPut(t.client, cObjKey, objKeyLen, cValue, valueLen, cNestedObjKeysArray, &cNestedObjKeyLenArray[0],
			cNestedObjKeysNum, cConsistencyType)
	} else {
		statusC = C.OCPut(t.client, cObjKey, objKeyLen, cValue, valueLen, cNestedObjKeysArray, nil, cNestedObjKeysNum,
			cConsistencyType)
	}
	if int(statusC.code) != common.Ok {
		return common.CreateStatus(int(statusC.code),
			fmt.Sprintf("Object cache client failed to Put. msg: %s", C.GoString(&statusC.errMsg[0])))
	}
	return common.CreateStatus(common.Ok, "")
}

// Get function except that it takes an array of objectKeys to get, and returns the array of string values for those
// objectKeys. The array index of the input objectKeys corresponds to the array index of the output value string.
// If all objectKeys are not found -> error is returned and output value string for the objectKey set to an empty string
func (t *ObjectClient) Get(objectKeys []string, timeoutMs ...int64) ([]string, common.Status) {
	rc := checkNullPtr(t)
	if int(rc.Code) != common.Ok {
		return nil, rc
	}
	// objectKeys
	if len(objectKeys) == 0 {
		return nil, common.CreateStatus(common.InvalidParam, "invalid param: objectKeys is null")
	}
	cObjectKeysArray := C.MakeCharsArray(C.int(len(objectKeys)))
	defer C.FreeCharsArray(cObjectKeysArray, C.int(len(objectKeys)))
	cObjectKeysLen := make([]C.size_t, len(objectKeys))
	for i := 0; i < len(objectKeys); i++ {
		cObjectKeysLen[i] = C.size_t(len(objectKeys[i]))
	}
	for idx, str := range objectKeys {
		C.SetCharsAtIdx(cObjectKeysArray, C.CString(str), C.int(idx))
	}
	// timeoutMs
	cTimeoutMs := C.uint(0)
	if len(timeoutMs) > 0 {
		cTimeoutMs = C.uint(timeoutMs[0])
	}
	// output value
	cValues := C.MakeCharsArray(C.int(len(objectKeys)))
	defer C.FreeCharsArray(cValues, C.int(len(objectKeys)))
	cValuesLen := C.MakeNumArray(C.int(len(objectKeys)))
	defer C.FreeNumArray(cValuesLen)
	// to Get
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	statusC := C.OCGet(t.client, cObjectKeysArray, &cObjectKeysLen[0], C.ulong(len(objectKeys)), cTimeoutMs, cValues,
		cValuesLen)
	// We can have partial results even if their is an error on a objectKeys
	values := make([]string, len(objectKeys))
	for i := 0; i < len(objectKeys); i++ {
		valLen := C.GetNumAtIdx(cValuesLen, C.int(i))
		if valLen > math.MaxInt32 {
			return nil, common.CreateStatus(common.InvalidParam,
				fmt.Sprintf("Invalid value size %d is get, which should be between (0, %d]", valLen, math.MaxInt32))
		}
		values[i] = C.GoStringN(C.GetCharsAtIdx(cValues, C.int(i)), C.int(C.GetNumAtIdx(cValuesLen, C.int(i))))
	}

	if int(statusC.code) != common.Ok {
		return values, common.CreateStatus(int(statusC.code),
			fmt.Sprintf("Object cache client failed to Get. msg: %s", C.GoString(&statusC.errMsg[0])))
	}

	return values, common.CreateStatus(common.Ok, "")
}

// GIncreaseRef function increases the global reference count to objects in the worker.
// The returned value contains the object key of the GIncreaseRef failure and status code.
func (t *ObjectClient) GIncreaseRef(objectKeys []string, remoteClientId ...string) ([]string, common.Status) {
	rc := checkNullPtr(t)
	if int(rc.Code) != common.Ok {
		return nil, rc
	}
	// objectKeys
	if len(objectKeys) == 0 {
		return nil, common.CreateStatus(common.InvalidParam, "invalid param: objectKeys is null")
	}
	objectKeysArray := C.MakeCharsArray(C.int(len(objectKeys)))
	defer C.FreeCharsArray(objectKeysArray, C.int(len(objectKeys)))
	for idx, str := range objectKeys {
		C.SetCharsAtIdx(objectKeysArray, C.CString(str), C.int(idx))
	}
	cObjectKeysLen := make([]C.size_t, len(objectKeys))
	for i := 0; i < len(objectKeys); i++ {
		cObjectKeysLen[i] = C.size_t(len(objectKeys[i]))
	}
	// remoteClientId
	var cRemoteClientId *C.char
	cRemoteClientIdLen := C.size_t(0)
	if len(remoteClientId) > 0 {
		cRemoteClientId = C.CString(remoteClientId[0])
		cRemoteClientIdLen = C.size_t(len(remoteClientId[0]))
	}
	defer C.free(unsafe.Pointer(cRemoteClientId))
	// failedObjectKeys
	cFailedObjectKeys := C.MakeCharsArray(C.int(len(objectKeys)))
	defer C.FreeCharsArray(cFailedObjectKeys, C.int(len(objectKeys)))
	var failedCount C.ulong
	// to OCGIncreaseRef
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	statusC := C.OCGIncreaseRef(t.client, objectKeysArray, &cObjectKeysLen[0], C.ulong(len(objectKeys)), cRemoteClientId,
		cRemoteClientIdLen, cFailedObjectKeys, &failedCount)
	if failedCount > C.ulong(len(objectKeys)) {
		return nil, common.CreateStatus(common.UnexpectedError,
			fmt.Sprintf("Unexpected failed object count %d.", failedCount))
	}
	// We can have partial results even if their is an error on a key
	failedObjectKeys := make([]string, failedCount)
	for i := C.ulong(0); i < failedCount; i++ {
		failedObjectKeys[i] = C.GoString(C.GetCharsAtIdx(cFailedObjectKeys, C.int(i)))
	}
	if int(statusC.code) != common.Ok {
		return failedObjectKeys, common.CreateStatus(int(statusC.code),
			fmt.Sprintf("Object cache client failed to GIncreaseRef. msg: %s", C.GoString(&statusC.errMsg[0])))
	}

	return failedObjectKeys, common.CreateStatus(common.Ok, "")
}

// GDecreaseRef function decrease the global reference count to objects in the worker.
// The returned value contains the object key of the GDecreaseRef failure and status code.
func (t *ObjectClient) GDecreaseRef(objectKeys []string, remoteClientId ...string) ([]string, common.Status) {
	rc := checkNullPtr(t)
	if int(rc.Code) != common.Ok {
		return nil, rc
	}
	// objectKeys
	if len(objectKeys) == 0 {
		return nil, common.CreateStatus(common.InvalidParam, "invalid param: objectKeys is null")
	}
	objectKeysArray := C.MakeCharsArray(C.int(len(objectKeys)))
	defer C.FreeCharsArray(objectKeysArray, C.int(len(objectKeys)))
	for idx, str := range objectKeys {
		C.SetCharsAtIdx(objectKeysArray, C.CString(str), C.int(idx))
	}
	cObjectKeysLen := make([]C.size_t, len(objectKeys))
	for i := 0; i < len(objectKeys); i++ {
		cObjectKeysLen[i] = C.size_t(len(objectKeys[i]))
	}
	// remoteClientId
	var cRemoteClientId *C.char
	cRemoteClientIdLen := C.size_t(0)
	if len(remoteClientId) > 0 {
		cRemoteClientId = C.CString(remoteClientId[0])
		cRemoteClientIdLen = C.size_t(len(remoteClientId[0]))
	}
	defer C.free(unsafe.Pointer(cRemoteClientId))
	// cFailedObjectKeys
	cFailedObjectKeys := C.MakeCharsArray(C.int(len(objectKeys)))
	defer C.FreeCharsArray(cFailedObjectKeys, C.int(len(objectKeys)))
	var failedCount C.ulong
	// to OCDeccreaseRef
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	statusC := C.OCDeccreaseRef(t.client, objectKeysArray, &cObjectKeysLen[0], C.ulong(len(objectKeys)), cRemoteClientId,
		cRemoteClientIdLen, cFailedObjectKeys, &failedCount)
	if failedCount > C.ulong(len(objectKeys)) {
		return nil, common.CreateStatus(common.UnexpectedError,
			fmt.Sprintf("Unexpected failed object count %d.", failedCount))
	}
	// We can have partial results even if their is an error on a key
	failedObjectKeys := make([]string, failedCount)
	for i := C.ulong(0); i < failedCount; i++ {
		failedObjectKeys[i] = C.GoString(C.GetCharsAtIdx(cFailedObjectKeys, C.int(i)))
	}
	if int(statusC.code) != common.Ok {
		return failedObjectKeys, common.CreateStatus(int(statusC.code),
			fmt.Sprintf("Object cache client failed to GDecreaseRef. msg: %s", C.GoString(&statusC.errMsg[0])))
	}

	return failedObjectKeys, common.CreateStatus(common.Ok, "")
}

// ReleaseGRefs function releases obj ref of remote client id when remote client that outside the cloud crash.
// A status code is returned, indicating that the ReleaseGRefs operation is successful or failed.
func (t *ObjectClient) ReleaseGRefs(remoteClientId string) common.Status {
	rc := checkNullPtr(t)
	if int(rc.Code) != common.Ok {
		return rc
	}
	// remoteClientId
	cRemoteClientId := C.CString(remoteClientId)
	defer C.free(unsafe.Pointer(cRemoteClientId))
	cRemoteClientIdLen := C.size_t(len(remoteClientId))
	// to OCReleaseGRefs
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	statusC := C.OCReleaseGRefs(t.client, cRemoteClientId, cRemoteClientIdLen)
	if int(statusC.code) != common.Ok {
		return common.CreateStatus(int(statusC.code),
			fmt.Sprintf("Object cache client failed to ReleaseGRefs. msg: %s", C.GoString(&statusC.errMsg[0])))
	}
	return common.CreateStatus(common.Ok, "")
}

func fillObjMetaInfos(objMetas []ObjMetaInfo, cObjSizes []C.size_t, cLocations **C.char, cLocNumPerObj []C.size_t) {
	objNum := len(objMetas)
	pos := 0
	for i := 0; i < objNum; i++ {
		objMetas[i].ObjSize = uint64(cObjSizes[i])
		num := int(cLocNumPerObj[i])
		objMetas[i].Locations = make([]string, 0, num)
		for j := 0; j < num; j++ {
			objMetas[i].Locations = append(objMetas[i].Locations, C.GoString(C.GetCharsAtIdx(cLocations, C.int(pos))))
			pos++
		}
	}
}

// GetObjMetaInfo function gets meta info of the given objects.
// A status code is returned, indicating that the GetObjMetaInfo operation is successful or failed.
func (t *ObjectClient) GetObjMetaInfo(tenantId string, objectKeys []string) ([]ObjMetaInfo, common.Status) {
	rc := checkNullPtr(t)
	if int(rc.Code) != common.Ok {
		return nil, rc
	}

	cTenantId := C.CString(tenantId)
	defer C.free(unsafe.Pointer(cTenantId))
	cTenantIdLen := C.size_t(len(tenantId))
	objNum := len(objectKeys)
	cObjKeys := C.MakeCharsArray(C.int(objNum))
	defer C.FreeCharsArray(cObjKeys, C.int(objNum))
	for idx, str := range objectKeys {
		C.SetCharsAtIdx(cObjKeys, C.CString(str), C.int(idx))
	}
	cObjKeysLen := make([]C.size_t, objNum)
	for i := 0; i < objNum; i++ {
		cObjKeysLen[i] = C.size_t(len(objectKeys[i]))
	}
	cObjSizes := make([]C.size_t, objNum)
	var cLocations **C.char
	var locationNum C.int
	cLocNumPerObj := make([]C.size_t, objNum)

	t.mutex.RLock()
	defer t.mutex.RUnlock()
	statusC := C.GetObjMetaInfo(t.client, cTenantId, cTenantIdLen, cObjKeys, &cObjKeysLen[0], C.ulong(objNum),
		&cObjSizes[0], &cLocations, &cLocNumPerObj[0], &locationNum)
	defer C.FreeCharsArray(cLocations, locationNum)

	if int(statusC.code) != common.Ok {
		return nil, common.CreateStatus(int(statusC.code),
			fmt.Sprintf("Object cache client failed to GetObjMetaInfo. msg: %s", C.GoString(&statusC.errMsg[0])))
	}
	objMetas := make([]ObjMetaInfo, objNum)
	fillObjMetaInfos(objMetas, cObjSizes, cLocations, cLocNumPerObj)

	return objMetas, common.CreateStatus(common.Ok, "")
}
