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

// Package kv is the package of the kv client
package kv

/*
#cgo CFLAGS: -I../include/
#cgo LDFLAGS: -L../lib -ldatasystem_c
#include <stdlib.h>
#include <string.h>
#include "datasystem/c_api/kv_client_c_wrapper.h"
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

// StateClient is the main handle object that allows you to make calls into the client API's.
// Create it with the call to CreateClient(), and then all subsequent api calls will flow through this handle.
type StateClient struct {
	client C.KVClient_p
	// lock Protect the StateClient
	mutex *sync.RWMutex
}

// WriteModeEnum is the new type defined.
// Use const NoneL2Cache and WriteThroughL2Cache and WriteBackL2Cache together to simulate the enumeration type of C++.
type WriteModeEnum int

// The constant NoneL2Cache and WriteThroughL2Cache and WriteBackL2Cache is of the WriteModeEnum type.
// They is used as two enumeration constants of WriteModeEnum to simulates the enumeration type of C++.
// When Set() is called, it can be used as WriteMode argument of SetParam to specify whether to write to the L2 cache.
const (
	NoneL2Cache WriteModeEnum = iota
	WriteThroughL2Cache
	WriteBackL2Cache
	NoneL2CacheEvict
)

// ExistenceOptEnum is the new type defined.
// Use const NONE and NX together to simulate the enum type of C++.
type ExistenceOptEnum int

// The const NONE and NX is of the ExistenceOptEnum type.
// They is used as two enumeration constants of ExistenceOptEnum to simulates the enumeration type of C++.
// When Set() is called, it can be used as ExistenceOpt argument of SetParam.
const (
	NONE ExistenceOptEnum = iota
	NX
)

// SetParam structure is used to transfer parameters during the set operation.
// It takes parameters including WriteMode.
// The WriteMode is a WriteModeEnum "enumeration" type,
// the options are NoneL2Cache or WriteThroughL2Cache or WriteBackL2Cache.
type SetParam struct {
	WriteMode    WriteModeEnum
	TTLSecond    uint32
	ExistenceOpt ExistenceOptEnum
}

func clearAndFree(pointer *C.char, size C.ulong) {
	bytes := unsafe.Slice(pointer, size)
	for i := C.ulong(0); i < size; i++ {
		bytes[i] = 0
	}
	C.free(unsafe.Pointer(pointer))
}

func checkNullPtr(ptr *StateClient) common.Status {
	if ptr == nil {
		return common.CreateStatus(common.UnexpectedError, "The ptr of StateClient is nil")
	}
	if ptr.client == nil {
		return common.CreateStatus(common.UnexpectedError, "The StateClient.client is nil")
	}
	if ptr.mutex == nil {
		return common.CreateStatus(common.UnexpectedError, "The StateClient.mutex is nil")
	}
	return common.CreateStatus(common.Ok, "")
}

// CreateClient function creates the StateClient and associates it with a given worker host and port numbers.
// After creation, a connection to the worker is NOT established yet.  To connect, see the Connect() call.
// To disconnect and/or release the resources from this client, use the Free() call.
func CreateClient(param common.ConnectArguments) StateClient {
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

	var ret StateClient
	ret.client = C.KVCreateClient(cWorkerHost, cWorkerPort, cWorkerTimeout, cWorkerToken, cWorkerTokenLen,
		cClientPublicKey, cClientPublicKeyLen, cClientPrivateKey, cClientPrivateKeyLen, cServerPublicKey, cServerPublicKeyLen,
		cAccessKey, cAccessKeyLen, cSecretKey, cSecretKeyLen, cTenantID, cTenantIDLen, cEnableCrossNodeConnection)
	ret.mutex = new(sync.RWMutex)
	return ret
}

// Init function connects the client to the worker.
func (t *StateClient) Init() common.Status {
	rc := checkNullPtr(t)
	if int(rc.Code) != common.Ok {
		return rc
	}
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	statusC := C.SCConnectWorker(t.client)
	if int(statusC.code) != common.Ok {
		return common.CreateStatus(int(statusC.code),
			fmt.Sprintf("State cache client failed to connect. msg: %s", C.GoString(&statusC.errMsg[0])))
	}
	return common.CreateStatus(common.Ok, "")
}

// UpdateAkSk function update aksk for the object client with given accessKey and secretKey, return error if failed.
func (t *StateClient) UpdateAkSk(accessKey string, secretKey []byte) common.Status {
	cAccessKey := C.CString(accessKey)
	cAccessKeyLen := C.ulong(len(accessKey))
	defer C.free(unsafe.Pointer(cAccessKey))

	cSecretKey := (*C.char)(C.CBytes(secretKey))
	cSecretKeyLen := C.ulong(len(secretKey))
	defer clearAndFree(cSecretKey, cSecretKeyLen)

	statusC := C.SCUpdateAkSk(t.client, cAccessKey, cAccessKeyLen, cSecretKey, cSecretKeyLen)
	if int(statusC.code) != common.Ok {
		return common.CreateStatus(int(statusC.code),
			fmt.Sprintf("Object cache client failed to update aksk. msg: %s", C.GoString(&statusC.errMsg[0])))
	}
	return common.CreateStatus(common.Ok, "")
}

// DestroyClient function frees all resources of the client and terminates a connection if it was connected
func (t *StateClient) DestroyClient() {
	rc := checkNullPtr(t)
	if int(rc.Code) == common.Ok {
		t.mutex.Lock()
		defer t.mutex.Unlock()
		C.SCFreeClient(t.client)
		t.client = nil
	}
}

// getExistenceOpt method is used to convert the ExistenceOpt type of Go to the string type of C.
func getExistenceOpt(existenceOpt ExistenceOptEnum) (*C.char, error) {
	switch existenceOpt {
	case NONE:
		return C.CString("NONE"), nil
	case NX:
		return C.CString("NX"), nil
	default:
		return nil, errors.New("invalidParam: The value provided to the SetParam.ExistenceOpt parameter should be NONE or NX")
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
			"NoneL2Cache, WriteThroughL2Cache, WriteBackL2Cache or NONE_L2_CACHE_EVICT")
	}
}

// Set function sets a string of data (value) into the worker.  The data is uniquely identified by key.
func (t *StateClient) Set(key string, value string, param SetParam) common.Status {
	rc := checkNullPtr(t)
	if int(rc.Code) != common.Ok {
		return rc
	}
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	keyLen := C.size_t(len(key))
	type MyString struct {
		Str *C.char
		Len int
	}
	myString := (*MyString)(unsafe.Pointer(&value))
	cValue := myString.Str
	valueLen := C.size_t(len(value))
	// WriteMode
	cWriteMode, err := getWriteMode(param.WriteMode)
	defer C.free(unsafe.Pointer(cWriteMode))
	if err != nil {
		return common.CreateStatus(common.InvalidParam, err.Error())
	}
	cTTLSecond := C.uint(param.TTLSecond)
	// ExistenceOpt
	cExistenceOpt, err := getExistenceOpt(param.ExistenceOpt)
	defer C.free(unsafe.Pointer(cExistenceOpt))
	if err != nil {
		return common.CreateStatus(common.InvalidParam, err.Error())
	}
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	statusC := C.SCSet(t.client, cKey, keyLen, cValue, valueLen, cWriteMode, cTTLSecond, cExistenceOpt)
	if int(statusC.code) != common.Ok {
		return common.CreateStatus(int(statusC.code),
			fmt.Sprintf("State cache client failed to Set. msg: %s", C.GoString(&statusC.errMsg[0])))
	}
	return common.CreateStatus(common.Ok, "")
}

// SetValue function sets a string of data (value) into the worker.  Return the key of object,
// if set error, return empty string.
func (t *StateClient) SetValue(value string, param SetParam) (string, common.Status) {
	rc := checkNullPtr(t)
	if int(rc.Code) != common.Ok {
		return "", rc
	}
	maxSetSize := math.MaxInt32 // 2G-1
	dataSize := len(value)
	if dataSize == 0 || dataSize > maxSetSize {
		return "", common.CreateStatus(common.InvalidParam,
			fmt.Sprintf("Invalid value size %d is set, which should be between (0, %d]", dataSize, maxSetSize))
	}
	type MyString struct {
		Str *C.char
		Len int
	}
	myString := (*MyString)(unsafe.Pointer(&value))
	cValue := myString.Str
	valueLen := C.size_t(dataSize)
	// WriteMode
	cWriteMode, err := getWriteMode(param.WriteMode)
	defer C.free(unsafe.Pointer(cWriteMode))
	if err != nil {
		return "", common.CreateStatus(common.InvalidParam, err.Error())
	}
	cTTLSecond := C.uint(param.TTLSecond)
	// ExistenceOpt
	cExistenceOpt, err := getExistenceOpt(param.ExistenceOpt)
	defer C.free(unsafe.Pointer(cExistenceOpt))
	if err != nil {
		return "", common.CreateStatus(common.InvalidParam, err.Error())
	}
	var cKey *C.char = nil
	defer func() {
		C.free(unsafe.Pointer(cKey))
	}()
	var keyLen C.size_t
	var key string
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	statusC := C.SCSetValue(t.client, &cKey, &keyLen, cValue, valueLen, cWriteMode, cTTLSecond, cExistenceOpt)
	key = C.GoString(cKey)
	if int(statusC.code) != common.Ok {
		return "", common.CreateStatus(int(statusC.code),
			fmt.Sprintf("State cache client failed to Set. msg: %s", C.GoString(&statusC.errMsg[0])))
	}
	return key, common.CreateStatus(common.Ok, "")
}

// Get function takes key identifier as input and fetches the string of data for that key and returns it.
func (t *StateClient) Get(key string, timeoutms ...uint32) (string, common.Status) {
	rc := checkNullPtr(t)
	if int(rc.Code) != common.Ok {
		return "", rc
	}
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	var cVal *C.char = nil
	defer func() {
		C.free(unsafe.Pointer(cVal))
	}()
	keyLen := C.size_t(len(key))
	var valLen C.size_t
	ctimeoutms := C.uint(0)
	if len(timeoutms) > 0 {
		ctimeoutms = C.uint(timeoutms[0])
	}
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	statusC := C.SCGet(t.client, cKey, keyLen, ctimeoutms, &cVal, &valLen)
	if int(statusC.code) != common.Ok {
		return "", common.CreateStatus(int(statusC.code),
			fmt.Sprintf("State cache client failed to Get. msg: %s", C.GoString(&statusC.errMsg[0])))
	}

	if math.MaxInt32 < valLen {
		return "", common.CreateStatus(common.InvalidParam,
			fmt.Sprintf("Invalid value size %d is get, which should be between (0, %d]", valLen, math.MaxInt32))
	}
	value := C.GoStringN(cVal, C.int(valLen))
	return value, common.CreateStatus(common.Ok, "")
}

// Del function deletes the key and its data.
func (t *StateClient) Del(key string) common.Status {
	rc := checkNullPtr(t)
	if int(rc.Code) != common.Ok {
		return rc
	}
	cKey := C.CString(key)
	cKeyLen := C.size_t(len(key))
	defer C.free(unsafe.Pointer(cKey))

	t.mutex.RLock()
	defer t.mutex.RUnlock()
	statusC := C.SCDel(t.client, cKey, cKeyLen)
	if int(statusC.code) != common.Ok {
		return common.CreateStatus(int(statusC.code),
			fmt.Sprintf("State cache client failed to delete. msg: %s", C.GoString(&statusC.errMsg[0])))
	}
	return common.CreateStatus(common.Ok, "")
}

// GetArray function is similar to Get(), except that it takes an array of keys to get, and returns and array of string
// values for those keys.  The array index of the input key corresponds to the array index of the output value string.
// If a key is not found -> error is returned and output value string for the key set to an empty string(length 0)
func (t *StateClient) GetArray(keys []string, timeoutms ...uint32) ([]string, common.Status) {
	rc := checkNullPtr(t)
	if int(rc.Code) != common.Ok {
		return nil, rc
	}
	if len(keys) == 0 {
		return nil, common.CreateStatus(common.InvalidParam, "invalid param: objectKeys is null")
	}

	// keys golang string array to c string array
	valuesC := C.MakeCharsArray(C.int(len(keys)))
	defer C.FreeCharsArray(valuesC, C.int(len(keys)))
	valuesLenC := C.MakeNumArray(C.int(len(keys)))
	defer C.FreeNumArray(valuesLenC)
	keyArray := C.MakeCharsArray(C.int(len(keys)))
	defer C.FreeCharsArray(keyArray, C.int(len(keys)))
	keyLenArray := make([]C.size_t, len(keys))
	for i := 0; i < len(keys); i++ {
		keyLenArray[i] = C.size_t(len(keys[i]))
	}

	for idx, str := range keys {
		cstr := C.CString(str)
		C.SetCharsAtIdx(keyArray, cstr, C.int(idx))
	}

	ctimeoutms := C.uint(0)
	if len(timeoutms) > 0 {
		ctimeoutms = C.uint(timeoutms[0])
	}
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	statusC := C.SCGetArray(t.client, keyArray, &keyLenArray[0], C.ulong(len(keys)), ctimeoutms, valuesC, valuesLenC)

	// We can have partial results even if their is an error on a key
	values := make([]string, len(keys))
	for i := 0; i < len(keys); i++ {
		valLen := C.GetNumAtIdx(valuesLenC, C.int(i))
		if valLen > math.MaxInt32 {
			return nil, common.CreateStatus(common.InvalidParam,
				fmt.Sprintf("Invalid value size %d is get, which should be between (0, %d]", valLen, math.MaxInt32))
		}
		values[i] = C.GoStringN(C.GetCharsAtIdx(valuesC, C.int(i)), C.int(C.GetNumAtIdx(valuesLenC, C.int(i))))
	}

	if int(statusC.code) != common.Ok {
		return values, common.CreateStatus(int(statusC.code),
			fmt.Sprintf("State cache client failed to Get. msg: %s", C.GoString(&statusC.errMsg[0])))
	}

	return values, common.CreateStatus(common.Ok, "")
}

// DelArray function is similar to Del(), except that it takes an array of keys
// to delete as an argument instead of a single key.
func (t *StateClient) DelArray(keys []string) ([]string, common.Status) {
	rc := checkNullPtr(t)
	if int(rc.Code) != common.Ok {
		return nil, rc
	}
	if len(keys) == 0 {
		return nil, common.CreateStatus(common.InvalidParam, "invalid param: objectKeys is null")
	}

	// keys golang string array to c string array
	failedKeysC := C.MakeCharsArray(C.int(len(keys)))
	defer C.FreeCharsArray(failedKeysC, C.int(len(keys)))

	// converting from golang to cstyle strings
	keyArray := C.MakeCharsArray(C.int(len(keys)))
	defer C.FreeCharsArray(keyArray, C.int(len(keys)))

	for idx, str := range keys {
		cstr := C.CString(str)
		C.SetCharsAtIdx(keyArray, cstr, C.int(idx))
	}

	var failedCount C.ulong
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	statusC := C.SCDelArray(t.client, keyArray, C.ulong(len(keys)), failedKeysC, &failedCount)

	if failedCount > C.ulong(len(keys)) {
		return nil, common.CreateStatus(common.UnexpectedError,
			fmt.Sprintf("Unexpected failed key count %d.", failedCount))
	}
	// We can have partial results even if their is an error on a key
	failedKeys := make([]string, failedCount)
	for i := C.ulong(0); i < failedCount; i++ {
		failedKeys[i] = C.GoString(C.GetCharsAtIdx(failedKeysC, C.int(i)))
	}

	if int(statusC.code) != common.Ok {
		return failedKeys, common.CreateStatus(int(statusC.code),
			fmt.Sprintf("State cache client failed to Del. msg: %s", C.GoString(&statusC.errMsg[0])))
	}

	return failedKeys, common.CreateStatus(common.Ok, "")
}

// GenerateKey function is generating a unique key for SET.
// If the key fails to be generated, an empty string is returned.
func (t *StateClient) GenerateKey() string {
	rc := checkNullPtr(t)
	if int(rc.Code) != common.Ok {
		return ""
	}
	var cKey *C.char = nil
	defer func() {
		C.free(unsafe.Pointer(cKey))
	}()
	var keyLen C.size_t
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	keyLen = C.SCGenerateKey(t.client, &cKey)
	return C.GoStringN(cKey, C.int(keyLen))
}
