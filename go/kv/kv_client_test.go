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
// Testing kv package
package kv

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

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

// A helper function so that the testcase can create state cache client.
func CreateClientForTest() (*StateClient, common.Status) {
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
	s = theClient.Init()
	if s.IsError() {
		theClient.DestroyClient()
		return nil, common.CreateStatus(UnknowError,
			"Init function connects the client to the worker failed!")
	}
	return &theClient, common.CreateStatus(common.Ok, "")
}

// A unit test for testing the connection api
func TestSet(t *testing.T) {
	var theClient *StateClient
	var s common.Status

	if theClient, s = CreateClientForTest(); theClient == nil {
		t.Fatalf(s.ToString())
	}
	defer theClient.DestroyClient()

	// Set a value
	var key1 string = "key23"
	var value1 string = "this is some data"
	var setParam = SetParam{WriteMode: NoneL2Cache}
	s = theClient.Set(key1, value1, setParam)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
}

// A unit test for testing the set api
func TestSetWriteMode(t *testing.T) {
	var theClient *StateClient
	var s common.Status

	if theClient, s = CreateClientForTest(); theClient == nil {
		t.Fatalf(s.ToString())
	}
	defer theClient.DestroyClient()

	// Set a value in NoneL2Cache mode
	var key1 string = "key111"
	var value1 string = "this is some data"
	var setParam1 = SetParam{WriteMode: NoneL2Cache}
	s = theClient.Set(key1, value1, setParam1)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	var resultValue string
	resultValue, s = (*theClient).Get(key1)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	if value1 != resultValue {
		t.Fatalf("Get is not getting value set by set(). value1: [%s], resultValue: [%s]", value1, resultValue)
	}

	// Set a value in WriteThroughL2Cache mode
	var key2 string = "key222"
	var value2 string = "this is some data"
	var setParam2 = SetParam{WriteMode: NoneL2Cache}
	s = theClient.Set(key2, value2, setParam2)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	resultValue, s = (*theClient).Get(key2)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	if value2 != resultValue {
		t.Fatalf("Get is not getting value set by set()")
	}

	// Set a value in WriteBackL2Cache mode
	var key3 string = "key333"
	var value3 string = "this is some data"
	var setParam3 = SetParam{WriteMode: NoneL2Cache}
	s = theClient.Set(key3, value3, setParam3)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	resultValue, s = (*theClient).Get(key3)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	if value3 != resultValue {
		t.Fatalf("Get is not getting value set by set()")
	}

	// Set a value in NoneL2CacheEvict mode
	var key4 string = "key444"
	var value4 string = "this is some data"
	var setParam4 = SetParam{WriteMode: NoneL2CacheEvict}
	s = theClient.Set(key4, value4, setParam4)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	resultValue, s = (*theClient).Get(key3)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	if value3 != resultValue {
		t.Fatalf("Get is not getting value set by set()")
	}
}

func TestSetTtlObject(t *testing.T) {
	var theClient *StateClient
	var s common.Status

	if theClient, s = CreateClientForTest(); theClient == nil {
		t.Fatalf(s.ToString())
	}
	defer theClient.DestroyClient()

	// Set a value in NoneL2Cache mode
	var key1 string = "newKey1"
	var value1 string = "hello"
	var setParam1 = SetParam{WriteMode: NoneL2Cache, TTLSecond: 1}
	s = theClient.Set(key1, value1, setParam1)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	var resultValue string
	resultValue, s = (*theClient).Get(key1)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	if value1 != resultValue {
		t.Fatalf("Get is not getting value set by set()")
	}
	const sleepTime int = 2
	time.Sleep(time.Duration(sleepTime) * time.Second)
	resultValue, s = (*theClient).Get(key1)
	if s.IsOk() {
		t.Fatalf(s.ToString())
	}
}

func TestGet(t *testing.T) {
	var theClient *StateClient
	var s common.Status

	if theClient, s = CreateClientForTest(); theClient == nil {
		t.Fatalf(s.ToString())
	}
	defer theClient.DestroyClient()

	s = common.Context.SetTraceID("TestGet")
	if s.IsError() {
		t.Fatalf(s.ToString())
	}

	// Set a value
	var key1 string = "key246"
	var value1 string = "this is some data"
	var setParam = SetParam{WriteMode: NoneL2Cache}
	s = theClient.Set(key1, value1, setParam)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}

	// Get the value
	var resultValue string
	resultValue, s = (*theClient).Get(key1)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	if value1 != resultValue {
		t.Fatalf("Get is not getting value set by set()")
	}
}

func TestGetValWithCutOffCharacter(t *testing.T) {
	var theClient *StateClient
	var s common.Status

	if theClient, s = CreateClientForTest(); theClient == nil {
		t.Fatalf(s.ToString())
	}
	defer theClient.DestroyClient()

	// Set a value whose key is illegal.
	var key1 string = "key\0001234"
	var value1 string = "this is some data"
	var setParam1 = SetParam{WriteMode: NoneL2Cache}
	s = theClient.Set(key1, value1, setParam1)
	if s.Code != common.InvalidParam {
		t.Fatalf("The error message is not suitable.")
	}

	// Set a valuel.
	var key2 string = "key1234"
	var value2 string = "this is \000 some data"
	var setParam2 = SetParam{WriteMode: NoneL2Cache}
	s = theClient.Set(key2, value2, setParam2)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}

	// Get the value
	var resultValue string
	resultValue, s = (*theClient).Get(key2)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	if value2 != resultValue {
		t.Fatalf("Get is not getting value set by set()")
	}
}

func TestDelSuccess(t *testing.T) {
	var theClient *StateClient
	var s common.Status

	if theClient, s = CreateClientForTest(); theClient == nil {
		t.Fatalf(s.ToString())
	}
	defer theClient.DestroyClient()

	// Set a value
	var key1 string = "key145"
	var value1 string = "this is some data"
	var setParam = SetParam{WriteMode: NoneL2Cache}
	s = theClient.Set(key1, value1, setParam)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}

	// Delete the key
	s = theClient.Del(key1)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
}

func TestDelArraySuccess(t *testing.T) {
	var theClient *StateClient
	var s common.Status

	if theClient, s = CreateClientForTest(); theClient == nil {
		t.Fatalf(s.ToString())
	}
	defer theClient.DestroyClient()

	var setParam = SetParam{WriteMode: NoneL2Cache}

	var numKeys uint64 = 0
	// Set a value
	var key1 string = "key789"
	var value1 string = "this is some data"
	s = theClient.Set(key1, value1, setParam)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	numKeys++

	// Set a value
	var key2 string = "key675"
	s = theClient.Set(key2, value1, setParam)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	numKeys++

	// Delete multiple keys
	outputE, s := theClient.DelArray([]string{"key789", "key675"})
	if s.IsError() {
		t.Fatalf(s.ToString())
	}

	if len(outputE) != 0 {
		t.Fatalf("Output Count DelArray Doesn't match")
	}
}

func TestGetArraySuccess(t *testing.T) {
	var theClient *StateClient
	var s common.Status

	if theClient, s = CreateClientForTest(); theClient == nil {
		t.Fatalf(s.ToString())
	}
	defer theClient.DestroyClient()

	var setParam = SetParam{WriteMode: NoneL2Cache}

	// Set a value
	var key1 string = "key9890"
	var value1 string = "data1"
	s = theClient.Set(key1, value1, setParam)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}

	// Set a value
	var key2 string = "key678"
	var value2 string = "data2"
	s = theClient.Set(key2, value2, setParam)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}

	// Getting multiple keys
	outputE, s := theClient.GetArray([]string{"key9890", "key678"})
	if s.IsError() {
		t.Fatalf(s.ToString())
	}

	if outputE[0] != value1 || outputE[1] != value2 {
		t.Fatalf("Output Strings for GetArray Doesn't match")
	}
}

func TestGetArrayWithCutOffCharacter(t *testing.T) {
	var theClient *StateClient
	var s common.Status

	if theClient, s = CreateClientForTest(); theClient == nil {
		t.Fatalf(s.ToString())
	}
	defer theClient.DestroyClient()

	var setParam = SetParam{WriteMode: NoneL2Cache}

	// Set a value
	var key1 string = "key9890"
	var value1 string = "this is \000 some data1"
	s = theClient.Set(key1, value1, setParam)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}

	// Set a value
	var key2 string = "key678"
	var value2 string = "this is \000 some data2"
	s = theClient.Set(key2, value2, setParam)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}

	// Getting multiple keys
	outputE, s := theClient.GetArray([]string{"key9890", "key678"})
	if s.IsError() {
		t.Fatalf(s.ToString())
	}

	if outputE[0] != value1 || outputE[1] != value2 {
		t.Fatalf("Output Strings for GetArray Doesn't match")
	}
}

func TestDelArrayFail(t *testing.T) {
	var theClient *StateClient
	var s common.Status

	if theClient, s = CreateClientForTest(); theClient == nil {
		t.Fatalf(s.ToString())
	}
	defer theClient.DestroyClient()

	// Set a value
	var key1 string = "key569"
	var value1 string = "this is some data"
	var setParam = SetParam{WriteMode: NoneL2Cache}
	s = theClient.Set(key1, value1, setParam)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}

	// Delete multiple keys and key2 is invalid
	outputE, s := theClient.DelArray([]string{"key569", "key22435"})
	if s.IsError() {
		t.Fatalf(s.ToString()) // Delete does not throw an error even if a key is missing
	}

	// Only key1 is deleted
	if len(outputE) != 0 {
		t.Fatalf("Output Count DelArray is not right")
	}
}

func TestGetArrayFail(t *testing.T) {
	var theClient *StateClient
	var s common.Status
	const TimeoutMs uint32 = 5 // Waiting 5ms for the result return if not ready.
	if theClient, s = CreateClientForTest(); theClient == nil {
		t.Fatalf(s.ToString())
	}
	defer theClient.DestroyClient()

	// Set a value
	var key2 string = "key67789"
	var value2 string = "data2"
	var setParam = SetParam{WriteMode: NoneL2Cache}
	s = theClient.Set(key2, value2, setParam)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}

	// Getting multiple keys -> Key1 missing
	outputE, s := theClient.GetArray([]string{"key12637", "key67789"}, TimeoutMs)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}

	if len(outputE[0]) != 0 || outputE[1] != value2 {
		t.Fatalf("Output Strings for GetArray Doesn't match")
	}
}

func TestSetWithoutKey(t *testing.T) {
	var theClient *StateClient
	var s common.Status

	if theClient, s = CreateClientForTest(); theClient == nil {
		t.Fatalf(s.ToString())
	}
	defer theClient.DestroyClient()

	var value2 string = "data2"
	var setParam = SetParam{WriteMode: NoneL2Cache}
	key, s := theClient.SetValue(value2, setParam)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}

	var resultValue string
	resultValue, s = (*theClient).Get(key)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	if value2 != resultValue {
		t.Fatalf("Get is not getting value set by set()")
	}

	// Delete the key
	s = theClient.Del(key)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
}

func TestGenerateKey(t *testing.T) {
	var theClient *StateClient
	var s common.Status

	if theClient, s = CreateClientForTest(); theClient == nil {
		t.Fatalf(s.ToString())
	}
	defer theClient.DestroyClient()

	key := theClient.GenerateKey()
	const generateKeyLen int = 73
	if len(key) != generateKeyLen || !strings.Contains(key, ";") {
		t.Fatalf("Generate key failed")
	}
}

func TestUseClientAfterFree(t *testing.T) {
	var theClient *StateClient
	var s common.Status

	if theClient, s = CreateClientForTest(); theClient == nil {
		t.Fatalf(s.ToString())
	}
	theClient.DestroyClient()

	var value string = "data"
	var setParam = SetParam{WriteMode: NoneL2Cache}
	_, s = theClient.SetValue(value, setParam)
	if s.IsOk() {
		t.Fatalf("SetValue should not return success after DestroyClient")
	}
}

func TestNXSet(t *testing.T) {
	var theClient *StateClient
	var s common.Status
	if theClient, s = CreateClientForTest(); theClient == nil {
		t.Fatalf(s.ToString())
	}
	defer theClient.DestroyClient()
	// Set a value
	var key string = "key_TestNXSet"
	var value string = "data"
	var setParam = SetParam{ExistenceOpt: NX}
	s = theClient.Set(key, value, setParam)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	// Get success
	outputE, s := theClient.GetArray([]string{key})
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	if outputE[0] != value {
		t.Fatalf("Output Strings for GetArray Doesn't match")
	}
	// Set a new value fail
	var value1 string = "data11"
	s = theClient.Set(key, value1, setParam)
	if s.Code != 2004 { // K_OC_KEY_ALREADY_EXIST = 2004
		t.Fatalf(s.ToString())
	}
	s = theClient.Del(key)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
}

func TestNONESet(t *testing.T) {
	var theClient *StateClient
	var s common.Status
	if theClient, s = CreateClientForTest(); theClient == nil {
		t.Fatalf(s.ToString())
	}
	defer theClient.DestroyClient()
	// Set a value
	var key string = "key_TestNONESet"
	var value string = "data"
	var setParam = SetParam{ExistenceOpt: NONE}
	s = theClient.Set(key, value, setParam)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	// Get success
	outputE, s := theClient.GetArray([]string{key})
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	if outputE[0] != value {
		t.Fatalf("Output Strings for GetArray Doesn't match")
	}
	// Set a new value success
	var value1 string = "data1"
	s = theClient.Set(key, value1, setParam)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	// Get success
	outputE, s = theClient.GetArray([]string{key})
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	if outputE[0] != value1 {
		t.Fatalf("Output Strings for GetArray Doesn't match")
	}
	s = theClient.Del(key)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
}

func TestSetTenantId(t *testing.T) {
	var theClient *StateClient
	var s common.Status

	if theClient, s = CreateClientForTest(); theClient == nil {
		t.Fatalf(s.ToString())
	}
	defer theClient.DestroyClient()

	common.Context.SetTenantId("tenant1")

	// Set a value
	var key1 string = "key"
	var value1 string = "this is some data"
	var setParam = SetParam{WriteMode: NoneL2Cache}
	s = theClient.Set(key1, value1, setParam)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}

	// Get the value
	var resultValue string
	resultValue, s = (*theClient).Get(key1)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	if value1 != resultValue {
		t.Fatalf("Get is not getting value set by set(). value1: [%s], resultValue: [%s]", value1, resultValue)
	}

	common.Context.SetTenantId("tenant2")
	resultValue, s = (*theClient).Get(key1)
	if resultValue != "" {
		t.Fatalf("Should not get the key of other tenant")
	}
}
