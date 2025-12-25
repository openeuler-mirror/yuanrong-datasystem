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
// Testing object package
package object

import (
	"fmt"
	"os"
	"strconv"
	"testing"

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
func CreateClientForTest() (*ObjectClient, common.Status) {
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
func TestObjectPutAndGet(t *testing.T) {
	var theClient *ObjectClient
	var s common.Status

	if theClient, s = CreateClientForTest(); theClient == nil {
		t.Fatalf(s.ToString())
	}
	defer theClient.DestroyClient()

	// Put objectKey1, objectKey2
	objectKeys := []string{"objectKey1", "objectKey2"}
	var value string = "this is some data"
	var putParam PutParam = PutParam{ConsistencyType: Pram}
	for i := 0; i < len(objectKeys); i++ {
		s = theClient.Put(objectKeys[i], value, putParam)
		if s.IsError() {
			t.Fatalf(s.ToString())
		}
	}
	// Get objectKey1, objectKey2
	var timeout int64 = 1000
	outputVals, s := theClient.Get(objectKeys, timeout)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	for i := 0; i < len(objectKeys); i++ {
		if outputVals[i] != value {
			t.Fatalf("Output Strings for Get Doesn't match")
		}
	}
}

func TestObjectRefApi(t *testing.T) {
	var theClient *ObjectClient
	var s common.Status

	if theClient, s = CreateClientForTest(); theClient == nil {
		t.Fatalf(s.ToString())
	}
	defer theClient.DestroyClient()
	// GIncreaseRef
	var objectKey string = "TestObjectRefApi_objectKey"
	faileedobjKeys, s := theClient.GIncreaseRef([]string{objectKey})
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	if len(faileedobjKeys) != 0 {
		t.Fatalf("Output Strings for Get Doesn't match")
	}
	// expect put and get success
	var value string = "this is some data"
	var putParam PutParam = PutParam{ConsistencyType: Pram}
	s = theClient.Put(objectKey, value, putParam)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	var timeout int64 = 1000
	getVals, s := theClient.Get([]string{objectKey}, timeout)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	if getVals[0] != value {
		t.Fatalf("Output Strings for Get Doesn't match")
	}
	// GDecreaseRef
	faileedobjKeys, s = theClient.GDecreaseRef([]string{objectKey})
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	if len(faileedobjKeys) != 0 {
		t.Fatalf("Output Strings for Get Doesn't match")
	}
	// expect get fail
	_, s = theClient.Get([]string{objectKey})
	if s.IsOk() {
		t.Fatalf(s.ToString())
	}
}

func TestObjectRefApiWithRemoteClientId(t *testing.T) {
	var theClient *ObjectClient
	var s common.Status

	if theClient, s = CreateClientForTest(); theClient == nil {
		t.Fatalf(s.ToString())
	}
	defer theClient.DestroyClient()
	// GIncreaseRef with remoteClientId
	var objectKey string = "TestObjectRefApiWithRemoteClientId_objectKey"
	var remoteClientId string = "TestObjectRefApiWithRemoteClientId_remoteClientId"
	faileedobjKeys, s := theClient.GIncreaseRef([]string{objectKey}, remoteClientId)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	if len(faileedobjKeys) != 0 {
		t.Fatalf("Output Strings for Get Doesn't match")
	}
	// expect put and get success
	var value string = "this is some data"
	var putParam PutParam = PutParam{ConsistencyType: Pram}
	s = theClient.Put(objectKey, value, putParam)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	var timeout int64 = 1000
	getVals, s := theClient.Get([]string{objectKey}, timeout)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	if getVals[0] != value {
		t.Fatalf("Output Strings for Get Doesn't match")
	}
	// GDecreaseRef with remoteClientId
	faileedobjKeys, s = theClient.GDecreaseRef([]string{objectKey}, remoteClientId)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	if len(faileedobjKeys) != 0 {
		t.Fatalf("Output Strings for Get Doesn't match")
	}
	// expect get fail
	_, s = theClient.Get([]string{objectKey})
	if s.IsOk() {
		t.Fatalf(s.ToString())
	}
}

func TestObjectReleaseGRefs(t *testing.T) {
	var theClient *ObjectClient
	var s common.Status

	if theClient, s = CreateClientForTest(); theClient == nil {
		t.Fatalf(s.ToString())
	}
	defer theClient.DestroyClient()
	// GIncreaseRef with remoteClientId
	var objectKey string = "TestObjectReleaseGRefs_objectKey1"
	var remoteClientId string = "TestObjectReleaseGRefs_remoteClientId1"
	faileedobjKeys, s := theClient.GIncreaseRef([]string{objectKey}, remoteClientId)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	if len(faileedobjKeys) != 0 {
		t.Fatalf("Output Strings for Get Doesn't match")
	}
	// expect put and get success
	var value string = "this is some data"
	var putParam PutParam = PutParam{ConsistencyType: Pram}
	s = theClient.Put(objectKey, value, putParam)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	var timeout int64 = 1000
	getVals, s := theClient.Get([]string{objectKey}, timeout)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	if getVals[0] != value {
		t.Fatalf("Output Strings for Get Doesn't match")
	}
	// ReleaseGRefs remoteClientId
	s = theClient.ReleaseGRefs(remoteClientId)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	// expect get fail
	_, s = theClient.Get([]string{objectKey})
	if s.IsOk() {
		t.Fatalf(s.ToString())
	}
}

func TestObjectPutParam(t *testing.T) {
	var theClient *ObjectClient
	var s common.Status

	if theClient, s = CreateClientForTest(); theClient == nil {
		t.Fatalf(s.ToString())
	}
	defer theClient.DestroyClient()

	var value string = "this is some data"
	objectKeys := []string{"obj0", "obj1", "obj2"}
	// Put obj1, PutParam: default is NoneL2Cache and Pram
	var putParam PutParam = PutParam{}
	s = theClient.Put(objectKeys[0], value, putParam)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	// Put obj2, PutParam: WriteThroughL2Cache and Causal
	putParam = PutParam{ConsistencyType: Causal}
	s = theClient.Put(objectKeys[1], value, putParam)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	// Put obj3, PutParam: WriteBackL2Cache and Causal
	putParam = PutParam{ConsistencyType: Causal}
	s = theClient.Put(objectKeys[2], value, putParam)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	// Get obj1, obj2, obj3
	outputVals, s := theClient.Get(objectKeys)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	for i := 0; i < len(objectKeys); i++ {
		if outputVals[i] != value {
			t.Fatalf("Output Strings for Get Doesn't match")
		}
	}
}

func TestObjectPutNestedObjectKeys(t *testing.T) {
	var theClient *ObjectClient
	var s common.Status

	if theClient, s = CreateClientForTest(); theClient == nil {
		t.Fatalf(s.ToString())
	}
	defer theClient.DestroyClient()

	var value string = "this is some data"

	nestedObjectKeys := []string{"obj00", "obj11"}
	// Put nestedObjectKeys
	var putParam PutParam = PutParam{}
	s = theClient.Put(nestedObjectKeys[0], value, putParam)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	s = theClient.Put(nestedObjectKeys[1], value, putParam)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	// GIncreaseRef nestedObjectKeys
	var objectKey string = "TestObjectPutNestedObjectKeys"
	faileedobjKeys, s := theClient.GIncreaseRef(nestedObjectKeys)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	if len(faileedobjKeys) != 0 {
		t.Fatalf("Output Strings for Get Doesn't match")
	}
	// Put objectKey with nestedObjectKeys
	s = theClient.Put(objectKey, value, putParam, nestedObjectKeys)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	// GDecreaseRef nestedObjectKeys
	faileedobjKeys, s = theClient.GDecreaseRef(nestedObjectKeys)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	if len(faileedobjKeys) != 0 {
		t.Fatalf("Output Strings for Get Doesn't match")
	}
	// expect to Get nestedObjectKeys success
	outputVals, s := theClient.Get(nestedObjectKeys)
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	for i := 0; i < len(nestedObjectKeys); i++ {
		if outputVals[i] != value {
			t.Fatalf("Output Strings for Get Doesn't match")
		}
	}
}

// Check ObjMetas
func checkObjMetas(objMetas []ObjMetaInfo, objSize []int, locSize []int) common.Status {
	objNum := len(objMetas)
	if objNum != len(objSize) || objNum != len(locSize) {
		return common.CreateStatus(common.InvalidParam, "checkObjMetas input not match")
	}
	for i := 0; i < objNum; i++ {
		if objMetas[i].ObjSize != uint64(objSize[i]) || len(objMetas[i].Locations) != locSize[i] {
			msg := fmt.Sprintf("checkObjMetas: objMetas is wrong, expected is [%d, %d], actually is [%d, %d]",
				objSize[i], locSize[i], objMetas[i].ObjSize, len(objMetas[i].Locations))
			return common.CreateStatus(common.UnexpectedError, msg)
		}
	}
	return common.CreateStatus(common.Ok, "")
}

// A unit test for testing the GetObjMetaInfo api
func TestGetObjMetaInfo(t *testing.T) {
	connectParameters, s := getWorkerForTest()
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	tenantId := "user1"
	connectParameters.TenantID = tenantId
	oc := CreateClient(connectParameters)
	if s = oc.Init(); s.IsError() {
		oc.DestroyClient()
		t.Fatalf("Init function connects the client to the worker failed!")
	}
	defer oc.DestroyClient()

	objectKey := "objGetObjMetaInfo"
	value := "data"
	para := PutParam{ConsistencyType: Pram}
	if s = oc.Put(objectKey, value, para); s.IsError() {
		t.Fatalf(s.ToString())
	}

	var objMetas []ObjMetaInfo
	if objMetas, s = oc.GetObjMetaInfo(tenantId, []string{objectKey}); s.IsError() {
		t.Fatalf(s.ToString())
	}
	if s = checkObjMetas(objMetas, []int{len(value)}, []int{1}); s.IsError() {
		t.Fatalf(s.ToString())
	}

	if objMetas, s = oc.GetObjMetaInfo("user2", []string{objectKey}); s.IsError() {
		t.Fatalf(s.ToString())
	}
	if s = checkObjMetas(objMetas, []int{0}, []int{0}); s.IsError() {
		t.Fatalf(s.ToString())
	}
}

// A unit test for testing the GetObjMetaInfo api
func TestGetObjMetaInfoMulti(t *testing.T) {
	connectParameters, s := getWorkerForTest()
	if s.IsError() {
		t.Fatalf(s.ToString())
	}
	tenantId := "user1"
	connectParameters.TenantID = tenantId
	oc := CreateClient(connectParameters)
	if s = oc.Init(); s.IsError() {
		oc.DestroyClient()
		t.Fatalf("Init function connects the client to the worker failed!")
	}
	defer oc.DestroyClient()

	objectKey1 := "objGetObjMetaInfoMulti1"
	objectKey2 := "objGetObjMetaInfoMulti2"
	value1 := "data"
	value2 := "value"
	para := PutParam{ConsistencyType: Pram}
	if s = oc.Put(objectKey1, value1, para); s.IsError() {
		t.Fatalf(s.ToString())
	}
	if s = oc.Put(objectKey2, value2, para); s.IsError() {
		t.Fatalf(s.ToString())
	}

	var objMetas []ObjMetaInfo
	if objMetas, s = oc.GetObjMetaInfo(tenantId, []string{objectKey1, objectKey2}); s.IsError() {
		t.Fatalf(s.ToString())
	}
	if s = checkObjMetas(objMetas, []int{len(value1), len(value2)}, []int{1, 1}); s.IsError() {
		t.Fatalf(s.ToString())
	}

	if objMetas, s = oc.GetObjMetaInfo(tenantId, []string{objectKey1, "obj3", objectKey2}); s.IsError() {
		t.Fatalf(s.ToString())
	}
	if s = checkObjMetas(objMetas, []int{len(value1), 0, len(value2)}, []int{1, 0, 1}); s.IsError() {
		t.Fatalf(s.ToString())
	}
}
