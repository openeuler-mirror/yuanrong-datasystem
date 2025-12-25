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

// Package main implements the main function for a demonstation of the golang object cache client api's
// A running cluster needs to be deployed first.  The host and port of the worker for the client to talk to needs to be
// passed into this program via environment variables WORKER_HOST and WORKER_PORT
package main

import (
	"fmt"
	"os"

	"clients/common"
	"clients/object"
	"clients/test/util"
)

func putValue(theClient object.ObjectClient, objectKey string, value string) {
	// Put a value
	fmt.Printf("Put data: %s for objectKey: %s\n", value, objectKey)
	var putParam = object.PutParam{}
	s := theClient.Put(objectKey, value, putParam)
	if s.IsError() {
		fmt.Println(s)
	}
	fmt.Println("Put success")
}

func getValues(theClient object.ObjectClient, objectKeys []string) {
	// Get the values
	output, s := theClient.Get(objectKeys, 0)
	if s.IsError() {
		fmt.Println(s)
	}

	for index, itr := range output {
		// If objectKey doesn't have value in datasystem golang API returns empty string for the objectKey
		if len(itr) != 0 && index < len(objectKeys) {
			fmt.Printf("Get success.  The value of objectKey %s is: %s\n", objectKeys[index], itr)
		}
	}
}

func gIncreaseRef(theClient object.ObjectClient, objectKeys []string, remoteClientId string) {
	failedObjKeys, s := theClient.GIncreaseRef(objectKeys)
	if s.IsError() {
		fmt.Println(s)
	}
	for index, itr := range failedObjKeys {
		if len(itr) != 0 && index < len(objectKeys) {
			fmt.Printf("GIncreaseRef fail.  The failed objectKey is: %s\n", itr)
		}
	}
	fmt.Printf("GIncreaseRef finish\n")
}

func gDecreaseRef(theClient object.ObjectClient, objectKeys []string, remoteClientId string) {
	failedObjKeys, s := theClient.GDecreaseRef(objectKeys)
	if s.IsError() {
		fmt.Println(s)
	}
	for index, itr := range failedObjKeys {
		if len(itr) != 0 && index < len(objectKeys) {
			fmt.Printf("GDecreaseRef fail.  The failed objectKey is: %s\n", itr)
		}
	}
	fmt.Printf("GDecreaseRef finish\n")
}

func releaseGRefs(theClient object.ObjectClient, remoteClientId string) {
	s := theClient.ReleaseGRefs(remoteClientId)
	if s.IsError() {
		fmt.Println(s)
	}
	fmt.Printf("releaseGRefs success. The value of remoteClientId is: %s\n", remoteClientId)
}

// The main function executes the object cache client api demo program.
func main() {
	fmt.Printf("!!! A short demo program for using the object cache client api from Golang !!!\n")
	var connectParameters = common.ConnectArguments{}
	var s common.Status
	if connectParameters, s = util.ConstructConnectArguments(); s.IsError() {
		fmt.Println(s)
		os.Exit(1)
	}
	// Create a object cache client
	fmt.Printf("1. Creating object cache client\n")
	theClient := object.CreateClient(connectParameters)
	defer theClient.DestroyClient()
	fmt.Printf("Created client successfully\n")

	fmt.Printf("2. Connect to worker\n")
	s = theClient.Init()
	if s.IsError() {
		fmt.Println(s)
		os.Exit(1)
	}
	fmt.Printf("Connected successfully\n")

	var remoteClientId string = "remoteClientId"
	fmt.Printf("3. GIncreaseRef\n")
	gIncreaseRef(theClient, []string{"object1", "object2"}, remoteClientId)

	fmt.Printf("4. Put and Get\n")
	putValue(theClient, "object1", "data1")
	putValue(theClient, "object2", "data2")
	getValues(theClient, []string{"object1", "object2"})

	fmt.Printf("5. GDecreaseRef\n")
	gDecreaseRef(theClient, []string{"object1", "object2"}, remoteClientId)

	fmt.Printf("6. ReleaseGRefs remoteClientId\n")
	releaseGRefs(theClient, remoteClientId)

	fmt.Printf("!!! End of the demo program for using the object cache client api from Golang !!!\n")
}
