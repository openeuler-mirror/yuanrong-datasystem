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

// Package main implements the main function for a demonstation of the golang state cache client api's
// A running cluster needs to be deployed first.  The host and port of the worker for the client to talk to needs to be
// passed into this program via environment variables WORKER_HOST and WORKER_PORT
package main

import (
	"fmt"
	"os"

	"clients/common"
	"clients/kv"
	"clients/test/util"
)

func setValue(theClient kv.KVClient, key string, value string) {
	// Set a value
	fmt.Printf("Set data: %s for key: %s\n", value, key)
	var setParam = kv.SetParam{}
	setParam.WriteMode = kv.NoneL2Cache
	s := theClient.Set(key, value, setParam)
	if s.IsError() {
		fmt.Println(s)
	}
	fmt.Printf("Set successfully\n")
}

func getValue(theClient kv.KVClient, key string) string {
	// Get the value
	var resultValue string
	fmt.Printf("Fetch the value for key: %s\n", key)
	resultValue, s := theClient.Get(key, 0)
	if s.IsError() {
		fmt.Println(s)
	}
	fmt.Printf("Fetch success.  Value is: %s\n", resultValue)
	return resultValue
}

func delValue(theClient kv.KVClient, key string) {
	// Delete the key
	fmt.Printf("Delete the key: %s\n", key)
	s := theClient.Del(key)
	if s.IsError() {
		fmt.Println(s)
	}
	fmt.Printf("Delete success\n")
}

func getValues(theClient kv.KVClient, keys []string) {
	output, s := theClient.GetArray(keys, 0)
	if s.IsError() {
		fmt.Println(s)
	}

	for index, itr := range output {
		// If key doesn't have value in datasystem golang API returns empty string for the key
		if len(itr) != 0 && index < len(keys) {
			fmt.Printf("Fetch success.  Value of key %s is: %s\n", keys[index], itr)
		}
	}
}

func delValues(theClient kv.KVClient, keys []string) {
	// Deleting multiple keys
	outputD, s := theClient.DelArray(keys)
	if s.IsError() {
		fmt.Println(s)
	}
	fmt.Printf("Fetch success.  Number of keys deleted: %d\n", outputD)
}

// The main function executes the state cache client api demo program.
func main() {
	fmt.Printf("!!! A short demo program for using the state cache client api from Golang !!!\n")
	var connectParameters = common.ConnectArguments{}
	var s common.Status
	if connectParameters, s = util.ConstructConnectArguments(); s.IsError() {
		fmt.Println(s)
		os.Exit(1)
	}
	// Create a state cache client
	fmt.Printf("1. Creating state cache client\n")
	theClient := kv.CreateClient(connectParameters)
	defer theClient.DestroyClient()
	fmt.Printf("Created client successfully\n")

	fmt.Printf("2. Connect to worker\n")
	s = theClient.Init()
	if s.IsError() {
		fmt.Println(s)
		os.Exit(1)
	}
	fmt.Printf("Connected successfully\n")

	fmt.Printf("3. Set, Get, and Delete Key1\n")
	setValue(theClient, "key1", "data1")
	getValue(theClient, "key1")
	delValue(theClient, "key1")

	fmt.Printf("4. Getting and Deleting multiple values using GetArray and DelArray\n")
	fmt.Printf("Setting Key1 and Key2 as data1 and data2\n")
	setValue(theClient, "key1", "data1")
	setValue(theClient, "key2", "data2")

	fmt.Printf("GetArray\n")
	getValues(theClient, []string{"key1", "key2"})
	fmt.Printf("DelArray\n")
	delValues(theClient, []string{"key1", "key2"})
	fmt.Printf("!!! End of the demo program for using the state cache client api from Golang !!!\n")
}
