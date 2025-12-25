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

// Package util is the tools for test.
package util

import (
	"fmt"
	"os"
	"strconv"

	"clients/common"
)

// getWorkerAddrFromEnv is a helper function to fetch the worker ip address and the port number from the environment
func getWorkerAddrFromEnv() (string, int, common.Status) {
	workerAddr := os.Getenv("WORKER_ADDR")
	workerPortStr := os.Getenv("WORKER_PORT")
	const INVALID int = 2 // same as C++ K_INVALID=2
	if len(workerAddr) == 0 || len(workerPortStr) == 0 {
		return "", -1, common.CreateStatus(INVALID, "WORKER_ADDR or WORKER_PORT env variables need to be set!")
	}
	workerPort, err := strconv.Atoi(workerPortStr)
	if err != nil {
		return "", -1, common.CreateStatus(INVALID, "WORKER_PORT env variable was set but had invalid value!")
	}
	return workerAddr, workerPort, common.CreateStatus(0, "")
}

// ConstructConnectArguments is a helper function to construct ConnectArguments for client.
func ConstructConnectArguments() (common.ConnectArguments, common.Status) {
	var workerAddr string
	var workerPort int
	var s common.Status
	if workerAddr, workerPort, s = getWorkerAddrFromEnv(); s.IsError() {
		return common.ConnectArguments{}, s
	}
	fmt.Printf("Demo will be using worker host %s on port %d\n", workerAddr, workerPort)
	var connectParameters = common.ConnectArguments{}
	connectParameters.Host = workerAddr
	connectParameters.Port = workerPort
	return connectParameters, common.CreateStatus(0, "")
}
