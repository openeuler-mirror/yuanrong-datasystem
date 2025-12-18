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

// Package common is a common package that provides the Status and handling
package common

// ConnectArguments structure is used to pass parameters while creating a client.
// It takes parameters including it's own host, port, timeout for the connection set with server etc.
// It is sent as parameter while creating a client by calling CreateClient().
type ConnectArguments struct {
	Host                      string
	Port                      int
	TimeoutMs                 int
	ClientPublicKey           string
	ClientPrivateKey          []byte
	ServerPublicKey           string
	AccessKey                 string
	SecretKey                 []byte
	TenantID                  string
	EnableCrossNodeConnection bool
}
