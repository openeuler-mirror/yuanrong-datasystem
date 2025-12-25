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

import (
	"errors"
	"fmt"
)

// Status encapsulates a return code with the error
type Status struct {
	Code int
	Err  error
}

// IsOk checks status is an success
func (s *Status) IsOk() bool {
	cOK := 0 // same as C++ K_OK=0
	return (s.Code == cOK)
}

// IsError checks status is an error
func (s *Status) IsError() bool {
	return !s.IsOk()
}

// ToString produces a nicely formatted string that contains the error and it's code
func (s *Status) ToString() string {
	return fmt.Sprintf("Status code: %d Error: %v", s.Code, s.Err)
}

// CreateStatus is an helper function to make it more convenient to generate an Status object
func CreateStatus(code int, msg string) Status {
	return Status{
		Err:  errors.New(msg),
		Code: code,
	}
}

// These constants are represented as status codes and are visible within the package.
// The purpose of defining constants is to improve code readability.
// These constant values correspond to C++ status codes.
const (
	// ok same as C++ K_OK=0
	Ok = 0

	// invalidParam same as C++ K_INVALID=2
	InvalidParam = 2

	// same as C++ K_UNKNOWN_ERROR=10
	UnexpectedError = 10
)
