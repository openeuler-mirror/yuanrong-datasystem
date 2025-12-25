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

// Package common is a common package that provides the Context and handling
package common

/*
#cgo CFLAGS: -I../include/
#cgo LDFLAGS: -L../lib -ldatasystem_c
#include <stdlib.h>
#include <string.h>
#include "datasystem/c_api/utilC.h"
*/
import "C"

import (
	"fmt"
	"unsafe"
)

type dsContext struct {
}

// Context is a global instance to manage trace id.
var Context = new(dsContext)

// Set trace id for all API calls of the current thread
func (c *dsContext) SetTraceID(traceID string) Status {
	cTraceID := C.CString(traceID)
	defer C.free(unsafe.Pointer(cTraceID))
	traceIDLen := C.size_t(len(traceID))

	statusC := C.ContextSetTraceId(cTraceID, traceIDLen)

	if int(statusC.code) != 0 {
		return CreateStatus(int(statusC.code),
			fmt.Sprintf("failed to set trace id. msg: %s", C.GoString(&statusC.errMsg[0])))
	}
	return CreateStatus(0, "")
}

// Set tenantId for all API calls of the current thread. There is no impact in token authentication scenario.
func (c *dsContext) SetTenantId(tenantId string) {
	cTenantId := C.CString(tenantId)
	defer C.free(unsafe.Pointer(cTenantId))
	tenantIdLen := C.size_t(len(tenantId))

	C.ContextSetTenantId(cTenantId, tenantIdLen)
}
