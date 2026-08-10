# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if (NOT DEFINED ELF_FILE OR NOT EXISTS "${ELF_FILE}")
    message(FATAL_ERROR "ELF_FILE does not identify an existing artifact: ${ELF_FILE}")
endif()
if (NOT DEFINED ELF_ROLE)
    set(ELF_ROLE CORE)
endif()

find_program(READELF_EXECUTABLE readelf REQUIRED)
execute_process(
        COMMAND "${READELF_EXECUTABLE}" -d "${ELF_FILE}"
        RESULT_VARIABLE READELF_RESULT
        OUTPUT_VARIABLE DYNAMIC_SECTION
        ERROR_VARIABLE READELF_ERROR)
if (NOT READELF_RESULT EQUAL 0)
    message(FATAL_ERROR "readelf failed for ${ELF_FILE}: ${READELF_ERROR}")
endif()

set(CANN_LIBRARIES libascendcl.so libcann_hixl.so libmetadef.so)
if (ELF_ROLE STREQUAL "CORE")
    foreach(CANN_LIBRARY IN LISTS CANN_LIBRARIES)
        string(FIND "${DYNAMIC_SECTION}" "Shared library: [${CANN_LIBRARY}" FOUND_POSITION)
        if (NOT FOUND_POSITION EQUAL -1)
            message(FATAL_ERROR "Core artifact ${ELF_FILE} directly depends on ${CANN_LIBRARY}")
        endif()
    endforeach()

    find_program(NM_EXECUTABLE nm REQUIRED)
    execute_process(
            COMMAND "${NM_EXECUTABLE}" -D -C --undefined-only "${ELF_FILE}"
            RESULT_VARIABLE NM_RESULT
            OUTPUT_VARIABLE UNDEFINED_SYMBOLS
            ERROR_VARIABLE NM_ERROR)
    if (NOT NM_RESULT EQUAL 0)
        message(FATAL_ERROR "nm failed for ${ELF_FILE}: ${NM_ERROR}")
    endif()
    if (UNDEFINED_SYMBOLS MATCHES "hixl::" OR UNDEFINED_SYMBOLS MATCHES "ge::AscendString")
        message(FATAL_ERROR "Core artifact ${ELF_FILE} contains an undefined CANN C++ ABI symbol")
    endif()
elseif (ELF_ROLE STREQUAL "PLUGIN")
    foreach(CANN_LIBRARY IN LISTS CANN_LIBRARIES)
        string(FIND "${DYNAMIC_SECTION}" "Shared library: [${CANN_LIBRARY}" FOUND_POSITION)
        if (FOUND_POSITION EQUAL -1)
            message(FATAL_ERROR "HIXL plugin ${ELF_FILE} does not depend on ${CANN_LIBRARY}")
        endif()
    endforeach()

    execute_process(
            COMMAND "${READELF_EXECUTABLE}" --dyn-syms --wide "${ELF_FILE}"
            RESULT_VARIABLE SYMBOL_RESULT
            OUTPUT_VARIABLE DYNAMIC_SYMBOLS
            ERROR_VARIABLE SYMBOL_ERROR)
    if (NOT SYMBOL_RESULT EQUAL 0)
        message(FATAL_ERROR "readelf symbol audit failed for ${ELF_FILE}: ${SYMBOL_ERROR}")
    endif()
    if (NOT DYNAMIC_SYMBOLS MATCHES "DsHixlGetApi")
        message(FATAL_ERROR "HIXL plugin ${ELF_FILE} does not export DsHixlGetApi")
    endif()
    string(REGEX MATCHALL "[^\n]*(GLOBAL|WEAK)[^\n]*" EXPORTED_SYMBOL_LINES "${DYNAMIC_SYMBOLS}")
    foreach(SYMBOL_LINE IN LISTS EXPORTED_SYMBOL_LINES)
        if (NOT SYMBOL_LINE MATCHES "UND" AND NOT SYMBOL_LINE MATCHES "DsHixlGetApi")
            message(FATAL_ERROR "HIXL plugin ${ELF_FILE} exposes an unexpected symbol: ${SYMBOL_LINE}")
        endif()
    endforeach()
else()
    message(FATAL_ERROR "Unsupported ELF_ROLE: ${ELF_ROLE}")
endif()
