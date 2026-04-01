include(FetchContent)
include(ProcessorCount)

set(DOWNLOAD_EXTRACT_TIMESTAMP TRUE)

set(TRANSFER_ENGINE_THIRDPARTY_SAFE_FLAGS
    "-fPIC -D_FORTIFY_SOURCE=2 -O2 -fstack-protector-strong -ffunction-sections -fdata-sections")

if (NOT TRANSFER_ENGINE_OPENSOURCE_DIR)
    if (DEFINED ENV{TRANSFER_ENGINE_OPENSOURCE_DIR})
        set(TRANSFER_ENGINE_OPENSOURCE_DIR $ENV{TRANSFER_ENGINE_OPENSOURCE_DIR})
    elseif(DEFINED ENV{DS_OPENSOURCE_DIR})
        set(TRANSFER_ENGINE_OPENSOURCE_DIR $ENV{DS_OPENSOURCE_DIR}/transfer_engine)
    else()
        string(SHA256 _TRANSFER_ENGINE_TEMP_PATH "${CMAKE_BINARY_DIR}/transfer_engine")
        set(TRANSFER_ENGINE_OPENSOURCE_DIR "/tmp/${_TRANSFER_ENGINE_TEMP_PATH}")
    endif()
endif()
get_filename_component(TRANSFER_ENGINE_OPENSOURCE_DIR "${TRANSFER_ENGINE_OPENSOURCE_DIR}" ABSOLUTE)

message(STATUS "transfer_engine third-party cache: ${TRANSFER_ENGINE_OPENSOURCE_DIR}")

find_program(TRANSFER_ENGINE_PATCH_EXECUTABLE patch)
find_program(TRANSFER_ENGINE_NINJA_EXECUTABLE ninja)

function(_te_exec_command)
    set(options)
    set(one_value_args WORKING_DIRECTORY)
    set(multi_value_args COMMAND)
    cmake_parse_arguments(ARG "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})

    execute_process(
        COMMAND ${ARG_COMMAND}
        WORKING_DIRECTORY ${ARG_WORKING_DIRECTORY}
        RESULT_VARIABLE _RET
    )
    if (NOT _RET EQUAL "0")
        message(FATAL_ERROR "Fail execute command: ${ARG_COMMAND}, error: ${_RET}")
    endif()
endfunction()

function(te_download_lib_pkg LIB_NAME URL SHA256)
    FetchContent_Declare(
        ${LIB_NAME}
        URL ${URL}
        URL_HASH SHA256=${SHA256}
    )
    FetchContent_GetProperties(${LIB_NAME})
    if (NOT ${LIB_NAME}_POPULATED)
        message(STATUS "Download ${LIB_NAME} from ${URL}")
        FetchContent_Populate(${LIB_NAME})
    endif()
    set(${LIB_NAME}_SOURCE_DIR "${${LIB_NAME}_SOURCE_DIR}" PARENT_SCOPE)
    set(${LIB_NAME}_BINARY_DIR "${${LIB_NAME}_BINARY_DIR}" PARENT_SCOPE)
endfunction()

function(TE_ADD_THIRDPARTY_LIB LIB_NAME)
    set(options)
    set(one_value_args URL SHA256 VERSION TOOLCHAIN CONF_PATH)
    set(multi_value_args CONF_OPTIONS PATCHES CXX_FLAGS C_FLAGS LINK_FLAGS EXTRA_MSGS)
    cmake_parse_arguments(ARG "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})

    string(TOLOWER "${LIB_NAME}" _LIB_NAME_LOWER)

    if (NOT ARG_TOOLCHAIN)
        set(ARG_TOOLCHAIN "cmake")
    endif()
    
    if (NOT TRANSFER_ENGINE_BUILD_THREAD_NUM)
      set(TRANSFER_ENGINE_BUILD_THREAD_NUM 8)
    endif()
    
    set(${LIB_NAME}_CONF_TXT "${_LIB_NAME_LOWER}_${ARG_SHA256}_${ARG_VERSION}_${ARG_TOOLCHAIN}")
    set(${LIB_NAME}_CONF_TXT "${${LIB_NAME}_CONF_TXT}_${ARG_CONF_OPTIONS}")
    set(${LIB_NAME}_CONF_TXT "${${LIB_NAME}_CONF_TXT}_${CMAKE_CXX_COMPILER_VERSION}_${CMAKE_C_COMPILER_VERSION}")
    set(${LIB_NAME}_CONF_TXT "${${LIB_NAME}_CONF_TXT}_${ARG_CXX_FLAGS}_${ARG_C_FLAGS}_${ARG_LINK_FLAGS}")
    foreach(_PATCH ${ARG_PATCHES})
        file(SHA256 "${_PATCH}" _PATCH_SHA256)
        set(${LIB_NAME}_CONF_TXT "${${LIB_NAME}_CONF_TXT}_${_PATCH_SHA256}")
    endforeach()
    foreach(_MSG ${ARG_EXTRA_MSGS})
        set(${LIB_NAME}_CONF_TXT "${${LIB_NAME}_CONF_TXT}_${_MSG}")
    endforeach()
    string(REPLACE ";" "_" ${LIB_NAME}_CONF_TXT "${${LIB_NAME}_CONF_TXT}")
    string(SHA256 _ROOT_SUFFIX "${${LIB_NAME}_CONF_TXT}")
    set(${LIB_NAME}_ROOT "${TRANSFER_ENGINE_OPENSOURCE_DIR}/${_LIB_NAME_LOWER}_${_ROOT_SUFFIX}")

    set(_VERIFY_FILE "${${LIB_NAME}_ROOT}/${LIB_NAME}_install.txt")
    if (EXISTS "${_VERIFY_FILE}")
        message(STATUS "${LIB_NAME} found in ${${LIB_NAME}_ROOT}")
        if (EXISTS "${${LIB_NAME}_ROOT}/lib64")
            set(${LIB_NAME}_LIB_PATH "${${LIB_NAME}_ROOT}/lib64")
        else()
            set(${LIB_NAME}_LIB_PATH "${${LIB_NAME}_ROOT}/lib")
        endif()
        set(${LIB_NAME}_ROOT "${${LIB_NAME}_ROOT}" PARENT_SCOPE)
        set(${LIB_NAME}_LIB_PATH "${${LIB_NAME}_LIB_PATH}" PARENT_SCOPE)
        return()
    endif()

    if (EXISTS "${${LIB_NAME}_ROOT}")
        file(REMOVE_RECURSE "${${LIB_NAME}_ROOT}")
    endif()

    te_download_lib_pkg(${_LIB_NAME_LOWER} "${ARG_URL}" "${ARG_SHA256}")

    foreach(_PATCH ${ARG_PATCHES})
        if (NOT TRANSFER_ENGINE_PATCH_EXECUTABLE)
            message(FATAL_ERROR "patch executable not found!")
        endif()
        execute_process(
            COMMAND ${TRANSFER_ENGINE_PATCH_EXECUTABLE} --verbose -p1 INPUT_FILE "${_PATCH}"
            WORKING_DIRECTORY "${${_LIB_NAME_LOWER}_SOURCE_DIR}"
            RESULT_VARIABLE _RET
        )
        if (NOT _RET EQUAL "0")
            message(FATAL_ERROR "Patch ${_PATCH} failed, error: ${_RET}")
        endif()
    endforeach()

    message(STATUS "Compiling ${LIB_NAME} in ${${_LIB_NAME_LOWER}_BINARY_DIR}")
    string(TOLOWER "${ARG_TOOLCHAIN}" _TOOLCHAIN_LOWER)

    if (_TOOLCHAIN_LOWER STREQUAL "cmake")
        if (ARG_CXX_FLAGS)
            list(APPEND ARG_CONF_OPTIONS "-DCMAKE_CXX_FLAGS=${ARG_CXX_FLAGS}")
        endif()
        if (ARG_C_FLAGS)
            list(APPEND ARG_CONF_OPTIONS "-DCMAKE_C_FLAGS=${ARG_C_FLAGS}")
        endif()
        if (ARG_LINK_FLAGS)
            list(APPEND ARG_CONF_OPTIONS "-DCMAKE_SHARED_LINKER_FLAGS=${ARG_LINK_FLAGS}")
        endif()
        list(APPEND ARG_CONF_OPTIONS "-DCMAKE_INSTALL_PREFIX=${${LIB_NAME}_ROOT}")
        list(APPEND ARG_CONF_OPTIONS "-DCMAKE_POSITION_INDEPENDENT_CODE=ON")
        if (CMAKE_GENERATOR)
            list(APPEND _GENERATOR_ARGS -G "${CMAKE_GENERATOR}")
        elseif(TRANSFER_ENGINE_NINJA_EXECUTABLE)
            list(APPEND _GENERATOR_ARGS -G Ninja)
        endif()

        _te_exec_command(
            COMMAND ${CMAKE_COMMAND} ${_GENERATOR_ARGS} ${ARG_CONF_OPTIONS} "${${_LIB_NAME_LOWER}_SOURCE_DIR}/${ARG_CONF_PATH}"
            WORKING_DIRECTORY "${${_LIB_NAME_LOWER}_BINARY_DIR}"
        )

        _te_exec_command(
            COMMAND ${CMAKE_COMMAND} --build . --target install -- -j${TRANSFER_ENGINE_BUILD_THREAD_NUM}
            WORKING_DIRECTORY "${${_LIB_NAME_LOWER}_BINARY_DIR}"
        )
    else()
        message(FATAL_ERROR "Unrecognized toolchain: ${ARG_TOOLCHAIN}")
    endif()

    file(WRITE "${_VERIFY_FILE}" "${${LIB_NAME}_CONF_TXT}")

    if (EXISTS "${${LIB_NAME}_ROOT}/lib64")
        set(${LIB_NAME}_LIB_PATH "${${LIB_NAME}_ROOT}/lib64")
    else()
        set(${LIB_NAME}_LIB_PATH "${${LIB_NAME}_ROOT}/lib")
    endif()
    set(${LIB_NAME}_ROOT "${${LIB_NAME}_ROOT}" PARENT_SCOPE)
    set(${LIB_NAME}_LIB_PATH "${${LIB_NAME}_LIB_PATH}" PARENT_SCOPE)
endfunction()
