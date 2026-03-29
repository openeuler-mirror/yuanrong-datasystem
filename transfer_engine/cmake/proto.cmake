function(TE_GENERATE_PROTO_CPP OUT_SRCS OUT_HDRS DEST_DIR)
    set(options)
    set(one_value_args SOURCE_ROOT PROTO_DEPEND)
    set(multi_value_args PROTO_FILES)
    cmake_parse_arguments(ARG "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})

    if (TRANSFER_ENGINE_PROTOC_EXECUTABLE)
        set(_PROTOC_EXECUTABLE "${TRANSFER_ENGINE_PROTOC_EXECUTABLE}")
    elseif (TARGET protobuf::protoc)
        set(_PROTOC_EXECUTABLE $<TARGET_FILE:protobuf::protoc>)
    elseif (Protobuf_PROTOC_EXECUTABLE)
        set(_PROTOC_EXECUTABLE "${Protobuf_PROTOC_EXECUTABLE}")
    else()
        find_program(_PROTOC_EXECUTABLE protoc REQUIRED)
    endif()

    file(MAKE_DIRECTORY "${DEST_DIR}")
    set(_PROTOC_ENV_COMMAND ${CMAKE_COMMAND} -E env)
    if (TRANSFER_ENGINE_PROTOBUF_LIB_PATH)
        list(APPEND _PROTOC_ENV_COMMAND
            "LD_LIBRARY_PATH=${TRANSFER_ENGINE_PROTOBUF_LIB_PATH}:$ENV{LD_LIBRARY_PATH}"
            "DYLD_LIBRARY_PATH=${TRANSFER_ENGINE_PROTOBUF_LIB_PATH}:$ENV{DYLD_LIBRARY_PATH}")
    endif()

    foreach(_PROTO_FILE ${ARG_PROTO_FILES})
        if (IS_ABSOLUTE "${_PROTO_FILE}")
            file(RELATIVE_PATH _PROTO_RELATIVE "${ARG_SOURCE_ROOT}" "${_PROTO_FILE}")
        else()
            set(_PROTO_RELATIVE "${_PROTO_FILE}")
        endif()
        get_filename_component(_PROTO_NAME "${_PROTO_FILE}" NAME_WE)
        get_filename_component(_PROTO_SUBDIR "${_PROTO_RELATIVE}" DIRECTORY)
        set(_PROTO_OUTPUT_DIR "${DEST_DIR}")
        if (_PROTO_SUBDIR)
            set(_PROTO_OUTPUT_DIR "${DEST_DIR}/${_PROTO_SUBDIR}")
        endif()
        set(_PROTO_SRC "${_PROTO_OUTPUT_DIR}/${_PROTO_NAME}.pb.cc")
        set(_PROTO_HDR "${_PROTO_OUTPUT_DIR}/${_PROTO_NAME}.pb.h")
        add_custom_command(
            OUTPUT "${_PROTO_SRC}" "${_PROTO_HDR}"
            COMMAND ${CMAKE_COMMAND} -E make_directory "${_PROTO_OUTPUT_DIR}"
            COMMAND ${CMAKE_COMMAND} -E chdir "${ARG_SOURCE_ROOT}"
                    ${_PROTOC_ENV_COMMAND}
                    ${_PROTOC_EXECUTABLE}
                    --proto_path=.
                    --cpp_out=${DEST_DIR}
                    "${_PROTO_RELATIVE}"
            DEPENDS ${_PROTO_FILE} ${ARG_PROTO_DEPEND}
            VERBATIM
        )
        list(APPEND _PROTO_SRCS "${_PROTO_SRC}")
        list(APPEND _PROTO_HDRS "${_PROTO_HDR}")
    endforeach()

    set(${OUT_SRCS} "${_PROTO_SRCS}" PARENT_SCOPE)
    set(${OUT_HDRS} "${_PROTO_HDRS}" PARENT_SCOPE)
endfunction()
