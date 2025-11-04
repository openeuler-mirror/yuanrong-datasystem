############################################################
# Datasystem targets and config files.
############################################################
set(DATASYSTEM_CONFIG_PATH sdk/cpp/lib/cmake/${PROJECT_NAME})

configure_package_config_file(cmake/config.cmake.in
        ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Config.cmake
        INSTALL_DESTINATION ${DATASYSTEM_CONFIG_PATH}
        NO_CHECK_REQUIRED_COMPONENTS_MACRO)
configure_file(${CMAKE_SOURCE_DIR}/cmake/version.cmake.in ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake @ONLY)

install(EXPORT ${PROJECT_NAME}Targets
        FILE ${PROJECT_NAME}Targets.cmake
        DESTINATION ${DATASYSTEM_CONFIG_PATH})

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Config.cmake
        ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake
        DESTINATION ${DATASYSTEM_CONFIG_PATH})

############################################################
# Datasystem rpc libraries.
############################################################
set(RPC_LIB_PATH "${ZeroMQ_LIB_PATH}/libzmq.so*")
if (BUILD_WITH_URMA)
        list(APPEND RPC_LIB_PATH "${URMA_LIB_LOCATION}/liburma*.so*")
        list(APPEND URMA_LIB_PATH "${URMA_IP_IB_LIB_LOCATION}/liburma_*.so*")
endif()
if (BUILD_WITH_RDMA)
        list(APPEND RPC_LIB_PATH "${UCX_LIB_PATH}/libuc*.so*")
        list(APPEND UCX_LIB_PATH "${UCX_LIB_PATH}/libuc*.so*")
endif()
############################################################
# Datasystem header files and share libraries.
############################################################
set(DATASYSTEM_SDK_USER_INCLUDEDIR sdk/cpp/include)
set(DATASYSTEM_SDK_USER_LIBPATH sdk/cpp/lib)

if (NOT ENABLE_PERF)
    install(DIRECTORY ${CMAKE_SOURCE_DIR}/include/datasystem
            DESTINATION ${DATASYSTEM_SDK_USER_INCLUDEDIR}
            FILES_MATCHING
            PATTERN "*.h"
            PATTERN "perf_client.h" EXCLUDE)
else()
    install(DIRECTORY ${CMAKE_SOURCE_DIR}/include/datasystem
            DESTINATION ${DATASYSTEM_SDK_USER_INCLUDEDIR})
endif ()


set(datasystem_INSTALL_LIBPATH ${DATASYSTEM_SDK_USER_LIBPATH})
install_datasystem_target(datasystem EXPORT_NAME DatasystemTargets)

if (BUILD_HETERO)
    # Set the permission of acl_plugin to 440
    install(TARGETS acl_plugin
            DESTINATION  ${DATASYSTEM_SDK_USER_LIBPATH}
            PERMISSIONS OWNER_READ GROUP_READ)
endif()

set(SDK_SPDLOG_LIB
    "${SPDLOG_LIB_PATH}/libds-spdlog.so*"
)

set(SDK_PROTOBUF_LIB
    "${Protobuf_LIB_PATH}/libprotobuf.so*"
)
set(SDK_PROTOC_LIB
    "${Protobuf_LIB_PATH}/libprotoc.so*"
)

set(SDK_USER_LIB_PATTERNS
    ${SDK_PROTOBUF_LIB}
    ${SDK_PROTOC_LIB})

set(SDK_USER_LIB_PATTERNS
        ${SDK_USER_LIB_PATTERNS}
        ${SDK_SPDLOG_LIB}
       "${SecureC_LIB_PATH}/libsecurec.so"
       "${TBB_LIB_PATH}/libtbb.so*"
       "${gRPC_LIB_PATH}/libgrpc.so*"
       "${gRPC_LIB_PATH}/libgrpc++.so*"
       "${gRPC_LIB_PATH}/libgpr.so*"
       "${gRPC_LIB_PATH}/libupb*"
       "${gRPC_LIB_PATH}/libutf8*"
       "${gRPC_LIB_PATH}/libaddress_sorting.so*"
       "${OpenSSL_LIB_PATH}/libssl.so*"
       "${OpenSSL_LIB_PATH}/libcrypto.so*"
       ${RPC_LIB_PATH}
)

install_file_pattern(
        PATH_PATTERN ${SDK_USER_LIB_PATTERNS}
        DEST_DIR ${DATASYSTEM_SDK_USER_LIBPATH}
)

############################################################
# Datasystem python share libraries.
############################################################
if (BUILD_PYTHON_API)
    # install target first to change install rpath to $ORIGIN
    set(DEPEND_TARGETS
            datasystem
            ds_client_py)

    if (BUILD_HETERO)
        list(APPEND DEPEND_TARGETS acl_plugin)
    endif()

    set(PYTHON_LIBPATH ${CMAKE_BINARY_DIR}/python_lib)
    # Install third party libraries we need.
    set(PYTHON_LIB_PATTERNS
            ${SDK_SPDLOG_LIB}
            ${SDK_PROTOBUF_LIB}
            "${SecureC_LIB_PATH}/libsecurec.so"
            "${TBB_LIB_PATH}/libtbb.so*"
            "${OpenSSL_LIB_PATH}/libssl.so*"
            ${RPC_LIB_PATH}
    )

    package_python(datasystem
            PYTHON_SRC_DIR ${CMAKE_SOURCE_DIR}/python
            CMAKE_INSTALL_PATH ${CMAKE_INSTALL_PREFIX}/sdk
            DEPEND_TARGETS ${DEPEND_TARGETS}
            THIRDPATRY_LIBS_PATTERN ${PYTHON_LIB_PATTERNS})
endif ()

############################################################
# Datasystem go header files and share libraries.
############################################################
if (BUILD_GO_API)
    set(DATASYSTEM_GO_INCLUDEDIR sdk/go/include)
    set(DATASYSTEM_GO_LIBPATH sdk/go/lib)
    set(DATASYSTEM_GO_PATH sdk)

    install(DIRECTORY ${CMAKE_SOURCE_DIR}/go
            DESTINATION ${DATASYSTEM_GO_PATH}
            FILES_MATCHING PATTERN "*")

    install(FILES ${CMAKE_SOURCE_DIR}/src/datasystem/c_api/status_definition.h
            ${CMAKE_SOURCE_DIR}/src/datasystem/c_api/state_cache_c_wrapper.h
            ${CMAKE_SOURCE_DIR}/src/datasystem/c_api/stream_cache_c_wrapper.h
            ${CMAKE_SOURCE_DIR}/src/datasystem/c_api/object_cache_c_wrapper.h
            ${CMAKE_SOURCE_DIR}/src/datasystem/c_api/utilC.h
            ${CMAKE_SOURCE_DIR}/src/datasystem/c_api/cipher.h
            DESTINATION ${DATASYSTEM_GO_INCLUDEDIR}/datasystem/c_api)

    set(datasystem_c_INSTALL_LIBPATH ${DATASYSTEM_GO_LIBPATH})
    install_datasystem_target(datasystem_c)

    set(datasystem_INSTALL_LIBPATH ${DATASYSTEM_GO_LIBPATH})
    install_datasystem_target(datasystem)

    set(GO_LIB_PATTERNS
            ${SDK_SPDLOG_LIB}
            ${SDK_PROTOBUF_LIB}
            ${SDK_PROTOC_LIB}
           "${SecureC_LIB_PATH}/libsecurec.so"
           "${TBB_LIB_PATH}/libtbb.so*"
           "${OpenSSL_LIB_PATH}/libssl.so*"
           "${OpenSSL_LIB_PATH}/libcrypto.so*"
           "${gRPC_LIB_PATH}/libgrpc.so*"
           "${gRPC_LIB_PATH}/libgrpc++.so*"
           "${gRPC_LIB_PATH}/libgpr.so*"
           "${gRPC_LIB_PATH}/libupb*"
           "${gRPC_LIB_PATH}/libutf8*"
           "${gRPC_LIB_PATH}/libaddress_sorting.so*"
           "${SPDLOG_LIB_PATH}/libds-spdlog.so*"
           ${RPC_LIB_PATH}
    )

    install_file_pattern(
            PATH_PATTERN ${GO_LIB_PATTERNS}
            DEST_DIR ${DATASYSTEM_GO_LIBPATH}
    )
endif ()



############################################################
# Datasystem bin and depends libs.
############################################################
set(DATASYSTEM_SERVICE_BINPATH service)
set(DATASYSTEM_SERVICE_LIBPATH service/lib)

set(datasystem_worker_INSTALL_BINPATH ${DATASYSTEM_SERVICE_BINPATH})
install_datasystem_target(datasystem_worker)

set(SERVICE_LIB_PATTERNS
     ${SDK_SPDLOG_LIB}
     ${SDK_PROTOBUF_LIB}
     ${SDK_PROTOC_LIB}
    "${SecureC_LIB_PATH}/libsecurec.so"
    "${TBB_LIB_PATH}/libtbb.so*"
    "${OpenSSL_LIB_PATH}/libssl.so*"
    "${OpenSSL_LIB_PATH}/libcrypto.so*"
    "${CURL_LIB_PATH}/libcurl.so*"
    "${JemallocShared_LIB_PATH}/libjemalloc.so*"
    "${gRPC_LIB_PATH}/libgrpc.so*"
    "${gRPC_LIB_PATH}/libgrpc++.so*"
    "${gRPC_LIB_PATH}/libgpr.so*"
    "${gRPC_LIB_PATH}/libupb*"
    "${gRPC_LIB_PATH}/libutf8*"
    "${gRPC_LIB_PATH}/libaddress_sorting.so*"
    "${OBS_LIB_PATH}/libeSDKLogAPI.so"
    "${OBS_LIB_PATH}/libeSDKOBS.so"
    "${OBS_LIB_PATH}/libiconv.so*"
    "${OBS_LIB_PATH}/libpcre.so*"
    "${OBS_LIB_PATH}/libxml2.so*"
    ${RPC_LIB_PATH}
    "${OBS_LIB_PATH}/libcjson.so*"
)

install_file_pattern(
        PATH_PATTERN ${SERVICE_LIB_PATTERNS}
        DEST_DIR ${DATASYSTEM_SERVICE_LIBPATH}
)

if (BUILD_WITH_URMA)
    install_file_pattern(
        PATH_PATTERN ${URMA_LIB_PATH}
        DEST_DIR ${DATASYSTEM_SERVICE_LIBPATH}/urma
        PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ
    )
endif()

if (BUILD_WITH_RDMA)
    install_file_pattern(
        PATH_PATTERN ${UCX_LIB_PATH}
        DEST_DIR ${DATASYSTEM_SERVICE_LIBPATH}/rdma
        PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ
    )
endif()
############################################################
# Datasystem deploy scripts generate zone.
############################################################
if (BUILD_PYTHON_API)
    # install target first to change install rpath to $ORIGIN
    set(DEPEND_TARGETS
        datasystem
        ds_client_py)

        if (BUILD_HETERO)
            list(APPEND DEPEND_TARGETS acl_plugin)
        endif()

    set(PYTHON_LIBPATH ${CMAKE_BINARY_DIR}/python_lib)

    set(PYTHON_LIB_PATTERNS
            ${SDK_SPDLOG_LIB}
            ${SDK_PROTOBUF_LIB}
           "${SecureC_LIB_PATH}/libsecurec.so"
           "${TBB_LIB_PATH}/libtbb.so*"
           "${CURL_LIB_PATH}/libcurl.so*"
           ${RPC_LIB_PATH}
    )

    package_datasystem_wheel(datasystem
        CMAKE_INSTALL_PATH ${CMAKE_INSTALL_PREFIX}
        DEPEND_TARGETS ${DEPEND_TARGETS}
        THIRDPATRY_LIBS_PATTERN ${PYTHON_LIB_PATTERNS})
endif()

