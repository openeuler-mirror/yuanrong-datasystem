############################################################
# Datasystem targets and config files.
############################################################
set(DATASYSTEM_CONFIG_PATH datasystem/sdk/cpp/lib/cmake/${PROJECT_NAME})

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

############################################################
# Datasystem header files and share libraries.
############################################################
set(DATASYSTEM_SDK_USER_INCLUDEDIR datasystem/sdk/cpp/include)
set(DATASYSTEM_SDK_USER_LIBPATH datasystem/sdk/cpp/lib)
set(DATASYSTEM_BAZEL_LIBPATH datasystem/sdk/cpp)
set(DATASYSTEM_SDK_USER_NEW_INCLUDEDIR cpp/include)
set(DATASYSTEM_SDK_USER_NEW_LIBPATH cpp/lib)
set(DATASYSTEM_BAZEL_NEW_LIBPATH cpp)

if (NOT ENABLE_PERF)
    install(DIRECTORY ${CMAKE_SOURCE_DIR}/include/datasystem
            DESTINATION ${DATASYSTEM_SDK_USER_INCLUDEDIR}
            FILES_MATCHING
            PATTERN "*.h"
            PATTERN "perf_client.h" EXCLUDE)

    install(DIRECTORY ${CMAKE_SOURCE_DIR}/include/datasystem
            DESTINATION ${DATASYSTEM_SDK_USER_NEW_INCLUDEDIR}
            FILES_MATCHING
            PATTERN "*.h"
            PATTERN "perf_client.h" EXCLUDE)
else()
    install(DIRECTORY ${CMAKE_SOURCE_DIR}/include/datasystem
            DESTINATION ${DATASYSTEM_SDK_USER_INCLUDEDIR})

    install(DIRECTORY ${CMAKE_SOURCE_DIR}/include/datasystem
            DESTINATION ${DATASYSTEM_SDK_USER_NEW_INCLUDEDIR})
endif ()


set(datasystem_INSTALL_LIBPATH ${DATASYSTEM_SDK_USER_LIBPATH})
install_datasystem_target(datasystem EXPORT_NAME DatasystemTargets)
set(datasystem_INSTALL_LIBPATH ${DATASYSTEM_SDK_USER_NEW_LIBPATH})
install_datasystem_target(datasystem)

if (BUILD_HETERO AND BUILD_HETERO_NPU)
    # Set the permission of acl_plugin to 440
    install(TARGETS acl_plugin
            DESTINATION  ${DATASYSTEM_SDK_USER_LIBPATH}
            PERMISSIONS OWNER_READ GROUP_READ)
    install(TARGETS acl_plugin
            DESTINATION  ${DATASYSTEM_SDK_USER_NEW_LIBPATH}
            PERMISSIONS OWNER_READ GROUP_READ)
endif()

set(etcdapi_proto_INSTALL_LIBPATH ${DATASYSTEM_SDK_USER_LIBPATH})
if (BUILD_HETERO AND BUILD_HETERO_GPU)
    # Set the permission of cuda_plugin to 440
    install(TARGETS cuda_plugin
            DESTINATION  ${DATASYSTEM_SDK_USER_LIBPATH}
            PERMISSIONS OWNER_READ GROUP_READ)
    install(TARGETS cuda_plugin
            DESTINATION  ${DATASYSTEM_SDK_USER_NEW_LIBPATH}
            PERMISSIONS OWNER_READ GROUP_READ)
endif()

set(ds_router_client_INSTALL_LIBPATH ${DATASYSTEM_SDK_USER_LIBPATH})
install_datasystem_target(etcdapi_proto EXPORT_NAME DatasystemTargets)
install_datasystem_target(ds_router_client EXPORT_NAME DatasystemTargets)

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
       "${CURL_LIB_PATH}/libcurl.so*"
       "${ICONV_LIB_PATH}/libiconv.so*"
       "${PCRE_LIB_PATH}/libpcre.so*"
       "${XML2_LIB_PATH}/libxml2.so*"
       "${CJSON_LIB_PATH}/libcjson.so*"
       "${absl_LIB_PATH}/libabseil_dll*"
)

install_file_pattern(
        PATH_PATTERN ${SDK_USER_LIB_PATTERNS}
        DEST_DIR ${DATASYSTEM_SDK_USER_LIBPATH}
)

install_file_pattern(
        PATH_PATTERN ${SDK_USER_LIB_PATTERNS}
        DEST_DIR ${DATASYSTEM_SDK_USER_NEW_LIBPATH}
)

############################################################
# Datasystem python share libraries.
############################################################
if (BUILD_PYTHON_API)
    # install target first to change install rpath to $ORIGIN
    set(DEPEND_TARGETS
            datasystem
            ds_client_py
            rpc_option_protos)

    if (BUILD_HETERO AND BUILD_HETERO_NPU)
        list(APPEND DEPEND_TARGETS acl_plugin)
    endif()
    if (BUILD_HETERO AND BUILD_HETERO_GPU)
        list(APPEND DEPEND_TARGETS cuda_plugin)
    endif()

    set(PYTHON_LIBPATH ${CMAKE_BINARY_DIR}/python_lib)
    # Install third party libraries we need.
    set(PYTHON_LIB_PATTERNS
            ${SDK_SPDLOG_LIB}
            ${SDK_PROTOBUF_LIB}
            "${SecureC_LIB_PATH}/libsecurec.so"
            "${TBB_LIB_PATH}/libtbb.so*"
            "${OpenSSL_LIB_PATH}/libssl.so*"
            "${OpenSSL_LIB_PATH}/libcrypto.so*"
            ${RPC_LIB_PATH}
    )

    package_python(yr/datasystem
            PYTHON_SRC_DIR ${CMAKE_SOURCE_DIR}/python
            CMAKE_INSTALL_PATH ${CMAKE_INSTALL_PREFIX}/datasystem/sdk
            DEPEND_TARGETS ${DEPEND_TARGETS}
            THIRDPATRY_LIBS_PATTERN ${PYTHON_LIB_PATTERNS})
endif ()

############################################################
# Datasystem go header files and share libraries.
############################################################
if (BUILD_GO_API)
    set(DATASYSTEM_GO_INCLUDEDIR datasystem/sdk/go/include)
    set(DATASYSTEM_GO_LIBPATH datasystem/sdk/go/lib)
    set(DATASYSTEM_GO_PATH datasystem/sdk)

    install(DIRECTORY ${CMAKE_SOURCE_DIR}/go
            DESTINATION ${DATASYSTEM_GO_PATH}
            FILES_MATCHING PATTERN "*")

    install(FILES ${CMAKE_SOURCE_DIR}/src/datasystem/c_api/status_definition.h
            ${CMAKE_SOURCE_DIR}/src/datasystem/c_api/kv_client_c_wrapper.h
            ${CMAKE_SOURCE_DIR}/src/datasystem/c_api/stream_client_c_wrapper.h
            ${CMAKE_SOURCE_DIR}/src/datasystem/c_api/object_client_c_wrapper.h
            ${CMAKE_SOURCE_DIR}/src/datasystem/c_api/utilC.h
            DESTINATION ${DATASYSTEM_GO_INCLUDEDIR}/datasystem/c_api)

    set(common_flags_INSTALL_LIBPATH ${DATASYSTEM_GO_LIBPATH})
    install_datasystem_target(common_flags)

    set(etcdapi_proto_INSTALL_LIBPATH ${DATASYSTEM_GO_LIBPATH})
    install_datasystem_target(etcdapi_proto)

    set(rpc_option_protos_INSTALL_LIBPATH ${DATASYSTEM_GO_LIBPATH})
    install_datasystem_target(rpc_option_protos)

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
           "${CURL_LIB_PATH}/libcurl.so*"
           "${gRPC_LIB_PATH}/libgrpc.so*"
           "${gRPC_LIB_PATH}/libgrpc++.so*"
           "${gRPC_LIB_PATH}/libgpr.so*"
           "${gRPC_LIB_PATH}/libupb*"
           "${gRPC_LIB_PATH}/libutf8*"
           "${gRPC_LIB_PATH}/libaddress_sorting.so*"
           "${SPDLOG_LIB_PATH}/libds-spdlog.so*"
           ${RPC_LIB_PATH}
           "${absl_LIB_PATH}/libabseil_dll*"
    )

    install_file_pattern(
            PATH_PATTERN ${GO_LIB_PATTERNS}
            DEST_DIR ${DATASYSTEM_GO_LIBPATH}
    )
endif ()

############################################################
# Datasystem java jar package.
############################################################
if (BUILD_JAVA_API)
    set(DATASYSTEM_JAR_PATH datasystem/sdk)
    install(FILES ${JAR_PACKAGE} DESTINATION ${DATASYSTEM_JAR_PATH})
endif ()

############################################################
# Datasystem bin and depends libs.
############################################################
set(DATASYSTEM_SERVICE_BINPATH datasystem/service)
set(DATASYSTEM_SERVICE_LIBPATH datasystem/service/lib)

set(datasystem_worker_shared_INSTALL_LIBPATH ${DATASYSTEM_SDK_USER_LIBPATH})
set(etcdapi_proto_INSTALL_LIBPATH ${DATASYSTEM_SERVICE_LIBPATH})
set(common_flags_INSTALL_LIBPATH ${DATASYSTEM_SDK_USER_LIBPATH})
install_datasystem_target(etcdapi_proto)
install_datasystem_target(common_flags)
install_datasystem_target(datasystem_worker_shared)
set(common_flags_INSTALL_LIBPATH ${DATASYSTEM_SERVICE_LIBPATH})
set(datasystem_worker_shared_INSTALL_LIBPATH ${DATASYSTEM_SERVICE_LIBPATH})
install_datasystem_target(common_flags)
install_datasystem_target(datasystem_worker_shared)
set(datasystem_worker_shared_INSTALL_LIBPATH ${DATASYSTEM_SDK_USER_NEW_LIBPATH})
set(common_flags_INSTALL_LIBPATH ${DATASYSTEM_SDK_USER_NEW_LIBPATH})
set(etcdapi_proto_INSTALL_LIBPATH ${DATASYSTEM_SDK_USER_NEW_LIBPATH})
install_datasystem_target(etcdapi_proto)
install_datasystem_target(datasystem_worker_shared)
install_datasystem_target(common_flags)
set(datasystem_worker_bin_INSTALL_BINPATH ${DATASYSTEM_SERVICE_BINPATH})
install_datasystem_target(datasystem_worker_bin)

set(rpc_option_protos_INSTALL_LIBPATH ${DATASYSTEM_SDK_USER_LIBPATH})
install_datasystem_target(rpc_option_protos)
set(rpc_option_protos_INSTALL_LIBPATH ${DATASYSTEM_SERVICE_LIBPATH})
install_datasystem_target(rpc_option_protos)
set(rpc_option_protos_INSTALL_LIBPATH ${DATASYSTEM_SDK_USER_NEW_LIBPATH})
install_datasystem_target(rpc_option_protos)

# Install common_metastore_service shared library
set(common_metastore_service_INSTALL_LIBPATH ${DATASYSTEM_SERVICE_LIBPATH})
install_datasystem_target(common_metastore_service)

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
    "${ICONV_LIB_PATH}/libiconv.so*"
    "${PCRE_LIB_PATH}/libpcre.so*"
    "${XML2_LIB_PATH}/libxml2.so*"
    ${RPC_LIB_PATH}
    "${CJSON_LIB_PATH}/libcjson.so*"
    "${absl_LIB_PATH}/libabseil_dll*"
)

install_file_pattern(
        PATH_PATTERN ${SERVICE_LIB_PATTERNS}
        DEST_DIR ${DATASYSTEM_SERVICE_LIBPATH}
)

if (BUILD_WITH_RDMA)
    list(APPEND UCX_BASE_LIB_PATH
                "${UCX_LIB_PATH}/libucp.so*"
                "${UCX_LIB_PATH}/libucs*.so*"
                "${UCX_LIB_PATH}/libuct.so*"
                "${UCX_LIB_PATH}/libucm.so*"
        )

    list(APPEND UCX_IB_LIB_PATH
                "${UCX_LIB_PATH}/libuct_*.so*"
                "${UCX_LIB_PATH}/libucx*.so*"
        )

    install_file_pattern(
                PATH_PATTERN ${UCX_BASE_LIB_PATH}
                DEST_DIR ${DATASYSTEM_SERVICE_LIBPATH}
                PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ
        )

    install_file_pattern(
                PATH_PATTERN ${UCX_IB_LIB_PATH}
                DEST_DIR "${DATASYSTEM_SERVICE_LIBPATH}/ucx"
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
    set(TRANSFER_ENGINE_WHEEL_LIB_TARGETS)
    set(TRANSFER_ENGINE_WHEEL_TOPLEVEL_TARGETS)
    set(TRANSFER_ENGINE_PYTHON_LIB_PATTERNS)

        if (BUILD_HETERO AND BUILD_HETERO_NPU)
            list(APPEND DEPEND_TARGETS acl_plugin)
        endif()
        if (BUILD_HETERO AND BUILD_HETERO_GPU)
            list(APPEND DEPEND_TARGETS cuda_plugin)
        endif()
        if (BUILD_TRANSFER_ENGINE AND TARGET _transfer_engine)
            list(APPEND TRANSFER_ENGINE_WHEEL_TOPLEVEL_TARGETS _transfer_engine)
        endif()
        if (BUILD_TRANSFER_ENGINE AND TARGET p2p_transfer)
            list(APPEND TRANSFER_ENGINE_WHEEL_LIB_TARGETS p2p_transfer)
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
    if (TRANSFER_ENGINE_GLOG_LIB_PATH)
        list(APPEND TRANSFER_ENGINE_PYTHON_LIB_PATTERNS
            "${TRANSFER_ENGINE_GLOG_LIB_PATH}/libglog.so*")
    endif()
    if (TRANSFER_ENGINE_PROTOBUF_LIB_PATH)
        list(APPEND TRANSFER_ENGINE_PYTHON_LIB_PATTERNS
            "${TRANSFER_ENGINE_PROTOBUF_LIB_PATH}/libprotobuf.so*"
            "${TRANSFER_ENGINE_PROTOBUF_LIB_PATH}/libprotoc.so*")
    endif()
    if (TRANSFER_ENGINE_ABSL_LIB_PATH)
        list(APPEND TRANSFER_ENGINE_PYTHON_LIB_PATTERNS
            "${TRANSFER_ENGINE_ABSL_LIB_PATH}/libabsl*.so*")
    endif()
    list(APPEND PYTHON_LIB_PATTERNS ${TRANSFER_ENGINE_PYTHON_LIB_PATTERNS})

    package_datasystem_wheel(datasystem
        CMAKE_INSTALL_PATH ${CMAKE_INSTALL_PREFIX}
        DEPEND_TARGETS ${DEPEND_TARGETS}
        WHEEL_LIB_TARGETS ${TRANSFER_ENGINE_WHEEL_LIB_TARGETS}
        WHEEL_TOPLEVEL_TARGETS ${TRANSFER_ENGINE_WHEEL_TOPLEVEL_TARGETS}
        THIRDPATRY_LIBS_PATTERN ${PYTHON_LIB_PATTERNS})
endif()
