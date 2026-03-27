include("${CMAKE_CURRENT_LIST_DIR}/external_libs/glog.cmake")

if (TRANSFER_ENGINE_ENABLE_P2P_THIRD_PARTY AND TRANSFER_ENGINE_BUILD_BUNDLED_P2P_SO
        AND TRANSFER_ENGINE_BUILD_BUNDLED_PROTOBUF)
    include("${CMAKE_CURRENT_LIST_DIR}/external_libs/protobuf.cmake")
endif()

if (TRANSFER_ENGINE_BUILD_TESTS)
    include("${CMAKE_CURRENT_LIST_DIR}/external_libs/gtest.cmake")
endif()

if (TRANSFER_ENGINE_BUILD_PYTHON)
    include("${CMAKE_CURRENT_LIST_DIR}/external_libs/pybind11.cmake")
endif()
