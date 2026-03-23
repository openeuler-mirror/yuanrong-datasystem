cmake_minimum_required(VERSION 3.14)

project(p2p-transferTests LANGUAGES CXX)

include(../cmake/project-is-top-level.cmake)
include(../cmake/folders.cmake)

# ---- Dependencies ----

if(PROJECT_IS_TOP_LEVEL)
  find_package(p2p-transfer REQUIRED)
  enable_testing()
endif()

# ---- Tests ----
find_library(HCCL_LIBRARY hccl HINTS ${Ascend_ROOT}/lib64)

add_executable(p2p-transfer_test source/p2p-transfer_test.cpp)
target_link_libraries(p2p-transfer_test PRIVATE p2p-transfer::p2p-transfer ${HCCL_LIBRARY})
target_compile_features(p2p-transfer_test PRIVATE cxx_std_11)
add_test(NAME p2p-transfer_test COMMAND p2p-transfer_test)

add_executable(p2p-transfer_test_batch source/p2p-transfer_test_batch.cpp)
target_link_libraries(p2p-transfer_test_batch PRIVATE p2p-transfer::p2p-transfer ${HCCL_LIBRARY})
target_compile_features(p2p-transfer_test_batch PRIVATE cxx_std_11)
add_test(NAME p2p-transfer_test_batch COMMAND p2p-transfer_test_batch)

add_executable(p2p-transfer_test_init source/p2p-transfer_test_init.cpp)
target_link_libraries(p2p-transfer_test_init PRIVATE p2p-transfer::p2p-transfer ${HCCL_LIBRARY})
target_compile_features(p2p-transfer_test_init PRIVATE cxx_std_11)
add_test(NAME p2p-transfer_test_init COMMAND p2p-transfer_test_init)

add_executable(p2p-transfer_test_batch_thread source/p2p-transfer_test_batch_thread.cpp)
target_link_libraries(p2p-transfer_test_batch_thread PRIVATE p2p-transfer::p2p-transfer ${HCCL_LIBRARY})
target_compile_features(p2p-transfer_test_batch_thread PRIVATE cxx_std_11)
add_test(NAME p2p-transfer_test_batch_thread COMMAND p2p-transfer_test_batch_thread)

add_executable(p2p-transfer_test_batch_recv source/p2p-transfer_test_batch_recv.cpp)
target_link_libraries(p2p-transfer_test_batch_recv PRIVATE p2p-transfer::p2p-transfer ${HCCL_LIBRARY})
target_compile_features(p2p-transfer_test_batch_recv PRIVATE cxx_std_11)
add_test(NAME p2p-transfer_test_batch_recv COMMAND p2p-transfer_test_batch_recv)

add_executable(p2p-transfer_test_batch_send source/p2p-transfer_test_batch_send.cpp)
target_link_libraries(p2p-transfer_test_batch_send PRIVATE p2p-transfer::p2p-transfer ${HCCL_LIBRARY})
target_compile_features(p2p-transfer_test_batch_send PRIVATE cxx_std_11)
add_test(NAME p2p-transfer_test_batch_send COMMAND p2p-transfer_test_batch_send)

add_executable(p2p-transfer_test_h2d source/p2p-transfer_test_h2d.cpp)
target_link_libraries(p2p-transfer_test_h2d PRIVATE p2p-transfer::p2p-transfer ${HCCL_LIBRARY})
target_compile_features(p2p-transfer_test_h2d PRIVATE cxx_std_11)
add_test(NAME p2p-transfer_test_h2d COMMAND p2p-transfer_test_h2d)

add_executable(p2p-transfer_test_h2d_yr source/p2p-transfer_test_h2d_yr.cpp)
target_link_libraries(p2p-transfer_test_h2d_yr PRIVATE p2p-transfer::p2p-transfer ${HCCL_LIBRARY})
target_compile_features(p2p-transfer_test_h2d_yr PRIVATE cxx_std_11)
add_test(NAME p2p-transfer_test_h2d_yr COMMAND p2p-transfer_test_h2d_yr)

# ---- End-of-file commands ----

add_folders(Test)
