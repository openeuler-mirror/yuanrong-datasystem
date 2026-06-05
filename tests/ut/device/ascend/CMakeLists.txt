# HIXL one-sided RH2D smoke test. Builds only when HIXL C++ API dependencies are discovered.

if (NOT ASCEND_HIXL_FOUND)
    message(STATUS "Skipping hixl_rh2d_smoke_test: Ascend HIXL C++ API dependencies not found.")
    return()
endif()

add_executable(hixl_rh2d_smoke_test
        hixl_rh2d_smoke_test.cpp
        $<TARGET_OBJECTS:_ds_ut_main_obj>)

target_link_libraries(hixl_rh2d_smoke_test PRIVATE
        GTest::gtest
        GTest::gmock
        common_log
        ${ASCEND_HIXL_LIBRARIES}
        pthread
        rt)

add_datasystem_test(hixl_rh2d_smoke_test TEST_ENVIRONMENTS ${TEST_ENVIRONMENT})
