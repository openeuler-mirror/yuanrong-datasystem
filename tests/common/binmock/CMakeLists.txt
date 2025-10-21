add_library(binmock STATIC function_stub.cpp binmock.cpp jmp_code.cpp)
target_link_libraries(binmock GTest::gmock)
set(MOCK_COMPILE_OPTIONS -U_FORTIFY_SOURCE -D_FORTIFY_SOURCE=0 -O0 -g -fPIC -fno-omit-frame-pointer -fno-stack-protector)
target_compile_options(binmock BEFORE PRIVATE ${MOCK_COMPILE_OPTIONS})

add_executable(binmock_spec test_binmock.cpp)
target_link_libraries(binmock_spec PRIVATE binmock GTest::gtest GTest::gmock GTest::gmock_main)
target_compile_options(binmock_spec BEFORE PRIVATE ${MOCK_COMPILE_OPTIONS})
add_datasystem_test(binmock_spec)