# using maven build jar package
set(JAVA_API_VERSION ${DATASYSTEM_VERSION})

execute_process(
    COMMAND find ${CMAKE_CURRENT_SOURCE_DIR} -name "*.java" -o -name pom.xml
    OUTPUT_VARIABLE JAVA_SRCS
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET
)

string(REPLACE "\n" ";" JAVA_SRCS_LIST ${JAVA_SRCS})

set(CLIENT_API_JAR ${CMAKE_CURRENT_SOURCE_DIR}/target/datasystem-${JAVA_API_VERSION}.jar)
set(CLIENT_API_JAR_FINAL ${CMAKE_CURRENT_SOURCE_DIR}/target/datasystem-${JAVA_API_VERSION}_${CMAKE_HOST_SYSTEM_PROCESSOR}.jar)

set(JNI_LIB_PATH ${CMAKE_BINARY_DIR}/java/jni_lib)
set(STRIP_GEN_HASH_SCRIPT ${CMAKE_SOURCE_DIR}/cmake/scripts/StripAndGenHash.cmake)

set(SRC_PATH_LIBS
    ${TBB_LIB_PATH}/libtbb.so.*
    ${CURL_LIB_PATH}/libcurl.so*
    ${ZeroMQ_LIB_PATH}/libzmq.so.*
    ${SecureC_LIB_PATH}/libsecurec.so
    ${SPDLOG_LIB_PATH}/libds-spdlog.so.*
    $<TARGET_FILE:client_jni_api>
    $<TARGET_FILE:datasystem>)

list(JOIN SRC_PATH_LIBS "," SRC_PATH_ALL_LIBS)

add_custom_command(OUTPUT ${JNI_LIB_PATH}/libclient_jni_api.so
    COMMAND ${CMAKE_COMMAND} -E remove_directory ${JNI_LIB_PATH}
    COMMAND ${CMAKE_COMMAND}
        -D SRC_PATH_PATTERN=${SRC_PATH_ALL_LIBS}
        -D DST_PATH=${JNI_LIB_PATH}
        -D STRIP_LIB:BOOL=${ENABLE_STRIP}
        -P ${STRIP_GEN_HASH_SCRIPT}
    DEPENDS client_jni_api
)

add_custom_command(OUTPUT ${CLIENT_API_JAR_FINAL}
    COMMAND mvn clean package
            -DskipTests
            -Dapi_version=${JAVA_API_VERSION}
            -Dcmake.build.dir=${CMAKE_BINARY_DIR}
            -Djni.lib.dir=${JNI_LIB_PATH}
            -Dcpu.architecture=${CMAKE_HOST_SYSTEM_PROCESSOR}
            -f ${CMAKE_CURRENT_SOURCE_DIR}/pom.xml
    COMMAND ${CMAKE_COMMAND} -E copy ${CLIENT_API_JAR} ${CLIENT_API_JAR_FINAL}
    DEPENDS ${JNI_LIB_PATH}/libclient_jni_api.so ${JAVA_SRCS_LIST}
)

add_custom_target(mvn_compile ALL DEPENDS ${CLIENT_API_JAR_FINAL})

# test all java api
if (WITH_TESTS)
    set(JAVA_TEST_CLASSES StreamClientTest ObjectClientTest KVClientTest)
    foreach(JAVA_TEST_CLASS ${JAVA_TEST_CLASSES})
        add_test(NAME JavaApiTest.${JAVA_TEST_CLASS}
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
            COMMAND mvn test
                    -Dapi_version=${JAVA_API_VERSION}
                    -Dcmake.build.dir=${CMAKE_BINARY_DIR}
                    -Dtest=${JAVA_TEST_CLASS}
                    -f pom.xml
        )
        set_property(TEST JavaApiTest.${JAVA_TEST_CLASS} PROPERTY
                    ENVIRONMENT
                    "LD_LIBRARY_PATH=${CMAKE_INSTALL_PREFIX}/service/lib:${OpenSSL_LIB_PATH}:${gRPC_LIB_PATH}:$ENV{LD_LIBRARY_PATH}")
        set_tests_properties(JavaApiTest.${JAVA_TEST_CLASS} PROPERTIES LABELS "java st level0")
    endforeach()
endif ()

set(JAR_PACKAGE ${CLIENT_API_JAR_FINAL} PARENT_SCOPE)