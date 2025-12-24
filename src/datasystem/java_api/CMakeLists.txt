#stream cache
set(CLIENT_JNI_API_SRCS
        stream/org_yuanrong_datasystem_stream_ConsumerImpl.cpp
        stream/org_yuanrong_datasystem_stream_ProducerImpl.cpp
        stream/org_yuanrong_datasystem_stream_StreamClient.cpp
        object/org_yuanrong_datasystem_object_ObjectClient.cpp
        object/org_yuanrong_datasystem_object_BufferImpl.cpp
        kv/org_yuanrong_datasystem_kv_KVClient.cpp
        kv/kv_impl.cpp
        org_yuanrong_datasystem_Context.cpp
        jni_util.cpp
        )

set(CLIENT_JNI_API_DEPEND_LIBS ds_flags datasystem)

add_library(client_jni_api SHARED ${CLIENT_JNI_API_SRCS})
target_include_directories(client_jni_api PUBLIC ${JAVA_INCLUDE_PATH} ${JAVA_INCLUDE_PATH2})
target_link_libraries(client_jni_api PRIVATE ${CLIENT_JNI_API_DEPEND_LIBS})
clean_build_rpath(client_jni_api)