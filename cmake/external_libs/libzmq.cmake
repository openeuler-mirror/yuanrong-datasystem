# Build the libs cppzmq depends
# libsodium
set(libsodium_VERSION 1.0.18)
if (NOT "$ENV{DS_LOCAL_LIBS_DIR}" STREQUAL "")
  set(libsodium_URL "$ENV{DS_LOCAL_LIBS_DIR}/opensource_third_party/1.0.18-RELEASE.zip")
else()
  set(libsodium_URL "https://gitee.com/mirrors/libsodium/repository/archive/1.0.18-RELEASE.zip")
endif()
set(libsodium_SHA256 "682cb74c39915074bfe08abc7bdcf8c6c34325e35ad746d672b8a9e951d3d1c2")

set(libsodium_CONF_OPTIONS
    --enable-shared=false 
    --disable-pie)

set(libsodium_C_FLAGS ${THIRDPARTY_SAFE_FLAGS})

add_thirdparty_lib(Sodium
  URL ${libsodium_URL}
  SHA256 ${libsodium_SHA256}
  FAKE_SHA256 ${libsodium_FAKE_SHA256}
  VERSION ${libsodium_VERSION}
  CONF_OPTIONS ${libsodium_CONF_OPTIONS}
  C_FLAGS ${libsodium_C_FLAGS}
  TOOLCHAIN configure
  EXTRA_MSGS ${DS_OPENSOURCE_DIR})

# libzmq
set(libzmq_VERSION 4.3.5)
if (NOT "$ENV{DS_LOCAL_LIBS_DIR}" STREQUAL "")
  set(libzmq_URL "$ENV{DS_LOCAL_LIBS_DIR}/opensource_third_party/v4.3.5.zip")
else()
  set(libzmq_URL "https://gitee.com/mirrors/libzmq/repository/archive/v4.3.5.zip")
endif()
set(libzmq_SHA256 "e9a5db55f88f7214614b7e334d74ac501ad0726bcc54241205b7f4b11f132744")

set(ENV{PKG_CONFIG_PATH} "${Sodium_ROOT}/lib/pkgconfig")
set(libzmq_CMAKE_OPTIONS
    -DCMAKE_BUILD_TYPE:STRING=Release
    -DZMQ_BUILD_TESTS:BOOL=OFF
    -DENABLE_CURVE:BOOL=ON
    -DWITH_LIBSODIUM:BOOL=ON
    -DWITH_LIBSODIUM_STATIC:BOOL=ON)

set(libzmq_CXX_FLAGS ${THIRDPARTY_SAFE_FLAGS})

set(libzmq_C_FLAGS ${THIRDPARTY_SAFE_FLAGS})

# compiler flags that are common across debug/release builds:
if (USE_SANITIZER)
  string(TOUPPER ${USE_SANITIZER} USE_SANITIZER)
  if (${USE_SANITIZER} STREQUAL "ADDRESS")
      set(libzmq_CMAKE_OPTIONS "${libzmq_CMAKE_OPTIONS} -DENABLE_ASAN:BOOL=ON")
      set(libzmq_C_FLAGS "${libzmq_C_FLAGS} -g3")
      set(libzmq_CXX_FLAGS "${libzmq_CXX_FLAGS} -g3")
  endif()
endif ()

set(libzmq_EXTRA_MSGS ${Sodium_ROOT})

add_thirdparty_lib(ZeroMQ
  URL ${libzmq_URL}
  SHA256 ${libzmq_SHA256}
  FAKE_SHA256 ${libzmq_FAKE_SHA256}
  VERSION ${libzmq_VERSION}
  CONF_OPTIONS ${libzmq_CMAKE_OPTIONS}
  CXX_FLAGS ${libzmq_CXX_FLAGS}
  C_FLAGS ${libzmq_C_FLAGS}
  EXTRA_MSGS ${libzmq_EXTRA_MSGS})

set(ZeroMQ_ROOT ${ZeroMQ_ROOT})
find_package(ZeroMQ ${libzmq_VERSION} REQUIRED)
include_directories(${ZeroMQ_INCLUDE_DIR})

function(GENERATE_ZMQ_CPP ZMQ_PROTO_LIB_DEPEND source_files header_files target_directory proto_src_directory)
    if (NOT ARGN)
        message(SEND_ERROR "Error: ZMQ_GENERATE_CPP() called without any proto files")
        return()
    endif ()

    set(${source_files})
    set(${header_files})
    foreach (file ${ARGN})
        get_filename_component(abs_file ${file} ABSOLUTE)
        get_filename_component(file_name ${file} NAME_WE)
        
        file(RELATIVE_PATH _REL_PATH ${CMAKE_SOURCE_DIR}/src ${abs_file})
        get_filename_component(_REL_DIR ${_REL_PATH} DIRECTORY)
        
        if(_REL_DIR)
          set(_OUTPUT_PREFIX "${target_directory}/${_REL_DIR}")
        else()
          set(_OUTPUT_PREFIX "${target_directory}")
        endif()
        
        file(MAKE_DIRECTORY ${_OUTPUT_PREFIX})

        list(APPEND ${source_files} "${_OUTPUT_PREFIX}/${file_name}.pb.cc")
        list(APPEND ${header_files} "${_OUTPUT_PREFIX}/${file_name}.pb.h")
        list(APPEND ${source_files} "${_OUTPUT_PREFIX}/${file_name}.service.rpc.pb.cc")
        list(APPEND ${header_files} "${_OUTPUT_PREFIX}/${file_name}.service.rpc.pb.h")
        list(APPEND ${source_files} "${_OUTPUT_PREFIX}/${file_name}.stub.rpc.pb.cc")
        list(APPEND ${header_files} "${_OUTPUT_PREFIX}/${file_name}.stub.rpc.pb.h")
        
        set(LD_LIB_PATH "${Protobuf_LIB_PATH}:$ENV{LD_LIBRARY_PATH}")
        add_custom_command(
            OUTPUT "${_OUTPUT_PREFIX}/${file_name}.pb.cc"
            "${_OUTPUT_PREFIX}/${file_name}.pb.h"
            "${_OUTPUT_PREFIX}/${file_name}.service.rpc.pb.cc"
            "${_OUTPUT_PREFIX}/${file_name}.service.rpc.pb.h"
            "${_OUTPUT_PREFIX}/${file_name}.stub.rpc.pb.cc"
            "${_OUTPUT_PREFIX}/${file_name}.stub.rpc.pb.h"
            COMMAND ${CMAKE_COMMAND} -E env LD_LIBRARY_PATH=${LD_LIB_PATH}
                    $<TARGET_FILE:protobuf::protoc>
            ARGS -I ${CMAKE_SOURCE_DIR}/src -I ${proto_src_directory}
                 --zmq_out ${target_directory} 
                 --cpp_out ${target_directory}
                 --plugin=protoc-gen-zmq=$<TARGET_FILE:zmq_plugin> 
                 ${abs_file}
            DEPENDS ${abs_file} "${CMAKE_SOURCE_DIR}/src/datasystem/common/rpc/plugin_generator/zmq_plugin.cpp"
            COMMENT "Running c++ ZMQ compiler on ${file}" VERBATIM)
        add_custom_target(ZMQ_PROTO_LIB_DEPEND_${file_name} DEPENDS
                "${_OUTPUT_PREFIX}/${file_name}.pb.cc"
                "${_OUTPUT_PREFIX}/${file_name}.pb.h"
                "${_OUTPUT_PREFIX}/${file_name}.service.rpc.pb.cc"
                "${_OUTPUT_PREFIX}/${file_name}.service.rpc.pb.h"
                "${_OUTPUT_PREFIX}/${file_name}.stub.rpc.pb.cc"
                "${_OUTPUT_PREFIX}/${file_name}.stub.rpc.pb.h")
        add_dependencies(${ZMQ_PROTO_LIB_DEPEND} ZMQ_PROTO_LIB_DEPEND_${file_name})
    endforeach ()

    set_source_files_properties(${${source_files}} ${${header_files}} PROPERTIES GENERATED TRUE)
    set(${source_files} ${${source_files}} PARENT_SCOPE)
    set(${header_files} ${${header_files}} PARENT_SCOPE)
endfunction()
