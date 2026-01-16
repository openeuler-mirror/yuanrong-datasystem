set(UCX_VERSION 1.18.0)
if ("$ENV{DS_PACKAGE}" STREQUAL "")
    set(UCX_URL "https://gitee.com/mirrors/ucxsource/repository/archive/v1.18.0.zip")
    set(UCX_SHA256 "99b94e14630b9f72044d965166c4b0985d80a9914cb52f015c573e3d27ee9f81")
else()
    gen_thirdparty_pkg(UCX UCX_URL UCX_SHA256 UCX_FAKE_SHA256 UCX_VERSION)
endif()

include(CheckIncludeFile)

check_include_file("rdma/rdma_cma.h" RDMA_CORE_FOUND)

if (RDMA_CORE_FOUND)
    message(STATUS "rdma-core found: rdma/rdma_cma.h header is available.")
else()
    message(FATAL_ERROR "rdma-core not found. Please install rdma-core to proceed.")
endif()

set(UCX_CONF_OPTIONS
        --enable-optimizations
        --with-verbs
        --with-rdmacm
        --enable-mt
        --without-go
        --without-java
        )

set(UCX_C_FLAGS ${THIRDPARTY_SAFE_FLAGS})
set(UCX_LINK_FLAGS "-Wl,-z,now")

set(UCX_AUTOGEN sh autogen.sh)

set(_ORG_LD_FLAGS $ENV{LDFLAGS})
set(ENV{LDFLAGS} "${THIRDPARTY_SAFE_FLAGS} ${_ORG_LD_FLAGS}")

add_thirdparty_lib(UCX
        URL ${UCX_URL}
        SHA256 ${UCX_SHA256}
        FAKE_SHA256 ${UCX_FAKE_SHA256}
        VERSION ${UCX_VERSION}
        CONF_OPTIONS ${UCX_CONF_OPTIONS}
        C_FLAGS ${UCX_C_FLAGS}
        TOOLCHAIN configure
        PRE_CONFIGURE ${UCX_AUTOGEN}
        )

set(UCX_DIR ${UCX_ROOT})
find_package(UCX ${UCX_VERSION} REQUIRED)

add_definitions(-DUSE_RDMA)
if(UCX_FOUND)
    include_directories(${UCX_INCLUDE_DIR})
endif()