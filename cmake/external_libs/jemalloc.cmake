set(jemalloc_VERSION 5.3.0)
if (NOT "$ENV{DS_LOCAL_LIBS_DIR}" STREQUAL "")
  set(jemalloc_URL "$ENV{DS_LOCAL_LIBS_DIR}/opensource_third_party/5.3.0.zip")
else()
  set(jemalloc_URL "https://gitee.com/mirrors/jemalloc/repository/archive/5.3.0.zip")
endif()
set(jemalloc_SHA256 "fcd383b168b72904f777e500631996366a633d1c40714f1601b7e739eb054613")

set(jemalloc_CONF_OPTIONS
    --with-malloc-conf=narenas:1,background_thread:true,max_background_threads:100,oversize_threshold:107374182400,lg_extent_max_active_fit:63
    --disable-zone-allocator
    --without-export
    --disable-shared
    --enable-static
    --disable-cxx
    --enable-stats
    --disable-initial-exec-tls
    --with-jemalloc-prefix=datasystem_)

set(jemalloc_C_FLAGS ${THIRDPARTY_SAFE_FLAGS})
set(jemalloc_LINK_FLAGS "-Wl,-z,now")

set(jemalloc_AUTOGEN sh autogen.sh)

set(_ORG_LD_FLAGS $ENV{LDFLAGS})
set(ENV{LDFLAGS} "${THIRDPARTY_SAFE_FLAGS} ${_ORG_LD_FLAGS}")

add_thirdparty_lib(Jemalloc
  URL ${jemalloc_URL}
  SHA256 ${jemalloc_SHA256}
  FAKE_SHA256 ${jemalloc_FAKE_SHA256}
  VERSION ${jemalloc_VERSION}
  CONF_OPTIONS ${jemalloc_CONF_OPTIONS}
  C_FLAGS ${jemalloc_C_FLAGS}
  TOOLCHAIN configure
  PRE_CONFIGURE ${jemalloc_AUTOGEN})

set(Jemalloc_DIR ${Jemalloc_ROOT})
find_package(Jemalloc ${jemalloc_VERSION} REQUIRED)

include_directories(${JEMALLOC_INCLUDE_DIR})

# build jemalloc shared library
set(JemallocShared_CONF_OPTIONS
    --enable-shared
    --disable-static
    --disable-cxx
    --enable-stats)

if (SUPPORT_JEPROF)
    message(STATUS "Support jemalloc memory profiling.")
    add_compile_definitions(SUPPORT_JEPROF)
    list(APPEND JemallocShared_CONF_OPTIONS --enable-prof)
endif ()

add_thirdparty_lib(JemallocShared
  URL ${jemalloc_URL}
  SHA256 ${jemalloc_SHA256}
  FAKE_SHA256 ${jemalloc_FAKE_SHA256}
  VERSION ${jemalloc_VERSION}
  CONF_OPTIONS ${JemallocShared_CONF_OPTIONS}
  C_FLAGS ${jemalloc_C_FLAGS}
  LINK_FLAGS ${jemalloc_LINK_FLAGS}
  TOOLCHAIN configure
  PRE_CONFIGURE ${jemalloc_AUTOGEN})

set(ENV{LDFLAGS} ${_ORG_LD_FLAGS})

set(JemallocShared_DIR ${JemallocShared_ROOT})
find_package(JemallocShared ${jemalloc_VERSION} REQUIRED)
