# cpprestsdk
set(cpprestsdk_VERSION v2.10.19)
 
set(cpprestsdk_URLS "https://github.com/microsoft/cpprestsdk/archive/refs/tags/v2.10.19.tar.gz")
 
set(cpprestsdk_SHA256S "4b0d14e5bfe77ce419affd253366e861968ae6ef2c35ae293727c1415bd145c8")  # 替换为实际的 SHA256 哈希
 
set(cpprestsdk_PATCH_COMMAND
    mkdir -p /ext/m30070657/OpenEulerDocker/datasystem/build/_deps/cpprestsdk-src/libs
    && git clone --depth=1 https://github.com/zaphoyd/websocketpp.git /ext/m30070657/OpenEulerDocker/datasystem/build/_deps/cpprestsdk-src/libs/websocketpp)
 
# 检查并设置 Boost 路径，cpprestsdk 通常依赖 Boost
if (EXISTS ${Boost_ROOT}/lib64)
    set(Boost_PKG_PATH ${Boost_ROOT}/lib64)
else()
    set(Boost_PKG_PATH ${Boost_ROOT}/lib)
endif()
 
set(cpprestsdk_PRE_CONFIGURE
    git submodule update --init)
 
# 设置 CMake 选项
set(cpprestsdk_CMAKE_OPTIONS
    -DCMAKE_BUILD_TYPE:STRING=Release
    -DCMAKE_CXX_STANDARD=17
    -DCMAKE_SKIP_RPATH:BOOL=TRUE
    -DBUILD_TESTS:BOOL=OFF
    -DBUILD_SHARED_LIBS:BOOL=OFF
    -DOPENSSL_INCLUDE_DIR:Path=${OPENSSL_INCLUDE_DIR}
    -DCPPREST_EXCLUDE_WEBSOCKETS=ON
    -DBUILD_SAMPLES:BOOL=OFF)
 
set(cpprestsdk_CXX_FLAGS "${THIRDPARTY_SAFE_FLAGS} -Wno-format-truncation -fPIE -pie -fPIC")
 
add_thirdparty_lib(cpprestsdk
    URL ${cpprestsdk_URLS}
    SHA256 ${cpprestsdk_SHA256S}
    FAKE_SHA256 ${cpprestsdk_FAKE_SHA256}
    VERSION ${cpprestsdk_VERSION}
    PRE_CONFIGURE ${cpprestsdk_PRE_CONFIGURE}
    CONF_OPTIONS ${cpprestsdk_CMAKE_OPTIONS}
    CXX_FLAGS ${cpprestsdk_CXX_FLAGS})
 
# 查找并包含 cpprestsdk
find_package(cpprestsdk REQUIRED PATHS ${cpprestsdk_ROOT} CONFIG)
 
get_property(cpprestsdk_INCLUDE_DIR TARGET cpprestsdk::cpprest PROPERTY INTERFACE_INCLUDE_DIRECTORIES)
include_directories(SYSTEM ${cpprestsdk_INCLUDE_DIR})