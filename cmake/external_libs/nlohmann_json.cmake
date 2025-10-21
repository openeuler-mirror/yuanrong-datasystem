set(nlohmann_json_VERSION 3.11.3)
if (NOT "$ENV{DS_LOCAL_LIBS_DIR}" STREQUAL "")
  set(nlohmann_json_URL "$ENV{DS_LOCAL_LIBS_DIR}/opensource_third_party/v3.11.3.zip")
else()
  set(nlohmann_json_URL "https://gitee.com/mirrors/nlohmann-json/repository/archive/v3.11.3.zip")
endif()
set(nlohmann_json_SHA256 "0deac294b2c96c593d0b7c0fb2385a2f4594e8053a36c52b11445ef4b9defebb")

set(nlohmann_json_CMAKE_OPTIONS
    -DJSON_BuildTests:BOOL=OFF
    -DCMAKE_BUILD_TYPE:STRING=Release)

set(nlohmann_json_CXX_FLAGS ${THIRDPARTY_SAFE_FLAGS})

add_thirdparty_lib(nlohmann_json 
  URL ${nlohmann_json_URL}
  SHA256 ${nlohmann_json_SHA256}
  FAKE_SHA256 ${nlohmann_json_FAKE_SHA256}
  VERSION ${nlohmann_json_VERSION}
  CONF_OPTIONS ${nlohmann_json_CMAKE_OPTIONS}
  CXX_FLAGS ${nlohmann_json_CXX_FLAGS})

set(nlohmann_json_DIR ${nlohmann_json_ROOT})
find_package(nlohmann_json ${nlohmann_json_VERSION} REQUIRED)