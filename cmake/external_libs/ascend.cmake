# The environment variable ASCEND_CUSTOM_PATH is used to locate the Ascend install path.
# So cmake can find the header files and libraries in the compile stage.
# If user don't set ASCEND_CUSTOM_PATH, find Ascend in `/usr/local/Ascend/ascend-toolkit/latest` in default.
if (DEFINED ENV{ASCEND_HOME_PATH})
    set(Ascend_ROOT $ENV{ASCEND_HOME_PATH})
elseif(DEFINED ENV{ASCEND_CUSTOM_PATH})
    set(Ascend_ROOT $ENV{ASCEND_CUSTOM_PATH}/latest)
else()
    set(Ascend_ROOT /usr/local/Ascend/ascend-toolkit/latest)
endif()

find_package(Ascend REQUIRED)

include_directories(SYSTEM ${ASCEND_INCLUDE_DIR})
include_directories(SYSTEM ${ASCEND_INCLUDE_DIR}/experiment/msprof)
include_directories(SYSTEM ${ASCEND_INCLUDE_DIR}/experiment/runtime)
if(EXISTS ${ASCEND_INCLUDE_DIR}/../pkg_inc/runtime)
    include_directories(SYSTEM ${ASCEND_INCLUDE_DIR}/../pkg_inc/runtime)
endif()
add_definitions(-DUSE_ASCEND)
