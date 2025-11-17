if (DOWNLOAD_UB)
    set(URMA_INCLUDE_LOCATION ${UB_ROOT}/include)

    set(URMA_LIB_LOCATION ${UB_ROOT}/lib64)
    set(URMA_IP_IB_LIB_LOCATION ${UB_ROOT}/lib64/urma)
else ()
    if (NOT URMA_INCLUDE_LOCATION)
        set(URMA_INCLUDE_LOCATION "/usr/include/umdk")
    endif()

    if (NOT URMA_LIB_LOCATION)
        set(URMA_LIB_LOCATION "/usr/lib64")
    endif()

    if (NOT URMA_IP_IB_LIB_LOCATION)
        set(URMA_IP_IB_LIB_LOCATION "/usr/lib64/urma")
    endif()
endif()

find_package(URMA REQUIRED)

include_directories(${URMA_INCLUDE_DIR})
include_directories(${URMA_INCLUDE_DIR}/common)

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-missing-field-initializers -Wno-unused-parameter")
add_definitions(-DUSE_URMA)
if (URMA_OVER_UB)
    add_definitions(-DURMA_OVER_UB)
endif()