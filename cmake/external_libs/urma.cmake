find_package(URMA REQUIRED)

include_directories(${URMA_INCLUDE_DIR})

add_definitions(-DUSE_URMA)
if (URMA_OVER_UB)
    message(STATUS "Build URMA over UB")
    add_definitions(-DURMA_OVER_UB)
endif()