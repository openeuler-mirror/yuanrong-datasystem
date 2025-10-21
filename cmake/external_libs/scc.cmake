if(DEFINED ENV{SCC_CUSTOM_PATH})
    set(SCC_PATH $ENV{SCC_CUSTOM_PATH})
else()
    set(SCC_PATH /usr/local/seccomponent)
endif()

file(GLOB SCC_LIBS "${SCC_PATH}/lib/lib*")
set(SCC_INCLUDE_DIRS ${SCC_PATH}/include)

message(STATUS "SCC_INCLUDE_DIRS=${SCC_INCLUDE_DIRS}")
message(STATUS "SCC_LIBS=${SCC_LIBS}")

include_directories(SYSTEM ${SCC_INCLUDE_DIRS})
