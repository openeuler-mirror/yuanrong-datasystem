set(GFLAG_SRCS
        common_gflags_validate.cpp
        flags.cpp)
 	 
set(GFLAG_LIBS
        ${ZLIB_LIBRARIES}
        ${SECUREC_LIBRARY}
        common_log)
  
 	 
add_library(common_util_gflag STATIC ${GFLAG_SRCS})
target_link_libraries(common_util_gflag PRIVATE ${GFLAG_LIBS})
add_library(common_flags SHARED common_gflag_define.cpp)
target_link_libraries(common_util_gflag PRIVATE ds_flags)