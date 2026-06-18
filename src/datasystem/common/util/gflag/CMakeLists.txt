set(GFLAG_SRCS
        common_gflags_validate.cpp
        config_monitor_state.cpp
        dynamic_config_updater.cpp
        flags.cpp)
 	 
set(GFLAG_LIBS
        ${ZLIB_LIBRARIES}
        ${SECUREC_LIBRARY}
        common_log
        nlohmann_json::nlohmann_json)
  
 	 
add_library(common_util_gflag STATIC ${GFLAG_SRCS})
target_link_libraries(common_util_gflag PRIVATE ${GFLAG_LIBS})
target_link_libraries(common_util_gflag PRIVATE ds_flags)
add_library(common_flags SHARED common_gflag_define.cpp eviction_watermark.cpp $<TARGET_OBJECTS:ds_flags>)
target_link_libraries(common_flags PRIVATE ${SECUREC_LIBRARY})
