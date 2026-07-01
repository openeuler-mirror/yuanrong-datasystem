add_library(ds_flags OBJECT flag_manager.cpp flags.cpp embedded_config.cpp)
target_link_libraries(ds_flags PRIVATE nlohmann_json::nlohmann_json)

set(DYNAMIC_FLAG_CONFIG_SRCS
        dynamic_flag_config.cpp
        common_flags_validate.cpp
        config_monitor_state.cpp
        dynamic_config_updater.cpp)

set(DYNAMIC_FLAG_CONFIG_LIBS
        ${ZLIB_LIBRARIES}
        ${SECUREC_LIBRARY}
        common_log
        nlohmann_json::nlohmann_json)

add_library(dynamic_flag_config STATIC ${DYNAMIC_FLAG_CONFIG_SRCS})
target_link_libraries(dynamic_flag_config PRIVATE ${DYNAMIC_FLAG_CONFIG_LIBS})
target_link_libraries(dynamic_flag_config PRIVATE ds_flags)

add_library(common_flags SHARED common_flag_define.cpp eviction_watermark.cpp $<TARGET_OBJECTS:ds_flags>)
target_link_libraries(common_flags PRIVATE ${SECUREC_LIBRARY})
