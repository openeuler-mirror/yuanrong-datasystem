add_library(ds_flags STATIC flag_manager.cpp flags.cpp embedded_config.cpp)
target_link_libraries(ds_flags)