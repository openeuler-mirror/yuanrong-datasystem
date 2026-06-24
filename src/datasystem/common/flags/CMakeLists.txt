add_library(ds_flags OBJECT flag_manager.cpp flags.cpp embedded_config.cpp)
target_link_libraries(ds_flags PRIVATE nlohmann_json::nlohmann_json)