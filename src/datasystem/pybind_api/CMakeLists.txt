add_library(ds_client_py MODULE
        pybind_register.cpp
        pybind_register_common.cpp
        pybind_register_object.cpp
        pybind_register_stream.cpp
        pybind_register_kv.cpp
        pybind_register_hetero.cpp
        pybind_register_context.cpp)

target_link_libraries(ds_client_py PRIVATE
        datasystem
        pybind11::module)

set_target_properties(ds_client_py PROPERTIES INSTALL_RPATH $ORIGIN)
target_link_options(ds_client_py PRIVATE "-Wl,--disable-new-dtags")
