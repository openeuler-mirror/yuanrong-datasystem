# add subdirectories
add_subdirectory(protos)
add_subdirectory(client)
add_subdirectory(common)
add_subdirectory(master)
add_subdirectory(server)
add_subdirectory(worker)

if (BUILD_PYTHON_API)
    add_subdirectory(pybind_api)
endif ()