def _datasystem_cmake_config_impl(ctx):
    config_file = ctx.actions.declare_file("DatasystemConfig.cmake")
    version_file = ctx.actions.declare_file("DatasystemConfigVersion.cmake")
    targets_file = ctx.actions.declare_file("DatasystemTargets.cmake")

    version = ctx.attr.version

    ctx.actions.write(
        output = config_file,
        content = """# Datasystem CMake configuration file
#
# This config sets the following variables in your project:
#
#   Datasystem_FOUND - true if Datasystem found on the system
#   Datasystem_INCLUDE_DIR - the directory containing Datasystem headers
#   datasystem - the Datasystem shared library target

get_filename_component(_INSTALL_PREFIX "${{CMAKE_CURRENT_LIST_DIR}}/../../.." ABSOLUTE)

# Provide set_and_check if not available (normally from CMakePackageConfigHelpers)
if(NOT COMMAND set_and_check)
  function(set_and_check var value)
    set(${{var}} "${{value}}" PARENT_SCOPE)
  endfunction()
endif()

if (NOT TARGET datasystem)
    include("${{CMAKE_CURRENT_LIST_DIR}}/DatasystemTargets.cmake")
endif()

set_and_check(Datasystem_INCLUDE_DIR "${{_INSTALL_PREFIX}}/include")
set(Datasystem_FOUND TRUE)
""",
    )

    ctx.actions.write(
        output = version_file,
        content = """# Datasystem CMake configuration version file
set(PACKAGE_VERSION "{version}")

if("${{PACKAGE_VERSION}}" VERSION_LESS "${{PACKAGE_FIND_VERSION}}")
  set(PACKAGE_VERSION_COMPATIBLE FALSE)
else()
  set(PACKAGE_VERSION_COMPATIBLE TRUE)
  if ("${{PACKAGE_VERSION}}" VERSION_EQUAL "${{PACKAGE_FIND_VERSION}}")
    set(PACKAGE_VERSION_EXACT TRUE)
  endif()
endif()
""".format(version = version),
    )

    ctx.actions.write(
        output = targets_file,
        content = """# Datasystem CMake targets file
# Config dir: cpp/lib/cmake/Datasystem/
# Library:    cpp/lib/libdatasystem.so
# Headers:    cpp/include/
if (NOT TARGET datasystem)
  add_library(datasystem SHARED IMPORTED)
  set_target_properties(datasystem PROPERTIES
    IMPORTED_LOCATION "${CMAKE_CURRENT_LIST_DIR}/../../libdatasystem.so"
    INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_CURRENT_LIST_DIR}/../../../include"
  )
endif()
""",
    )

    return [DefaultInfo(files = depset([config_file, version_file, targets_file]))]

datasystem_cmake_config = rule(
    implementation = _datasystem_cmake_config_impl,
    attrs = {
        "version": attr.string(mandatory = True),
    },
)
