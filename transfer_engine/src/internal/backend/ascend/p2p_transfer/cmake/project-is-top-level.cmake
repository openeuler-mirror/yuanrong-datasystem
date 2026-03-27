# Determines whether current project is a top level or subproject.
# Used to adjust warnings and build process based on how the project is build (e.g. don't allow building in
# developer mode as subproject)

# This variable is set by project() in CMake 3.21+
string(
    COMPARE EQUAL
    "${CMAKE_SOURCE_DIR}" "${PROJECT_SOURCE_DIR}"
    PROJECT_IS_TOP_LEVEL
)
