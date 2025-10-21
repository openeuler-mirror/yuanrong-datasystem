# CMake script for cleaning build rpath of build rpath.
if (NOT BIN_FILE OR NOT EXISTS "${BIN_FILE}" OR IS_SYMLINK "${BIN_FILE}")
  message(FATAL_ERROR "BIN_FILE not specified, not exist or is symlink!")
endif()

# Remove the build rpath.
file(RPATH_REMOVE FILE "${BIN_FILE}")

# Check rpath remove success or not.
file(RPATH_CHECK FILE "${BIN_FILE}" RPATH "")