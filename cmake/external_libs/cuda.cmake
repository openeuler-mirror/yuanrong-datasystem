# The environment variable CUDA_CUSTOM_PATH is used to locate the CUDA install path.
# So cmake can find the header files and libraries in the compile stage.
# If user don't set CUDA_CUSTOM_PATH, find CUDA in `/usr/local/cuda` in default.
if (DEFINED ENV{CUDA_HOME_PATH})
    set(Cuda_ROOT $ENV{CUDA_HOME_PATH})
elseif(DEFINED ENV{CUDA_CUSTOM_PATH})
    set(Cuda_ROOT $ENV{CUDA_CUSTOM_PATH})
else()
    set(Cuda_ROOT /usr/local/cuda)
endif()

find_package(Cuda REQUIRED)

include_directories(SYSTEM ${CUDA_INCLUDE_DIR})
add_definitions(-DUSE_CUDA)
