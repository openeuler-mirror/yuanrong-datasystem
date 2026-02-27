# - Find CUDA (cuda_runtime.h, libcudart.so, libnccl.so)
# This module defines
#  CUDA_INCLUDE_DIR, directory containing headers
#  CUDA_LIBRARY, Location of libcudart's shared library
#  NCCL_LIBRARY, Location of libnccl's shared library
#  CUDA_FOUND, whether CUDA has been found

find_path(CUDA_INCLUDE_DIR cuda_runtime.h
        DOC   "Path to the CUDA header file"
        HINTS
            ${Cuda_ROOT}/include
            /usr/local/cuda/include
            /usr/include
        PATHS
            ${Cuda_ROOT}/include
        NO_DEFAULT_PATH)

find_library(CUDA_LIBRARY ${CMAKE_SHARED_LIBRARY_PREFIX}cudart${CMAKE_SHARED_LIBRARY_SUFFIX}
        DOC   "Path to CUDA runtime library"
        HINTS
            ${Cuda_ROOT}/lib64
            ${Cuda_ROOT}/lib
            /usr/local/cuda/lib64
            /usr/local/lib
            /usr/lib64
            /usr/lib/x86_64-linux-gnu
        PATHS
            ${Cuda_ROOT}/lib64
            ${Cuda_ROOT}/lib
        NO_DEFAULT_PATH)

find_library(NCCL_LIBRARY
        ${CMAKE_SHARED_LIBRARY_PREFIX}nccl${CMAKE_SHARED_LIBRARY_SUFFIX}
        DOC   "Path to NCCL library"
        HINTS
            ${Cuda_ROOT}/lib64
            ${Cuda_ROOT}/lib
            /usr/local/lib
            /usr/lib
            /usr/lib64
            /usr/lib/x86_64-linux-gnu
        PATHS
            ${Cuda_ROOT}/lib64
            ${Cuda_ROOT}/lib
        NO_DEFAULT_PATH)

message("cuda lib: ${CUDA_LIBRARY}")
message("nccl lib: ${NCCL_LIBRARY}")

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Cuda REQUIRED_VARS
        CUDA_LIBRARY NCCL_LIBRARY CUDA_INCLUDE_DIR)
