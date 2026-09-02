list(APPEND CLIENT_MMAP_SRCS
        embedded_mmap_table_entry.cpp
        embedded_mmap_table.cpp
        immap_table.cpp
        shm_mmap_table_entry.cpp
        shm_mmap_table.cpp)

add_library(client_mmap_static STATIC ${CLIENT_MMAP_SRCS})
target_link_libraries(client_mmap_static PRIVATE common_cuda_host_memory common_util)
