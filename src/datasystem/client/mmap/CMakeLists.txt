list(APPEND CLIENT_MMAP_SRCS
        embedded_mmap_table_entry.cpp
        embedded_mmap_table.cpp
        immap_table.cpp
        shm_mmap_table_entry.cpp
        shm_mmap_table.cpp)

list(APPEND CLIENT_DEPEND_LIBS
    common_util)

add_library(client_mmap_static STATIC ${CLIENT_MMAP_SRCS})