set(SHM_UNIT_INFO_DEPEND_LIBS
        common_log
        common_util
)
add_library(common_shm_unit_info STATIC shm_unit_info.cpp)
target_link_libraries(common_shm_unit_info PRIVATE ${SHM_UNIT_INFO_DEPEND_LIBS})

set(SHARED_MEM_SRCS
        allocator.cpp
        jemalloc.cpp
        arena.cpp
        shm_unit.cpp
        resource_pool.cpp
        shared_disk_detecter.cpp
        mmap/base_mmap.cpp
        mmap/disk_mmap.cpp
        mmap/dev_mmap.cpp
        mmap/mem_mmap.cpp)

set(SHARED_MEM_DEPEND_LIBS
        ${JEMALLOC_PIC_LIBRARY}
        dl
        common_perf
        common_shm_unit_info
        )

add_library(common_shared_memory STATIC ${SHARED_MEM_SRCS})
target_link_libraries(common_shared_memory PRIVATE ${SHARED_MEM_DEPEND_LIBS})