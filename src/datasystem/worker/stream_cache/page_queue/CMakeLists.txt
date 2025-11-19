set(SC_PAGE_QUEUE_SRCS
    page_queue_base.cpp
    exclusive_page_queue.cpp
    shared_page_queue.cpp
    shared_page_queue_group.cpp
    page_queue_handler.cpp
)

set(SC_PAGE_QUEUE_DEPEND_LIBS
    common_log
    common_util
)

add_library(sc_page_queue STATIC ${SC_PAGE_QUEUE_SRCS})
target_link_libraries(sc_page_queue PRIVATE ${SC_PAGE_QUEUE_DEPEND_LIBS})
add_dependencies(sc_page_queue
    share_memory_protos
)
