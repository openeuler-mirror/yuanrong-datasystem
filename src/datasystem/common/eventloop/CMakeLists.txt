set(EVENT_LOOP_SRCS
        event_loop.cpp
        timer_queue.cpp
        )

set(EVENT_LOOP_DEPEND_LIBS
        ${SECUREC_LIBRARY}
        common_log
        common_util
        )

add_library(common_event_loop STATIC ${EVENT_LOOP_SRCS})
target_link_libraries(common_event_loop PRIVATE ${EVENT_LOOP_DEPEND_LIBS})
