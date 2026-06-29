set(COMMON_TASK_ACTION_SRCS
        task_action.cpp
        task_action_registry.cpp
        )

set(COMMON_TASK_ACTION_DEPEND_LIBS
        common_util
        )

add_library(common_task_action STATIC ${COMMON_TASK_ACTION_SRCS})
target_link_libraries(common_task_action PRIVATE ${COMMON_TASK_ACTION_DEPEND_LIBS})
