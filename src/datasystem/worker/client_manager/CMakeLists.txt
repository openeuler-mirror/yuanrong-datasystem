
set(WORKER_CLIENT_MANAGER_SRCS
        client_info.cpp
        client_manager.cpp
        )

set(WORKER_CLIENT_MANAGER_DEPEND_LIBS
        common_util
        )

add_library(worker_client_manager STATIC ${WORKER_CLIENT_MANAGER_SRCS})
target_link_libraries(worker_client_manager PRIVATE ${WORKER_CLIENT_MANAGER_DEPEND_LIBS})
