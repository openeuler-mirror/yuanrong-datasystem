add_library(worker_runtime_types INTERFACE)
target_include_directories(worker_runtime_types INTERFACE ${PROJECT_SOURCE_DIR}/src)

add_library(worker_runtime_state STATIC worker_runtime_state.cpp)
target_include_directories(worker_runtime_state PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(worker_runtime_state PUBLIC worker_runtime_types)

add_library(worker_service_admission STATIC worker_service_admission.cpp)
target_include_directories(worker_service_admission PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(worker_service_admission PUBLIC worker_runtime_state common_util)

add_library(worker_admission_facade STATIC worker_admission_facade.cpp)
target_include_directories(worker_admission_facade PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(worker_admission_facade PUBLIC worker_service_admission)

add_library(worker_runtime_facade STATIC worker_runtime_facade.cpp)
target_include_directories(worker_runtime_facade PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(worker_runtime_facade PUBLIC worker_admission_facade common_util)

add_library(worker_isolation_coordinator STATIC worker_isolation_coordinator.cpp)
target_include_directories(worker_isolation_coordinator PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(worker_isolation_coordinator PUBLIC worker_runtime_facade common_inject common_log)

add_library(worker_runtime_core INTERFACE)
target_link_libraries(worker_runtime_core INTERFACE
        worker_admission_facade
        worker_isolation_coordinator
        worker_runtime_facade
        worker_runtime_state
        worker_runtime_types
        worker_service_admission
        )
