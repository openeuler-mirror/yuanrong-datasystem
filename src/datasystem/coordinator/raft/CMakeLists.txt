add_library(coordinator_raft_peer STATIC coordinator_raft_peer.cpp)
target_include_directories(coordinator_raft_peer PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/src>
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>)
target_link_libraries(coordinator_raft_peer PUBLIC
        datasystem_braft
        common_util)

add_library(coordinator_raft_types STATIC coordinator_raft_types.cpp)
target_include_directories(coordinator_raft_types PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/src>
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>)
target_link_libraries(coordinator_raft_types
        PUBLIC common_util
        PRIVATE coordinator_raft_peer)

add_library(coordinator_raft_state_machine STATIC coordinator_raft_state_machine.cpp)
target_include_directories(coordinator_raft_state_machine PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/src>
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>)
target_link_libraries(coordinator_raft_state_machine
        PUBLIC
        coordinator_raft_types
        datasystem_braft
        common_util
        PRIVATE
        common_log)

add_library(coordinator_raft_node STATIC coordinator_raft_node.cpp)
target_include_directories(coordinator_raft_node PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/src>
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>)
target_link_libraries(coordinator_raft_node
        PUBLIC
        coordinator_raft_types
        coordinator_raft_state_machine
        datasystem_braft
        PRIVATE
        coordinator_raft_peer
        common_log
        common_rpc_zmq
        common_util)

add_library(coordinator_membership_manager STATIC coordinator_membership_manager.cpp)
target_include_directories(coordinator_membership_manager PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/src>
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>)
target_link_libraries(coordinator_membership_manager
        PUBLIC
        common_util
        PRIVATE
        coordinator_raft_node
        coordinator_raft_peer
        common_log)
