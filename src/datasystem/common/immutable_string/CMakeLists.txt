set(IMMUTABLE_STRING_SRC
    immutable_string_pool.cpp
    immutable_string.cpp
    ref_count_string.cpp
    )

set(IMMUTABLE_STRING_DEPNEDS_LIB
        ${TBB_LIBRARY}
        common_log
    )

add_library(common_immutable_string STATIC ${IMMUTABLE_STRING_SRC})
target_link_libraries(common_immutable_string PRIVATE ${IMMUTABLE_STRING_DEPNEDS_LIB})