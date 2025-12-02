set(STRING_REF_SRC
    string_entity.cpp
    string_pool.cpp
)

set(STRING_REF_DEPENDS_LIB
    ${TBB_LIBRARY}
    common_log
)

add_library(string_ref STATIC ${STRING_REF_SRC})
target_link_libraries(string_ref PRIVATE ${STRING_REF_DEPENDS_LIB})
