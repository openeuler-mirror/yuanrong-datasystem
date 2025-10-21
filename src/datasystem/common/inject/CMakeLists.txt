if (WITH_TESTS)
        set(INJECT_SRCS inject_point.cpp)
        set(INJECT_DEPEND_LIBS
                common_util
        )
        add_library(common_inject STATIC ${INJECT_SRCS})
        target_link_libraries(common_inject PRIVATE ${INJECT_DEPEND_LIBS})
else()
        add_library(common_inject INTERFACE)
endif()
