if (NOT SRC_PATH_PATTERN OR NOT DST_PATH)
    message(FATAL_ERROR "Can't find SRC_PATH_PATTERN or DST_PATH")
endif ()

find_program(Objcopy_EXECUTABLE objcopy)

string(REPLACE "," ";" SRC_PATH_PATTERN_LIST ${SRC_PATH_PATTERN})

foreach(PATTERN ${SRC_PATH_PATTERN_LIST})
    # support match pattern
    FILE(GLOB_RECURSE SRC_PATH_LIST ${PATTERN})
    list(APPEND SRC_PATH_LISTS ${SRC_PATH_LIST})
endforeach()

foreach(SRC_PATH ${SRC_PATH_LISTS})
    if (NOT EXISTS ${SRC_PATH})
        message(FATAL_ERROR "file ${SRC_PATH} not exists")
    endif ()

    if (NOT Objcopy_EXECUTABLE)
        message(FATAL_ERROR "command objcopy not found")
    endif ()

    get_filename_component(FILE_NAME ${SRC_PATH} NAME)
    get_filename_component(BASE_NAME ${SRC_PATH} NAME_WE)

    file(COPY ${SRC_PATH} DESTINATION ${DST_PATH})

    # Symlink don't need to strip
    if (STRIP_LIB AND NOT IS_SYMLINK ${SRC_PATH})
        message("strip lib ${SRC_PATH}")
        execute_process(
            COMMAND ${Objcopy_EXECUTABLE} --only-keep-debug ${DST_PATH}/${FILE_NAME} ${DST_PATH}/${FILE_NAME}.sym
        )

        execute_process(
            COMMAND ${Objcopy_EXECUTABLE} --add-gnu-debuglink=${DST_PATH}/${FILE_NAME}.sym ${DST_PATH}/${FILE_NAME}
        )

        execute_process(
            COMMAND ${Objcopy_EXECUTABLE} --strip-all ${DST_PATH}/${FILE_NAME}
        )
    endif ()

    # If don't need to generate so.properties, set IGNORE_SO_PROPERTY to TRUE
    if (NOT IGNORE_SO_PROPERTY AND NOT IS_SYMLINK ${SRC_PATH})
        file(SHA256  ${DST_PATH}/${FILE_NAME} FILE_HASH)
        file(APPEND ${DST_PATH}/so.properties "${BASE_NAME}=${FILE_NAME}\n")
        file(APPEND ${DST_PATH}/so.properties "${BASE_NAME}.hash=${FILE_HASH}\n")
    endif ()
endforeach()