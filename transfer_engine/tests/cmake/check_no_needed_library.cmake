if (NOT READELF_EXECUTABLE OR NOT EXISTS "${READELF_EXECUTABLE}")
    message(FATAL_ERROR "readelf executable is unavailable: ${READELF_EXECUTABLE}")
endif()

if (NOT LIBRARY_PATH OR NOT EXISTS "${LIBRARY_PATH}")
    message(FATAL_ERROR "shared library is unavailable: ${LIBRARY_PATH}")
endif()

if (NOT NEEDED_LIBRARY)
    message(FATAL_ERROR "NEEDED_LIBRARY is required")
endif()

execute_process(
    COMMAND "${READELF_EXECUTABLE}" --dynamic "${LIBRARY_PATH}"
    RESULT_VARIABLE readelf_result
    OUTPUT_VARIABLE dynamic_entries
    ERROR_VARIABLE readelf_error
)

if (NOT readelf_result EQUAL 0)
    message(FATAL_ERROR "failed to inspect ${LIBRARY_PATH}: ${readelf_error}")
endif()

string(REGEX MATCH "[^\n]*\\(NEEDED\\)[^\n]*\\[[^]\n]*${NEEDED_LIBRARY}[^]\n]*\\][^\n]*" needed_match
       "${dynamic_entries}")
if (needed_match)
    message(FATAL_ERROR
        "${LIBRARY_PATH} depends on forbidden shared library ${NEEDED_LIBRARY}:\n${needed_match}")
endif()
