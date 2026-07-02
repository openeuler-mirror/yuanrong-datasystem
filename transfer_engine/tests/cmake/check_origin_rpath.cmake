if (NOT READELF_EXECUTABLE OR NOT EXISTS "${READELF_EXECUTABLE}")
    message(FATAL_ERROR "readelf is unavailable: ${READELF_EXECUTABLE}")
endif()

if (NOT LIBRARY_PATH OR NOT EXISTS "${LIBRARY_PATH}")
    message(FATAL_ERROR "shared library is unavailable: ${LIBRARY_PATH}")
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

string(REGEX MATCH "\\(RPATH\\).*\\$ORIGIN" origin_rpath_match "${dynamic_entries}")
if (NOT origin_rpath_match)
    message(FATAL_ERROR "${LIBRARY_PATH} must use DT_RPATH with $ORIGIN for package-local transitive dependencies")
endif()

string(REGEX MATCH "\\(RUNPATH\\)" runpath_match "${dynamic_entries}")
if (runpath_match)
    message(FATAL_ERROR "${LIBRARY_PATH} must not use DT_RUNPATH because it is not inherited by dependency children")
endif()
