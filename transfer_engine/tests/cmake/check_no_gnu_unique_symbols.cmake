if (NOT READELF_EXECUTABLE OR NOT EXISTS "${READELF_EXECUTABLE}")
    message(FATAL_ERROR "readelf executable is unavailable: ${READELF_EXECUTABLE}")
endif()

if (NOT LIBRARY_PATH OR NOT EXISTS "${LIBRARY_PATH}")
    message(FATAL_ERROR "shared library is unavailable: ${LIBRARY_PATH}")
endif()

execute_process(
    COMMAND "${READELF_EXECUTABLE}" --wide --dyn-syms "${LIBRARY_PATH}"
    RESULT_VARIABLE readelf_result
    OUTPUT_VARIABLE dynamic_symbols
    ERROR_VARIABLE readelf_error
)

if (NOT readelf_result EQUAL 0)
    message(FATAL_ERROR "failed to inspect ${LIBRARY_PATH}: ${readelf_error}")
endif()

string(REGEX MATCHALL "[^\n]*[ \t]UNIQUE[ \t][^\n]*" unique_symbols "${dynamic_symbols}")
if (unique_symbols)
    list(JOIN unique_symbols "\n" unique_symbol_text)
    message(FATAL_ERROR
        "${LIBRARY_PATH} exports STB_GNU_UNIQUE symbols that can escape RTLD_LOCAL:\n${unique_symbol_text}")
endif()
