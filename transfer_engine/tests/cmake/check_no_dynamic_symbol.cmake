if (NOT READELF_EXECUTABLE OR NOT EXISTS "${READELF_EXECUTABLE}")
    message(FATAL_ERROR "readelf executable is unavailable: ${READELF_EXECUTABLE}")
endif()

if (NOT LIBRARY_PATH OR NOT EXISTS "${LIBRARY_PATH}")
    message(FATAL_ERROR "shared library is unavailable: ${LIBRARY_PATH}")
endif()

if (NOT FORBIDDEN_SYMBOL)
    message(FATAL_ERROR "FORBIDDEN_SYMBOL is required")
endif()

execute_process(
    COMMAND "${READELF_EXECUTABLE}" --wide --dyn-syms --demangle "${LIBRARY_PATH}"
    RESULT_VARIABLE readelf_result
    OUTPUT_VARIABLE dynamic_symbols
    ERROR_VARIABLE readelf_error
)

if (NOT readelf_result EQUAL 0)
    message(FATAL_ERROR "failed to inspect ${LIBRARY_PATH}: ${readelf_error}")
endif()

if (DEFINED_ONLY)
    string(REGEX REPLACE "[^\n]*[ \t]UND[ \t][^\n]*\n?" "" dynamic_symbols "${dynamic_symbols}")
endif()

string(REGEX MATCHALL "[^\n]*${FORBIDDEN_SYMBOL}[^\n]*" forbidden_symbols "${dynamic_symbols}")
if (forbidden_symbols)
    list(JOIN forbidden_symbols "\n" forbidden_symbol_text)
    message(FATAL_ERROR
        "${LIBRARY_PATH} contains forbidden dynamic symbols matching ${FORBIDDEN_SYMBOL}:\n${forbidden_symbol_text}")
endif()
