if (NOT FILE_PATH OR NOT EXISTS "${FILE_PATH}")
    message(FATAL_ERROR "source file is unavailable: ${FILE_PATH}")
endif()

if (NOT FORBIDDEN_TEXT)
    message(FATAL_ERROR "FORBIDDEN_TEXT is required")
endif()

file(READ "${FILE_PATH}" source_text)
string(FIND "${source_text}" "${FORBIDDEN_TEXT}" forbidden_offset)
if (NOT forbidden_offset EQUAL -1)
    message(FATAL_ERROR "${FILE_PATH} contains forbidden text: ${FORBIDDEN_TEXT}")
endif()
