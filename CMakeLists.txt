cmake_minimum_required(VERSION 3.10)

# ------------------------------------------------------------
# Project setup
# ------------------------------------------------------------
set(TARGET_NAME read_rdf)
set(SERD_NAME serd_lib)

find_package(LibXml2 REQUIRED)

project(${TARGET_NAME} LANGUAGES C CXX)

set(EXTENSION_NAME         ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

set(EXTENSION_SOURCES
    src/read_rdf_extension.cpp
    src/serd_buffer.cpp
    src/rdf_xml_parser.cpp
    src/xml_buffer.cpp
)

# ------------------------------------------------------------
# SERD — internal static library
# ------------------------------------------------------------
add_library(${SERD_NAME} STATIC
    serd/src/base64.c
    serd/src/byte_source.c
    serd/src/env.c
    serd/src/n3.c
    serd/src/node.c
    serd/src/read_utf8.c
    serd/src/reader.c
    serd/src/string.c
    serd/src/system.c
    serd/src/uri.c
    serd/src/writer.c
)

target_include_directories(${SERD_NAME}
    PRIVATE
        serd/include
)

# ------------------------------------------------------------
# DuckDB extension: static + loadable
# ------------------------------------------------------------
build_static_extension(
    ${TARGET_NAME}
    ${EXTENSION_SOURCES}
)

build_loadable_extension(
    ${TARGET_NAME}
    ""
    ${EXTENSION_SOURCES}
)

# Add target-local include paths
target_include_directories(${EXTENSION_NAME}
    PRIVATE
        src/include
        serd/include
    ${LIBXML2_INCLUDE_DIR}
)
target_include_directories(${LOADABLE_EXTENSION_NAME}
    PRIVATE
        src/include
        serd/include
    ${LIBXML2_INCLUDE_DIR}
)

# Link SERD internally (no leakage, no exports)
target_link_libraries(${EXTENSION_NAME}
        ${SERD_NAME}
        LibXml2::LibXml2
)

target_link_libraries(${LOADABLE_EXTENSION_NAME}
        ${SERD_NAME}
        LibXml2::LibXml2
)

# Windows: force static SERD behavior (avoid dllimport)
if (WIN32)
    target_compile_definitions(${SERD_NAME} PRIVATE SERD_STATIC)
    target_compile_definitions(${EXTENSION_NAME} PRIVATE SERD_STATIC)
    target_compile_definitions(${LOADABLE_EXTENSION_NAME} PRIVATE SERD_STATIC)
endif()

install(
    TARGETS ${SERD_NAME}
    EXPORT "${DUCKDB_EXPORT_SET}"
    ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"  # .a file on Unix, .lib on Windows
)
install(
    TARGETS ${EXTENSION_NAME} 
    EXPORT  "${DUCKDB_EXPORT_SET}"
    LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
    ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"
)
