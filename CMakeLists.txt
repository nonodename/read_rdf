cmake_minimum_required(VERSION 3.10)

# Set extension name here
set(TARGET_NAME read_rdf)

# If decide to re-instate VCPKG will need env variables 
# for vcpkg - uncomment and set paths if using vcpkg
#VCPKG_ROOT=<your code root>/vcpkg
#VCPKG_TOOLCHAIN_PATH=<your code root>/vcpkg/scripts/buildsystems/vcpkg.cmake

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

# Correct placement: define SERD_STATIC before compiling serd sources
if(WIN32)
    add_definitions(-DSERD_STATIC)
endif()

include_directories(src/include)

set(EXTENSION_SOURCES src/read_rdf_extension.cpp src/serd_buffer.cpp src/base64.c src/byte_source.c src/env.c src/n3.c src/node.c src/reader.c src/string.c src/system.c src/uri.c src/writer.c)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})


# On Windows, serd is built as a static library, but the headers default to
# dllimport. Define SERD_STATIC to use static linking and avoid LNK2019/LNK1120 errors.
#if(WIN32)
#    target_compile_definitions(${EXTENSION_NAME} PRIVATE SERD_STATIC)
#    target_compile_definitions(${LOADABLE_EXTENSION_NAME} PRIVATE SERD_STATIC)
#endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
