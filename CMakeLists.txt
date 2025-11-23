cmake_minimum_required(VERSION 3.10)

# Set extension name here
set(TARGET_NAME read_rdf)

find_package(PkgConfig REQUIRED)
pkg_check_modules(SERD REQUIRED IMPORTED_TARGET serd-0)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES src/read_rdf_extension.cpp src/serd_buffer.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(${EXTENSION_NAME} PkgConfig::SERD)
target_include_directories(${EXTENSION_NAME} PRIVATE ${SERD_INCLUDE_DIRS}) 

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
