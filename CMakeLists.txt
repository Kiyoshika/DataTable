cmake_minimum_required(VERSION 3.10.0)

set(DATATABLE_VERSION 0.1.0)
project(DataTable LANGUAGES C VERSION ${DATATABLE_VERSION})

set(CMAKE_C_STANDARD 99)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

option(DEBUG_MODE "Compile in debug mode" OFF)
option(COMPILE_TESTS "Compile tests" OFF)
option(COMPILE_STATIC "Compile as static library" ON)

if (DEBUG_MODE)
  message("Compiling in debug mode...")
  set(CMAKE_C_FLAGS "-O0 -g -Wall -Wextra -fsanitize=address")
else()
  message("Compiling in release mode...")
  set(CMAKE_C_FLAGS "-O2 -Wall")
endif()

add_subdirectory(src)

if (COMPILE_TESTS)
  enable_testing()
  add_subdirectory(test)

  # copy test CSV data to build/test/DataTable directory
  file(COPY mytable.csv DESTINATION test/DataTable)
endif()

unset(DEBUG_MODE)
unset(COMPILE_TESTS)
unset(COMPILE_STATIC)

# CONFIGURATION AND INSTALL TARGETS
# TO MAKE LIBRARY DISTRIBUTABLE

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/datatableConfig.cmake "# empty")

write_basic_package_version_file(
  ${CMAKE_CURRENT_BINARY_DIR}/datatableConfigVersion.cmake 
  VERSION ${JSON_VERSION}
  COMPATIBILITY SameMajorVersion)

file(GLOB DATATABLEHEADERS include/*.h)
install(TARGETS datatable DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(FILES ${DATATABLEHEADERS}  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/datatable)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/datatableConfig.cmake DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/datatable)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/datatableConfigVersion.cmake DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/datatable)
