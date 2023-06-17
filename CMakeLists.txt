cmake_minimum_required(VERSION 3.10.0)

project(DataTable C)

set(CMAKE_C_STANDARD 99)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

option(DEBUG_MODE "Compile in debug mode" OFF)
option(COMPILE_TESTS "Compile tests" OFF)

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
