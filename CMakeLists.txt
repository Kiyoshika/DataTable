cmake_minimum_required(VERSION 3.10.0)

project(DataTable C)

set(CMAKE_C_STANDARD 99)

set(CMAKE_C_FLAGS "-O0 -g -Wall -Wextra -Wpedantic -fsanitize=address")
#set(CMAKE_C_FLAGS "-O0 -g -Wall -Wextra -Wpedantic")
#set(CMAKE_C_FLAGS "-O2 -Wall")

add_subdirectory(src)

enable_testing()
add_subdirectory(test)
