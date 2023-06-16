cmake_minimum_required(VERSION 3.10.0)

project(DataTable C)

set(CMAKE_C_STANDARD 99)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

set(CMAKE_C_FLAGS "-O0 -g -Wall")
#set(CMAKE_C_FLAGS "-O0 -g -Wall -fsanitize=address")
#set(CMAKE_C_FLAGS "-O0 -g -Wall -Wextra -Wpedantic -fsanitize=address")
#set(CMAKE_C_FLAGS "-O0 -g -Wall -Wextra -Wpedantic")
#set(CMAKE_C_FLAGS "-O2 -Wall")

add_subdirectory(src)

enable_testing()
add_subdirectory(test)

# copy test CSV data to build/test/DataTable directory
file(COPY mytable.csv DESTINATION test/DataTable)
