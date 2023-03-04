add_library(dt_column DataColumn.c)
target_include_directories(dt_column PUBLIC ${DataTable_SOURCE_DIR}/include)

add_executable(main main.c)
target_include_directories(main PUBLIC ${DataTable_SOURCE_DIR}/include)
target_link_libraries(main dt_column)
