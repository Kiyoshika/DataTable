add_executable(dt_column_create dt_column_create.c)
target_include_directories(dt_column_create PUBLIC ${DataTable_SOURCE_DIR}/include)
target_link_libraries(dt_column_create dt_column)

add_test(NAME dt_column_create COMMAND dt_column_create)
