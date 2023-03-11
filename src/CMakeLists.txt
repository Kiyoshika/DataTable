add_library(dt_column DataColumn.c)
target_include_directories(dt_column PUBLIC ${DataTable_SOURCE_DIR}/include)

add_library(datatable DataTable)
target_include_directories(datatable PUBLIC ${DataTable_SOURCE_DIR}/include)
target_link_libraries(datatable dt_column)
