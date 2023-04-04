add_library(dt_column DataColumn.c)
target_include_directories(dt_column PUBLIC ${DataTable_SOURCE_DIR}/include)

add_library(datatable DataTable.c)
target_include_directories(datatable PUBLIC ${DataTable_SOURCE_DIR}/include)
target_link_libraries(datatable dt_column dt_hashtable)

add_library(dt_hashtable HashTable.c)
target_include_directories(dt_hashtable PUBLIC ${DataTable_SOURCE_DIR}/include)
target_link_libraries(dt_hashtable datatable)
