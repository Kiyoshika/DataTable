add_library(datatable SHARED DataTable.c HashTable.c DataColumn.c)
target_include_directories(datatable PUBLIC ${DataTable_SOURCE_DIR}/include)
