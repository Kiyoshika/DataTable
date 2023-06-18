if (COMPILE_STATIC)
  add_library(datatable STATIC DataTable.c HashTable.c DataColumn.c)
else()
  add_library(datatable SHARED DataTable.c HashTable.c DataColumn.c)
endif()

target_include_directories(datatable PUBLIC ${DataTable_SOURCE_DIR}/include)
