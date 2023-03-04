add_library(dt_column DataColumn.c)
target_include_directories(dt_column PUBLIC ${DataTable_SOURCE_DIR}/include)
