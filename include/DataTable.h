#ifndef DATA_TABLE_H
#define DATA_TABLE_H

#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include <stdarg.h>
#include <stdbool.h>

#include "DataColumn.h"
#include "StatusCodes.h"

#define MAX_COL_LEN 101

struct ColumnPair
{
	char name[MAX_COL_LEN];
	struct DataColumn* column;
};

struct DataTable
{
	struct ColumnPair* columns;
	size_t n_columns;
	size_t column_capacity;
	size_t n_rows;
};

// create a new empty table with n_columns by passing and array of
// column names and data types.
// returns NULL on failure (e.g., couldn't allocate enough memory)
struct DataTable*
dt_table_create(
	const size_t n_columns,
	const char (*column_names)[MAX_COL_LEN],
	const enum data_type_e* data_types);

// free memory allocated by table (also frees the underlying columns)
void
dt_table_free(
	struct DataTable** const table);

// insert a row by passing n_column arguments by pointer (be sure you are using correct types).
// returns DT_ALLOC_ERROR if there was a problem resizing, DT_SUCCESS otherwise.
enum status_code_e
dt_table_insert_row(
	struct DataTable* const table,
	size_t n_columns,
	...);

// set value at specified row/column index.
void
dt_table_set_value(
	struct DataTable* const table,
	const size_t row,
	const size_t column,
	const void* const value);

// get value at specified row/column index as a pointer
void*
dt_table_get_value(
	const struct DataTable* const table,
	const size_t row,
	const size_t column);

// select a subset of columns and return a COPY of the table.
// pass the # of columns to select followed by the array of column names.
// every column MUST be present in original table or NULL will be returned.
// WARNING: do not directly reassign as you will lose the original pointer and cause a memory leak.
// returns NULL on failure (e.g., out of memory)
struct DataTable*
dt_table_select(
	const struct DataTable* const table,
	const size_t n_columns,
	const char (*columns)[MAX_COL_LEN]);

// filter a single column by name and return a (newly-allocated) table
// containing the rows that matched the filter callback.
// returns NULL on failure (e.g., out of memory)
struct DataTable*
dt_table_filter_by_name(
	const struct DataTable* const table,
	const char* const column,
	void* user_data,
	bool (*filter_callback)(void* item, void* user_data));

// filter a single column by index and return a (newly-allocated) table
// containing the rows that matched the filter callback.
// returns NULL on failure (e.g., out of memory)
struct DataTable*
dt_table_filter_by_idx(
	const struct DataTable* const table,
	const size_t column_idx,
	void* user_data,
	bool (*filter_callback)(void* item, void* user_data));

#endif
