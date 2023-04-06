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
// returns DT_ALLOC_ERROR if there was a problem resizing.
// returns DT_SUCCESS otherwise.
enum status_code_e
dt_table_insert_row(
	struct DataTable* const table,
	size_t n_columns,
	...);

// insert an empty row (typically used internally but exposed to the public)
// returns DT_ALLOC_ERROR if there was a problem resizing.
// returns DT_SUCCESS otherwise.
enum status_code_e
dt_table_insert_empty_row(
	struct DataTable* const table);

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

// copy the "skeleton" of a table (column names and types but NOT the data).
// this is mainly used internally but could be used for very specific use cases.
// returns NULL on failure.
struct DataTable*
dt_table_copy_skeleton(
	const struct DataTable* const table);

// filter a single column by name and return a (newly-allocated) table
// containing the rows that matched the filter callback.
// returns NULL on failure (e.g., out of memory)
struct DataTable*
dt_table_filter_by_name(
	const struct DataTable* const table,
	const char* const column,
	void* user_data,
	bool (*filter_callback)(void* item, void* user_data));

// filter multiple columns specified by [column_names] and return a
// (newly-allocated) table containing the rows where AT LEAST ONE COLUMN
// matched the filter callback.
// returns NULL on failure.
struct DataTable*
dt_table_filter_OR_by_name(
	const struct DataTable* const table,
	const char(*column_names)[MAX_COL_LEN],
	const size_t n_columns,
	void* user_data,
	bool (**filter_callback)(void* item, void* user_data));

// filter multiple columns specified by [column_names] and return a
// (newly-allocated) table containing the rows where ALL COLUMNS matched 
// the filter callback.
// returns NULL on failure.
struct DataTable*
dt_table_filter_AND_by_name(
	const struct DataTable* const table,
	const char(*column_names)[MAX_COL_LEN],
	const size_t n_columns,
	void* user_data,
	bool (**filter_callback)(void* item, void* user_data));

// filter a single column by index and return a (newly-allocated) table
// containing the rows that matched the filter callback.
// returns NULL on failure (e.g., out of memory)
struct DataTable*
dt_table_filter_by_idx(
	const struct DataTable* const table,
	const size_t column_idx,
	void* user_data,
	bool (*filter_callback)(void* item, void* user_data));

// filter multiple columns by index and return a (newly-allocated) table
// containing the rows where matched AT LEAST ONE COLUMN
// matched the filter callback for the specified column (passed as an
// array of function pointers).
// returns NULL on failure (e.g., out of memory)
struct DataTable*
dt_table_filter_OR_by_idx(
	const struct DataTable* const table,
	const size_t* column_indices,
	const size_t n_columns,
	void* user_data,
	bool (**filter_callback)(void* item, void* user_data));

// filter multiple columns by index and return a (newly-allocated) table
// containing the rows where ALL COLUMNS matched the filter callback
// for the specified column (passed as an array of function pointers).
// returns NULL on failure (e.g., out of memory)
struct DataTable*
dt_table_filter_AND_by_idx(
	const struct DataTable* const table,
	const size_t* column_indices,
	const size_t n_columns,
	void* user_data,
	bool (**filter_callback)(void* item, void* user_data));

// return a pointer to a column (NOT a copy).
// returns NULL if column is not found.
struct DataColumn*
dt_table_get_column_ptr_by_name(
	const struct DataTable* const table,
	const char* const column_name);

// return a pointer to a column (NOT a copy).
// returns NULL if index is out of bounds.
struct DataColumn*
dt_table_get_column_ptr_by_index(
	const struct DataTable* const table,
	const size_t column_idx);

// return deep copy of entire table.
// returns NULL on failure.
struct DataTable*
dt_table_copy(
	const struct DataTable* const table);

// append two tables together (dest is modified inplace)
// returns DT_SIZE_MISMATCH if n_columns don't match
// returns DT_TYPE_MISMATCH if each column do not have same types
// returns DT_SUCCESS otherwise
enum status_code_e
dt_table_append_single(
	struct DataTable* const dest,
	const struct DataTable* const src);

// append multiple tables together (dest is modified inplace)
// by specifying n_tables and passing all tables to append as additional args.
// returns DT_SIZE_MISMATCH if n_columns don't match between dest and any of the other tables
// returns DT_TYPE_MISMATCH if column types don't match between dest and any of the other tables
// returns DT_SUCCESS otherwise
enum status_code_e
dt_table_append(
	struct DataTable* const dest,
	const size_t n_tables,
	...);

// check whether or not two rows are equal.
// more of an internal function but exposing publicly.
bool
dt_table_rows_equal(
	const struct DataTable* table1,
	const size_t row_idx_1,
	const struct DataTable* table2,
	const size_t row_idx_2);	

// return a newly-allocated table with all the distinct rows.
// returns NULL on failure (e.g., out of memory)
struct DataTable*
dt_table_distinct(
	const struct DataTable* table);

// insert new column into table.
// NOTE: the column is COPIED when inserted; ownership is NOT transferred.
// returns DT_ALLOC_ERROR if failed to resize.
// returns DT_SiZE_MISMATCH if column's rows do not match table.
// returns DT_SUCCESS otherwise
enum status_code_e
dt_table_insert_column(
	struct DataTable* table,
	const struct DataColumn* const column,
	const char* const column_name);

#endif
