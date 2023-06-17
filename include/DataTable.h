#ifndef DATA_TABLE_H
#define DATA_TABLE_H

#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>

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
	const enum data_type_e* const data_types);

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

// insert a row from an array of pointers.
// this serves as an alternative to dt_table_insert_row and is mainly used internally.
// returns DT_ALLOC_ERROR if there was a problem resizing.
// returns DT_SUCCESS otherwise.
enum status_code_e
dt_table_insert_row_from_array(
	struct DataTable* const table,
	void* items);

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
const void*
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
dt_table_append_by_row(
	struct DataTable* const dest,
	const struct DataTable* const src);

// append multiple tables together (dest is modified inplace)
// by specifying n_tables and passing all tables to append as additional args.
// returns DT_SIZE_MISMATCH if n_columns don't match between dest and any of the other tables
// returns DT_TYPE_MISMATCH if column types don't match between dest and any of the other tables
// returns DT_SUCCESS otherwise
enum status_code_e
dt_table_append_multiple_by_row(
	struct DataTable* const dest,
	const size_t n_tables,
	...);

// check whether or not two rows are equal.
// more of an internal function but exposing publicly.
bool
dt_table_rows_equal(
	const struct DataTable* table1,
	const size_t row_idx_1,
  const size_t* const table1_column_indices,
	const struct DataTable* table2,
	const size_t row_idx_2,
  const size_t* const table2_column_indices,
  const size_t n_column_indices);	

// return a newly-allocated table with all the distinct rows.
// returns NULL on failure (e.g., out of memory)
struct DataTable*
dt_table_distinct(
	const struct DataTable* table);

// insert new column into table.
// NOTE: the column is COPIED when inserted; ownership is NOT transferred.
//
// returns DT_ALLOC_ERROR if failed to resize.
// returns DT_SiZE_MISMATCH if column's rows do not match table.
// returns DT_DUPLICATE if column name already exists
// returns DT_SUCCESS otherwise
enum status_code_e
dt_table_insert_column(
	struct DataTable* table,
	const struct DataColumn* const column,
	const char* const column_name);

// take an array of column indices and drop them inplace (the column
// indices will be shifted/adjusted accordingly)
enum status_code_e
dt_table_drop_columns_by_index(
	struct DataTable* table,
	const size_t n_columns,
	size_t* column_indices);

// take an array of columns and drop them inplace (the column
// indices will be shifted/adjusted accordingly)
enum status_code_e
dt_table_drop_columns_by_name(
	struct DataTable* table,
	const size_t n_columns,
	const char (*columns)[MAX_COL_LEN]);

// drop any columns containing at least one NULL value in a row.
// modifies the table inplace
void
dt_table_drop_columns_with_null(
	struct DataTable* table);

// drop any rows containing at least one NULL value in a column
// modifies the table inplace
enum status_code_e
dt_table_drop_rows_with_null(
	struct DataTable* table);

// apply a user-defined callback function on column [column_name].
// optionally pass custom [user_data] and/or reference columns within the table with [column_value_names].
// when using [column_value_names], each column provided (in order) will correspond to a column_value[i].
// i.e., if passing { "col1", "col3" } in [column_value_names] then column_value[0] --> col1 and column_value[1] --> col3
//
// NOTE: cannot apply function if column contains any NULL values
//
// returns DT_ALLOC_ERROR if there was a problem allocating memory
// returns DT_FAILURE for other general errors
// returns DT_SUCCESS otherwise
enum status_code_e
dt_table_apply_column(
	struct DataTable* const table,
	const char* const column_name,
	void (*callback)(void* current_row_value, void* user_data, const void** const column_values),
	void* user_data,
	const char (*column_value_names)[MAX_COL_LEN],
	const size_t n_column_values);

// apply a user-defined callback function on ALL cells within a table.
// optionally pass custom [user_data].
void
dt_table_apply_all(
	struct DataTable* const table,
	void (*callback)(void* current_cell_value, void* user_data),
	void* user_data);

// fill column's values by name.
// returns DT_COLUMN_NOT_FOUND if column isn't found.
// returns DT_SUCCESS otherwise.
enum status_code_e
dt_table_fill_column_values_by_name(
	struct DataTable* const table,
	const char* const column_name,
	const void* const value);

// fill columns' values by index
// returns DT_INDEX_ERROR if index is out of bounds.
// returns DT_SUCCESS otherwise.
enum status_code_e
dt_table_fill_column_values_by_index(
	struct DataTable* const table,
	const size_t column_index,
	const void* const value);

// fill entire table with a value.
void
dt_table_fill_all_values(
	struct DataTable* const table,
	const void* const value);

// replace NULL values in a specified column with the given value.
// returns DT_COLUMN_NOT_FOUND if column is not found.
// returns DT_SUCCESS otherwise.
enum status_code_e
dt_table_replace_column_null_values_by_name(
	struct DataTable* const table,
	const char* const column_name,
	const void* const value);

// replace NULL values in a specified column with the given value.
// returns DT_INDEX_ERROR if index is out of bounds.
// returns DT_SUCCESS otherwise.
enum status_code_e
dt_table_replace_column_null_values_by_index(
	struct DataTable* const table,
	const size_t column_index,
	const void* const value);

// replace ALL NULL values in entire table with the given value.
void
dt_table_replace_all_null_values(
	struct DataTable* const table,
	const void* const value);

// sample rows from a data table with or without replacement.
// if NOT sampling with replacement, then n_samples must be <= n_rows.
// returns a newly-allocated data table containing the sampled rows.
// returns NULL on failure (e.g., memory allocation or invalid size)
struct DataTable*
dt_table_sample_rows(
	const struct DataTable* const table,
	const size_t n_samples,
	const bool with_replacement);

// split a table in two according to a proportion between 0 and 1 (exclusive).
// 
// e.g., if proportion is 75%, then 75% of the records will go into split1
// and remaining 25% into split2.
// 
// split1 and split2 MUST be NULL pointers (they will be allocated in the function).
//
// returns DT_BAD_ARG is proportion is not in open interval (0.00f, 1.00f)
// returns DT_BAD_ARG if either split1 or split2 is not NULL
// returns DT_ALLOC_ERROR if couldn't allocate enough memory
// returns DT_SUCCESS otherwise
enum status_code_e
dt_table_split(
	const struct DataTable* const table,
	const float proportion,
	struct DataTable** split1,
	struct DataTable** split2);

// read and construct a data table from a CSV file.
// NOTE: assumes first row is a header row to determine column names
// NOTE: handles a maximum of 4095 characters per line (will be truncated)
// NOTE: will ignore delimiters inside single (') or double (") quotes.
//
// user can optionally pass the column types (if so, need to provide n_columns)
// to set the appropriate data types for each column.
// otherwise, we naively assume int64_t for all int-like types, double for all decimals and STRING for anything else.
//
// if you pass NULL to column_types, you can set n_columns to 0 (it's ignored anyways)
//
// returns NULL on failure
struct DataTable*
dt_table_read_csv(
	const char* const filepath,
	const char delim,
	const enum data_type_e* const column_types);

// append two tables by columns (horizontally).
// tables MUST have same number of rows.
// [dest] table is modified inplace with the new columns appended
//
// returns DT_SIZE_MISMATCH if rows are different
// returns DT_ALLOC_ERROR if couldn't allocate enough memory
// returns DT_DUPLICATE if column name already exists (only if allow_duplicate_column_names = false)
// returns DT_SUCCESS otherwise
enum status_code_e
dt_table_append_by_column(
	struct DataTable* const dest,
	const struct DataTable* const src);

// append multiple tables by columns (horizontally).
// each table MUST have same number of rows.
// [dest] table is modified inplace with the new columns appended
//
// returns DT_SIZE_MISMATCH if any of the tables' rows are different
// returns DT_ALLOC_ERROR if couldn't allocate enough memory
// returns DT_DUPLICATE if column name already exists (only if allow_duplicate_column_names = false)
// returns DT_SUCCESS otherwise
enum status_code_e
dt_table_append_multiple_by_column(
	struct DataTable* const dest,
	const size_t n_tables,
	...);

// cast one more more columns to a new data type.
// pass the number of columns to cast along with the array
// of column names and the array of NEW column data types.
// 
// returns DT_COLUMN_NOT_FOUND if one of the columns isn't found.
// returns DT_SUCCESS otherwise
enum status_code_e
dt_table_cast_columns(
	struct DataTable* const table,
	const size_t n_columns,
	const char (*column_names)[MAX_COL_LEN],
	const enum data_type_e* new_column_types);

struct DataTable*
dt_table_join_inner(
  const struct DataTable* const left_table,
  const struct DataTable* const right_table,
  const char (*join_columns)[MAX_COL_LEN],
  const size_t n_join_columns);

struct DataTable*
dt_table_join_left(
  const struct DataTable* const left_table,
  const struct DataTable* const right_table,
  const char (*join_columns)[MAX_COL_LEN],
  const size_t n_join_columns);

struct DataTable*
dt_table_join_right(
  const struct DataTable* const left_table,
  const struct DataTable* const right_table,
  const char (*join_columns)[MAX_COL_LEN],
  const size_t n_join_columns);

struct DataTable*
dt_table_join_full(
  const struct DataTable* const left_table,
  const struct DataTable* const right_table,
  const char (*join_columns)[MAX_COL_LEN],
  const size_t n_join_columns);

bool
dt_table_check_isnull(
  const struct DataTable* const table,
  const size_t row_idx,
  const size_t col_idx);

bool
dt_table_row_contains_null(
  const struct DataTable* const table,
  const size_t row_idx);

#endif
