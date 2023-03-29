#ifndef DATA_COLUMN_H
#define DATA_COLUMN_H

#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <stdarg.h>

#include "StatusCodes.h"

enum data_type_e
{
	FLOAT,
	DOUBLE,
	INT8,
	INT16,
	INT32,
	INT64,
	UINT8,
	UINT16,
	UINT32,
	UINT64,
	STRING
};

// convert data type enum to string representation.
char* dt_type_to_str(
	const enum data_type_e type);

struct DataColumn
{
	enum data_type_e type;
	size_t type_size;
	void* value;
	size_t n_values;
	size_t value_capacity;
	void (*deallocator)(void*);
};

// pass a NULL-initialized DataTable to allocate [capacity] number of items.
// this will create n_values = capacity all equal to zero.
// size of each item is determined by the passed type.
// returns DT_ALLOC_ERROR if couldn't properly allocate memory, DT_SUCCESS otherwise.
enum status_code_e
dt_column_create(
	struct DataColumn** column,
	const size_t capacity,
	const enum data_type_e type);

// copy the address of a value into a specified position.
// if value is NULL, 0 will be written instead.
// returns DT_INDEX_ERROR if index is out of bounds, DT_SUCCESS otherwise.
enum status_code_e
dt_column_set_value(
	struct DataColumn* const column,
	const size_t index,
	const void * const value);

// append the address of a value to the column, dynamically growing in size.
// if value is NULL, 0 will be written instead.
// returns DT_ALLOC_ERROR if there was a problem resizing, DT_SUCCESS otherwise.
enum status_code_e
dt_column_append_value(
	struct DataColumn* const column,
	const void * const value);

// fetch the item at specified position and copy its contents into value pointer.
// returns DT_INDEX_ERROR if index is out of bounds, DT_SUCCESS otherwise.
enum status_code_e
dt_column_get_value(
	const struct DataColumn* const column,
	const size_t index,
	void* const value);

void*
dt_column_get_value_ptr(
	const struct DataColumn* const column,
	const size_t index);

// fill all values of a column with a specified value
void
dt_column_fill_values(
	struct DataColumn* const column,
	const void* const value);

// sum the items in a column and write the result into result pointer.
// the items will automatically be cast according to the type (float, integer, etc) so be sure your result pointer is typed appropriately.
// returns nothing.
void
dt_column_sum(
	const struct DataColumn* const column,
	void* const result);

// free memory allocated by column and reset all of its contents (just in case)
void
dt_column_free(
	struct DataColumn** column);

// apply user callback function on each row within a column (optionally passing custom user data)
void
dt_column_iterate_rows(
	struct DataColumn* const column,
	void* user_data,
	void (*user_callback)(void* item, void* user_data));

// copy contents of a column into a newly-allocated column.
// returns NULL on failure (e.g., allocation issue).
struct DataColumn*
dt_column_copy(
	const struct DataColumn* const column);

// iterate each row of column and apply a user callback (with optional user data) and return an array of 0/1s indicating if the row should be kept or not. The size of the returned array is the same as column->n_values. 
// if an allocation error occurrs, NULL is returned 
size_t*
dt_column_filter(
	const struct DataColumn* const column,
	void* user_data,
	bool (*filter_callback)(void* item, void* user_data));

// subset a column by a "boolean" size_t array containing 0s and 1s.
// will skip rows that are 0 and keep the 1s and return a newly-allocated column
// containing selected values. Assumes the boolean_idx array is the same size
// as the column.
// returns NULL on failure (e.g., allocation issue).
struct DataColumn*
dt_column_subset_by_boolean(
	const struct DataColumn* const column,
	const size_t* const boolean_idx);


// subset a column by specific indices which returns a newly-allocated column or returns NULL on failure (e.g., if one of the indices is out of bounds).
struct DataColumn*
dt_column_subset_by_index(
	const struct DataColumn* const column,
	const size_t* const indices,
	const size_t n_indices);

// resize a column to new size with n_values.
// returns DT_ALLOC_ERROR if reallocation fails, DT_SUCCESS otherwise.
// if new size is bigger, values are defaulted to 0.
enum status_code_e
dt_column_resize(
	struct DataColumn* const column,
	const size_t n_values);

// union [src] column into [dest] column (will be resized).
// returns DT_ALLOC_ERROR if unable to reallocate memory,
// returns DT_TYPE_MISMATCH if types don't match
// returns DT_SUCCESS otherwise
enum status_code_e
dt_column_union(
	struct DataColumn* const dest,
	const struct DataColumn* const src);

// union multiple columns [src] into [dest] column (will be resized).
// returns DT_ALLOC_ERROR if unable to reallocate memory.
// returns DT_TYPE_MISMATCH if types don't match
// returns DT_SUCCESS otherwise
enum status_code_e
dt_column_union_multiple(
	struct DataColumn* const dest,
	const size_t n_columns,
	...);

// a macro for handling arbitrary types to sum values in a column
#define _sum_item(item, user_data, type) \
	type* _item = item; \
	type* _data = user_data; \
	*_data += *_item;

#define _max_item(item, user_data, type) \
	type* _item = item; \
	type* _data = user_data; \
	if (*_item > *_data) \
		*_data = *_item;

#define _min_item(item, user_data, type) \
	type* _item = item; \
	type* _data = user_data; \
	if (*_item < *_data) \
		*_data = *_item;

// a macro for iterating over a column and calling an arbitrary aggregation macro
#define _agg_loop(column, user_data, type, agg_func) \
	for (size_t i = 0; i < column->n_values; ++i) \
    { \
		agg_func(get_index_ptr(column, i), user_data, type); \
	} \

// a macro for iterating over a column and calling an arbitrary aggregation macro
// then dividing by its size
#define _agg_loop_divide_size(column, user_data, type, agg_func) \
	for (size_t i = 0; i < column->n_values; ++i) \
    { \
		agg_func(get_index_ptr(column, i), user_data, type); \
	} \
    { \
		type* _data = user_data; \
		*_data /= column->n_values; \
	}

// sum all the values in a column and store result into [result].
// the type is determined by column->type.
void
dt_column_sum(
	const struct DataColumn* const column,
	void* result);

// get the maximum value in a column and store the result into [result].
// the type is determined by column->type.
void
dt_column_max(
	const struct DataColumn* const column,
	void* result);

// get the minimum value in a column and store the result into [result].
// the type is determined by column->type.
void
dt_column_min(
	const struct DataColumn* const column,
	void* result);

// get the average value in a column and store the result into [result].
// the type is determined by column->type.
void
dt_column_avg(
	const struct DataColumn* const column,
	void* result);

// for each row in [dest], add the value from the corresponding row from [src].
// dest += src (for each row).
// returns DT_SIZE_MISMATCH if columns are different sizes.
// returns DT_TYPE_MISMATCH if columns are different types.
// returns DT_SUCCESS otherwise.
enum status_code_e
dt_column_add(
	struct DataColumn* const dest,
	const struct DataColumn* const src);

// for each row in [dest], subtract the value from the corresponding row from [src].
// dest -= src (for each row).
// returns DT_SIZE_MISMATCH if columns are different sizes.
// returns DT_TYPE_MISMATCH if columns are different types.
// returns DT_SUCCESS otherwise.
enum status_code_e
dt_column_subtract(
	struct DataColumn* const dest,
	const struct DataColumn* src);

// for each row in [dest], multiply the value from the corresponding row from [src].
// dest *= src (for each row).
// returns DT_SIZE_MISMATCH if columns are different sizes.
// returns DT_TYPE_MISMATCH if columns are different types.
// returns DT_SUCCESS otherwise.
enum status_code_e
dt_column_multiply(
	struct DataColumn* const dest,
	const struct DataColumn* src);

// for each row in [dest], divide the value from the corresponding row from [src].
// dest /= src (for each row).
// returns DT_SIZE_MISMATCH if columns are different sizes.
// returns DT_TYPE_MISMATCH if columns are different types.
// returns DT_SUCCESS otherwise.
enum status_code_e
dt_column_divide(
	struct DataColumn* const dest,
	const struct DataColumn* src);

#endif
