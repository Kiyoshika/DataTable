#ifndef DATA_COLUMN_H
#define DATA_COLUMN_H

#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stddef.h>

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
	UINT64
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

#endif
