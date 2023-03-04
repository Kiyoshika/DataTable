#ifndef DATA_COLUMN_H
#define DATA_COLUMN_H

#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stddef.h>

#include "StatusCodes.h"

enum data_type_e
{
	// TODO: other types
	FLOAT,
	DOUBLE,
	INT32
};

// convert data type enum to string representation.
char* dt_type_to_str(
	const enum data_type_e type);

struct DataColumn
{
	enum data_type_e type;
	void* value;
	size_t n_values;
	size_t value_capacity;
};

// pass a NULL-initialized DataTable to allocate [capacity] number of items.
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

#endif
