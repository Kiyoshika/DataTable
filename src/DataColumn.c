#include "DataColumn.h"

char* dt_type_to_str(
	const enum data_type_e type)
{
	switch (type)
	{
		case FLOAT:
			return "FLOAT";
		case DOUBLE:
			return "DOUBLE";
		case INT8:
			return "INT8";
		case INT16:
			return "INT16";
		case INT32:
			return "INT32";
		case INT64:
			return "INT64";
		case UINT8:
			return "UINT8";
		case UINT16:
			return "UINT16";
		case UINT32:
			return "UINT32";
		case UINT64:
			return "UINT64";
	}

	return "UNKNOWN";
}

static size_t get_type_size(
	const enum data_type_e type)
{
	switch (type)
	{
		case FLOAT:
			return sizeof(float);
		case DOUBLE:
			return sizeof(double);
		case INT8:
			return sizeof(int8_t);
		case INT16:
			return sizeof(int16_t);
		case INT32:
			return sizeof(int32_t);
		case INT64:
			return sizeof(int64_t);
		case UINT8:
			return sizeof(uint8_t);
		case UINT16:
			return sizeof(uint16_t);
		case UINT32:
			return sizeof(uint32_t);
		case UINT64:
			return sizeof(uint64_t);
	}

	return 0;
}

static void* get_index_ptr(
	const struct DataColumn* const column,
	const size_t index)
{
	return ((char*)column->value + index * column->type_size);
}

enum status_code_e
dt_column_create(
	struct DataColumn** column,
	const size_t capacity,
	const enum data_type_e type)
{
	(*column) = malloc(sizeof(**column));
	if (!*column)
		return DT_ALLOC_ERROR;

	// NOTE: *2 + 1 to handle the case of 0 (empty column, we still want the ability to append)
	(*column)->value = calloc(capacity * 2 + 1, get_type_size(type));
	if (!(*column)->value)
	{
		free(*column);
		*column = NULL;
		return DT_ALLOC_ERROR;
	}
	(*column)->type = type;
	(*column)->type_size = get_type_size(type);
	(*column)->n_values = capacity;
	(*column)->value_capacity = capacity * 2 + 1;

	return DT_SUCCESS;
}

void
dt_column_free(
	struct DataColumn** column)
{
	free((*column)->value);
	(*column)->value = NULL;

	(*column)->n_values = 0;
	(*column)->value_capacity = 0;

	free(*column);
	*column = NULL;
}

enum status_code_e
dt_column_set_value(
	struct DataColumn* const column,
	const size_t index,
	const void * const value)
{
	if (index >= column->n_values)
		return DT_INDEX_ERROR;

	void* value_at = get_index_ptr(column, index);
	memcpy(value_at, value, column->type_size);

	return DT_SUCCESS;
}

enum status_code_e
dt_column_append_value(
	struct DataColumn* const column,
	const void * const value)
{
	void* value_at = get_index_ptr(column, column->n_values);
	memcpy(value_at, value, column->type_size);

	column->n_values++;
	if (column->n_values >= column->value_capacity)
	{
		void* alloc = realloc(column->value, column->value_capacity * 2 * column->type_size);
		if (!alloc)
			return DT_ALLOC_ERROR;
		column->value = alloc;
		column->value_capacity *= 2;
		for (size_t i = column->n_values; i < column->value_capacity; ++i)
			memset(get_index_ptr(column, i), 0, column->type_size);
	}

	return DT_SUCCESS;
}

enum status_code_e
dt_column_get_value(
	const struct DataColumn* const column,
	const size_t index,
	void* const value)
{
	if (index >= column->n_values)
		return DT_INDEX_ERROR;

	memcpy(value, get_index_ptr(column, index), column->type_size);

	return DT_SUCCESS;
}

void
dt_column_fill_values(
	struct DataColumn* const column,
	const void* const value)
{
	for (size_t i = 0; i < column->n_values; ++i)
		dt_column_set_value(column, i, value);
}

void
dt_column_iterate_rows(
	struct DataColumn* const column,
	void* user_data,
	void (*user_callback)(void* item, void* user_data))
{
	for (size_t i = 0; i < column->n_values; ++i)
		user_callback(get_index_ptr(column, i), user_data);
}

struct DataColumn*
dt_column_copy(
	const struct DataColumn* const column)
{
	struct DataColumn* copy_column = NULL;
	if (!dt_column_create(&copy_column, column->n_values, column->type))
		return NULL;

	for (size_t i = 0; i < column->n_values; ++i)
	{
		void* source = get_index_ptr(column, i);
		void* dest = get_index_ptr(copy_column, i);
		memcpy(dest, source, column->type_size);
	}

	return copy_column;
}

size_t*
dt_column_filter(
	const struct DataColumn* const column,
	void* user_data,
	bool (*filter_callback)(void* item, void* user_data),
	ssize_t* n_items_returned)
{
	// initially allocate a size_t array as large as column.
	// then after filtering is finished, reallocate to the appropriate size.
	*n_items_returned = 0;

	size_t* filtered_idx = calloc(column->n_values, sizeof(size_t));
	if (!filtered_idx)
		goto alloc_fail;

	size_t current_idx = 0;

	for (size_t i = 0; i < column->n_values; ++i)
		if (filter_callback(get_index_ptr(column, i), user_data))
			filtered_idx[current_idx++] = i;

	*n_items_returned = current_idx;
	void* alloc = realloc(filtered_idx, *n_items_returned * sizeof(size_t));
	if (!alloc)
		goto alloc_fail;
	filtered_idx = alloc;
	return filtered_idx;

alloc_fail:
	*n_items_returned = -1;
	return NULL;
	
}

struct DataColumn*
dt_column_subset(
	const struct DataColumn* const column,
	const size_t* const indices,
	const size_t n_indices)
{
	if (n_indices == 0)
		return NULL;

	struct DataColumn* subset = NULL;
	dt_column_create(&subset, n_indices, column->type);

	for (size_t i = 0; i < n_indices; ++i)
	{
		if (indices[i] >= column->n_values)
			goto bad_index;

		void* source = get_index_ptr(column, indices[i]);
		void* dest = get_index_ptr(subset, i);
		memcpy(dest, source, column->type_size);
	}

	return subset;

bad_index:
	dt_column_free(&subset);
	return NULL;
}

enum status_code_e
dt_column_resize(
	struct DataColumn* const column,
	const size_t n_values)
{
	if (n_values > column->value_capacity)
	{
		void* alloc = realloc(column->value, n_values * column->type_size);
		if (!alloc)
			return DT_ALLOC_ERROR;
		column->value_capacity = n_values;
		column->value = alloc;

		for (size_t i = column->n_values; i < n_values; ++i)
			memset(get_index_ptr(column, i), 0, column->type_size);
	}

	column->n_values = n_values;

	return DT_SUCCESS;
}

enum status_code_e
dt_column_union(
	struct DataColumn* const dest,
	const struct DataColumn* const src)
{
	if (dest->type != src->type)
		return DT_TYPE_MISMATCH;

	size_t initial_size = dest->n_values;

	if (!dt_column_resize(dest, dest->n_values + src->n_values))
		return DT_ALLOC_ERROR;

	size_t src_idx = 0;
	for (; initial_size < dest->n_values; ++initial_size)
	{
		void* get = get_index_ptr(src, src_idx++);
		void* set = get_index_ptr(dest, initial_size);
		memcpy(set, get, src->type_size);
	}

	return DT_SUCCESS;
}

enum status_code_e
dt_column_union_multiple(
	struct DataColumn* const dest,
	const size_t n_columns,
	...)
{
	va_list src_columns;
	va_start(src_columns, n_columns);
	enum status_code_e status = DT_SUCCESS;

	for (size_t i = 0; i < n_columns; ++i)
	{
		status = dt_column_union(dest, va_arg(src_columns, const struct DataColumn* const));
		if (status != DT_SUCCESS)
			goto cleanup;
	}
	
cleanup:
	va_end(src_columns);
	return status;
}

void
dt_column_sum(
	const struct DataColumn* const column,
	void* result)
{
	switch (column->type)
	{
		case UINT8:
			_agg_loop(column, result, uint8_t, _sum_item);
			break;
		case UINT16:
			_agg_loop(column, result, uint16_t, _sum_item);
			break;
		case UINT32:
			_agg_loop(column, result, uint32_t, _sum_item);
			break;
		case UINT64:
			_agg_loop(column, result, uint64_t, _sum_item);
			break;
		case INT8:
			_agg_loop(column, result, int8_t, _sum_item);
			break;
		case INT16:
			_agg_loop(column, result, int16_t, _sum_item);
			break;
		case INT32:
			_agg_loop(column, result, int32_t, _sum_item);
			break;
		case INT64:
			_agg_loop(column, result, int64_t, _sum_item);
			break;
		case FLOAT:
			_agg_loop(column, result, float, _sum_item);
			break;
		case DOUBLE:
			_agg_loop(column, result, double, _sum_item);
			break;
	}
}

void
dt_column_max(
	const struct DataColumn* const column,
	void* result)
{
	// start maximum at first item
	memcpy(result, get_index_ptr(column, 0), column->type_size);

	switch (column->type)
	{
		case UINT8:
			_agg_loop(column, result, uint8_t, _max_item);
			break;
		case UINT16:
			_agg_loop(column, result, uint16_t, _max_item);
			break;
		case UINT32:
			_agg_loop(column, result, uint32_t, _max_item);
			break;
		case UINT64:
			_agg_loop(column, result, uint64_t, _max_item);
			break;
		case INT8:
			_agg_loop(column, result, int8_t, _max_item);
			break;
		case INT16:
			_agg_loop(column, result, int16_t, _max_item);
			break;
		case INT32:
			_agg_loop(column, result, int32_t, _max_item);
			break;
		case INT64:
			_agg_loop(column, result, int64_t, _max_item);
			break;
		case FLOAT:
			_agg_loop(column, result, float, _max_item);
			break;
		case DOUBLE:
			_agg_loop(column, result, double, _max_item);
			break;
	}
}

void
dt_column_min(
	const struct DataColumn* const column,
	void* result)
{
	// start minimum at first item
	memcpy(result, get_index_ptr(column, 0), column->type_size);

	switch (column->type)
	{
		case UINT8:
			_agg_loop(column, result, uint8_t, _min_item);
			break;
		case UINT16:
			_agg_loop(column, result, uint16_t, _min_item);
			break;
		case UINT32:
			_agg_loop(column, result, uint32_t, _min_item);
			break;
		case UINT64:
			_agg_loop(column, result, uint64_t, _min_item);
			break;
		case INT8:
			_agg_loop(column, result, int8_t, _min_item);
			break;
		case INT16:
			_agg_loop(column, result, int16_t, _min_item);
			break;
		case INT32:
			_agg_loop(column, result, int32_t, _min_item);
			break;
		case INT64:
			_agg_loop(column, result, int64_t, _min_item);
			break;
		case FLOAT:
			_agg_loop(column, result, float, _min_item);
			break;
		case DOUBLE:
			_agg_loop(column, result, double, _min_item);
			break;
	}
}

void
dt_column_avg(
	const struct DataColumn* const column,
	void* result)
{
	switch (column->type)
	{
		case UINT8:
			_agg_loop_divide_size(column, result, uint8_t, _sum_item);
			break;
		case UINT16:
			_agg_loop_divide_size(column, result, uint16_t, _sum_item);
			break;
		case UINT32:
			_agg_loop_divide_size(column, result, uint32_t, _sum_item);
			break;
		case UINT64:
			_agg_loop_divide_size(column, result, uint64_t, _sum_item);
			break;
		case INT8:
			_agg_loop_divide_size(column, result, int8_t, _sum_item);
			break;
		case INT16:
			_agg_loop_divide_size(column, result, int16_t, _sum_item);
			break;
		case INT32:
			_agg_loop_divide_size(column, result, int32_t, _sum_item);
			break;
		case INT64:
			_agg_loop_divide_size(column, result, int64_t, _sum_item);
			break;
		case FLOAT:
			_agg_loop_divide_size(column, result, float, _sum_item);
			break;
		case DOUBLE:
			_agg_loop_divide_size(column, result, double, _sum_item);
			break;
	}
}
