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
		case STRING:
			return "STRING";
	}

	return "UNKNOWN";
}

size_t
dt_type_to_size(
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
		case STRING:
			return sizeof(char*);
	}

	return 0;
}

static void* 
get_index_ptr(
	const struct DataColumn* const column,
	const size_t index)
{
	return ((char*)column->value + index * column->type_size);
}

static enum status_code_e
__insert_null_value(
	struct DataColumn* column,
	const size_t index)
{
	if (!column->null_value_indices)
	{
		column->n_null_values = 0;
		column->null_value_capacity = 1;
		column->null_value_indices = calloc(1, sizeof(size_t));
		if (!column->null_value_indices)
			return DT_ALLOC_ERROR;
	}

	column->null_value_indices[column->n_null_values++] = index;
	if (column->n_null_values == column->null_value_capacity)
	{
		size_t new_capacity = column->null_value_capacity *= 2;
		void* alloc = realloc(column->null_value_indices, new_capacity * sizeof(size_t));
		if (!alloc)
			return DT_ALLOC_ERROR;
		column->null_value_indices = alloc;
		column->null_value_capacity = new_capacity;
	}

	void* value_at = get_index_ptr((const struct DataColumn* const)column, index);
	memset(value_at, 0, column->type_size);

	return DT_SUCCESS;
}

static void
dt_string_dealloc(
	void* item)
{
	char** _item = item;
	free(*_item);
	*_item = NULL;
}

static bool
__is_null_value_index(
	const struct DataColumn* const column,
	const size_t index)
{
	for (size_t i = 0; i < column->n_null_values; ++i)
		if (column->null_value_indices[i] == index)
			return true;

	return false;
}

static void
__remove_null_value_index(
	struct DataColumn* const column,
	const size_t index)
{
	for (size_t i = 0; i < column->n_null_values - 1; ++i)
		column->null_value_indices[i] = column->null_value_indices[i + 1];

	// technically doesn't matter since we'll be decrementing the
	// logical size, but setting it to zero anyways (could prevent
	// confusion if debugging.)
	column->null_value_indices[column->n_null_values - 1] = 0;
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
	(*column)->value = calloc(capacity * 2 + 1, dt_type_to_size(type));
	if (!(*column)->value)
	{
		free(*column);
		*column = NULL;
		return DT_ALLOC_ERROR;
	}
	(*column)->type = type;
	(*column)->type_size = dt_type_to_size(type);
	(*column)->n_values = capacity;
	(*column)->value_capacity = capacity * 2 + 1;

	if (type == STRING)
		(*column)->deallocator = &dt_string_dealloc;
	else
		(*column)->deallocator = NULL;

	(*column)->null_value_indices = NULL;
	(*column)->n_null_values = 0;
	(*column)->null_value_capacity = 1;

	return DT_SUCCESS;
}

void
dt_column_free(
	struct DataColumn** column)
{
	if ((*column)->deallocator)
		for (size_t i = 0; i < (*column)->n_values; ++i)
			dt_string_dealloc(get_index_ptr(*column, i));

	free((*column)->value);
	(*column)->value = NULL;

	(*column)->n_values = 0;
	(*column)->value_capacity = 0;

	if ((*column)->null_value_indices)
	{
		(*column)->n_null_values = 0;
		(*column)->null_value_capacity = 1;
		free((*column)->null_value_indices);
		(*column)->null_value_indices = NULL;
	}

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

	// if string type, be sure to deallocate before re-writing
	if (column->type == STRING)
	{
		char** value_str = value_at;
		free(*value_str);
	}
	
	if (!value)
	{
		memset(value_at, 0, column->type_size);
		enum status_code_e status = __insert_null_value(column, index);
		if (status != DT_SUCCESS)
			return status;
	}
	else
		memcpy(value_at, value, column->type_size);

	// overwriting NULL value with non-null value,
	// remove the index from the null values array and
	// decrement null value counter
	if (value && __is_null_value_index(column, index))
	{
		__remove_null_value_index(column, index);
		column->n_null_values--;
	}

	return DT_SUCCESS;
}

enum status_code_e
dt_column_append_value(
	struct DataColumn* const column,
	const void * const value)
{
	void* value_at = get_index_ptr(column, column->n_values);
	if (!value)
	{
		memset(value_at, 0, column->type_size);
		enum status_code_e status = __insert_null_value(column, column->n_values);
		if (status != DT_SUCCESS)
			return status;
	}
	else
	{
		// if column type is heap-allocated, a NEW copy is created
		// before appending into column
		if (column->type == STRING)
		{
			const char** const value_str_addr = (const char** const)value;
			const char* const value_str = strdup(*value_str_addr);
			memcpy(value_at, &value_str, column->type_size);
		}
		else
			memcpy(value_at, value, column->type_size);
	}

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
dt_column_append(
	struct DataColumn* const dest,
	const struct DataColumn* const src)
{
	if (dest->type != src->type)
		return DT_TYPE_MISMATCH;

	for (size_t i = 0; i < src->n_values; ++i)
	{
		enum status_code_e status = dt_column_append_value(
				dest,
				get_index_ptr(src, i));

		if (status != DT_SUCCESS)
			return status;
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

void*
dt_column_get_value_ptr(
	const struct DataColumn* const column,
	const size_t index)
{
	if (index >= column->n_values)
		return NULL;

	return get_index_ptr(column, index);
}

void
dt_column_fill_values(
	struct DataColumn* const column,
	const void* const value)
{
	for (size_t i = 0; i < column->n_values; ++i)
	{
		// if string type, each need to be separately heap-allocated
		if (column->type == STRING)
		{
			char* value_str = strdup(*(const char** const)value);
			dt_column_set_value(column, i, &value_str);
		}
		else
			dt_column_set_value(column, i, value);
	}

	// free the original string user provided since it's been copied
	// into every row and no longer needed
	if (column->type == STRING)
	{
		char** original = (char**)value; // cast to avoid warnings about const-ness
		free(*original);
		*original = NULL;
	}
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
		// creating a deep copy, so if the data is heap-allocated,
		// we need to heap-allocate a copy as well
		if (column->type == STRING)
		{
			char** source_addr = source;
			char* source_content = strdup(*source_addr);
			memcpy(dest, &source_content, copy_column->type_size);
		}
		else
			memcpy(dest, source, column->type_size);
	}

	// copy null values (if any)
	if (column->n_null_values > 0)
	{
		copy_column->n_null_values = column->n_null_values;
		copy_column->null_value_capacity = column->n_null_values + 1;
		copy_column->null_value_indices = calloc(column->n_null_values + 1, sizeof(size_t));
		if (!copy_column->null_value_indices)
		{
			dt_column_free(&copy_column);
			return NULL;
		}

		for (size_t i = 0; i < column->n_null_values; ++i)
			copy_column->null_value_indices[i] = column->null_value_indices[i];
	}

	return copy_column;
}

size_t*
dt_column_filter(
	const struct DataColumn* const column,
	void* user_data,
	bool (*filter_callback)(void* item, void* user_data))
{
	// an array of 0/1 indicating whether or not to keep the row
	size_t* filtered_idx = calloc(column->n_values, sizeof(size_t));
	if (!filtered_idx)
		return NULL;

	for (size_t i = 0; i < column->n_values; ++i)
		if (filter_callback(get_index_ptr(column, i), user_data))
			filtered_idx[i] = 1;

	return filtered_idx;
}

struct DataColumn*
dt_column_subset_by_boolean(
	const struct DataColumn* const column,
	const size_t* const boolean_idx)
{
	// determine new size by summing up ones
	size_t new_size = 0;
	for (size_t i = 0; i < column->n_values; ++i)
		new_size += boolean_idx[i];

	struct DataColumn* subset = NULL;
	dt_column_create(&subset, new_size, column->type);

	if (!subset)
		return NULL;

	// start copying over values (for all the 1s; ignore 0s)
	size_t current_idx = 0;
	for (size_t i = 0; i < column->n_values; ++i)
	{
		if (boolean_idx[i] == 1)
		{
			void* src = get_index_ptr(column, i);
			void* dest = get_index_ptr(subset, current_idx++);
			memcpy(dest, src, column->type_size);
		}
	}

	return subset;
}

struct DataColumn*
dt_column_subset_by_index(
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

static int
sizet_compare(
	const void* a,
	const void* b)
{
	const size_t* _a = a;
	const size_t* _b = b;

	if (*_a > *_b)
		return 1;
	else if (*_a < *_b)
		return -1;

	return 0;
}

struct DataColumn*
dt_column_drop_by_index(
	const struct DataColumn* const column,
	const size_t* const sorted_indices_ascending,
	const size_t n_indices)
{
	if (n_indices == 0)
		return NULL;

	// cannot drop more indices than possible values
	if (n_indices > column->n_values)
		return NULL;

	struct DataColumn* subset = NULL;
	dt_column_create(&subset, column->n_values - n_indices, column->type);

	size_t current_idx = 0;
	for (size_t i = 0; i < column->n_values; ++i)
	{
		// if index is NOT found, then we can copy the value
		if (!bsearch(&i, sorted_indices_ascending, n_indices, sizeof(size_t), &sizet_compare))
		{
			void* source = get_index_ptr(column, i);
			void* dest = get_index_ptr(subset, current_idx++);
			memcpy(dest, source, column->type_size);
		}
	}

	return subset;
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
		case STRING: // aggregates on strings not supported (at least, for now)
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
		case STRING: // aggregates on strings not supported (at least, for now)
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
		case STRING: // aggregates on strings not supported (at least, for now)
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
		case STRING: // aggregates on strings not supported (at least, for now)
			break;
	}
}


// macros to add values across two columns for arbitrary types
#define _add_values(dest, src, type) *(type*)dest += *(type*)src
#define _add_values_loop(dest, src, type) \
	for (size_t i = 0; i < dest->n_values; ++i) \
    { \
		_add_values(get_index_ptr(dest, i), get_index_ptr(src, i), type); \
	}

enum status_code_e
dt_column_add(
	struct DataColumn* const dest,
	const struct DataColumn* src)
{
	if (dest->n_values != src->n_values)
		return DT_SIZE_MISMATCH;

	if (dest->type != src->type)
		return DT_TYPE_MISMATCH;

	switch (dest->type)
	{
		case UINT8:
			_add_values_loop(dest, src, uint8_t);
			break;
		case UINT16:
			_add_values_loop(dest, src, uint16_t);
			break;
		case UINT32:
			_add_values_loop(dest, src, uint32_t);
			break;
		case UINT64:
			_add_values_loop(dest, src, uint64_t);
			break;
		case INT8:
			_add_values_loop(dest, src, int8_t);
			break;
		case INT16:
			_add_values_loop(dest, src, int16_t);
			break;
		case INT32:
			_add_values_loop(dest, src, int32_t);
			break;
		case INT64:
			_add_values_loop(dest, src, int64_t);
			break;
		case FLOAT:
			_add_values_loop(dest, src, float);
			break;
		case DOUBLE:
			_add_values_loop(dest, src, double);
			break;
		case STRING: // aggregates on strings not supported (at least, for now)
			break;
	}

	return DT_SUCCESS;
}

// macros to subtract values across two columns for arbitrary types
#define _subtract_values(dest, src, type) *(type*)dest -= *(type*)src
#define _subtract_values_loop(dest, src, type) \
	for (size_t i = 0; i < dest->n_values; ++i) \
    { \
		_subtract_values(get_index_ptr(dest, i), get_index_ptr(src, i), type); \
	}

enum status_code_e
dt_column_subtract(
	struct DataColumn* const dest,
	const struct DataColumn* src)
{
	if (dest->n_values != src->n_values)
		return DT_SIZE_MISMATCH;

	if (dest->type != src->type)
		return DT_TYPE_MISMATCH;

	switch (dest->type)
	{
		case UINT8:
			_subtract_values_loop(dest, src, uint8_t);
			break;
		case UINT16:
			_subtract_values_loop(dest, src, uint16_t);
			break;
		case UINT32:
			_subtract_values_loop(dest, src, uint32_t);
			break;
		case UINT64:
			_subtract_values_loop(dest, src, uint64_t);
			break;
		case INT8:
			_subtract_values_loop(dest, src, int8_t);
			break;
		case INT16:
			_subtract_values_loop(dest, src, int16_t);
			break;
		case INT32:
			_subtract_values_loop(dest, src, int32_t);
			break;
		case INT64:
			_subtract_values_loop(dest, src, int64_t);
			break;
		case FLOAT:
			_subtract_values_loop(dest, src, float);
			break;
		case DOUBLE:
			_subtract_values_loop(dest, src, double);
			break;
		case STRING: // aggregates on strings not supported (at least, for now)
			break;
	}

	return DT_SUCCESS;
}

// macros to multiply values across two columns for arbitrary types
#define _multiply_values(dest, src, type) *(type*)dest *= *(type*)src
#define _multiply_values_loop(dest, src, type) \
	for (size_t i = 0; i < dest->n_values; ++i) \
    { \
		_multiply_values(get_index_ptr(dest, i), get_index_ptr(src, i), type); \
	}

enum status_code_e
dt_column_multiply(
	struct DataColumn* const dest,
	const struct DataColumn* src)
{
	if (dest->n_values != src->n_values)
		return DT_SIZE_MISMATCH;

	if (dest->type != src->type)
		return DT_TYPE_MISMATCH;

	switch (dest->type)
	{
		case UINT8:
			_multiply_values_loop(dest, src, uint8_t);
			break;
		case UINT16:
			_multiply_values_loop(dest, src, uint16_t);
			break;
		case UINT32:
			_multiply_values_loop(dest, src, uint32_t);
			break;
		case UINT64:
			_multiply_values_loop(dest, src, uint64_t);
			break;
		case INT8:
			_multiply_values_loop(dest, src, int8_t);
			break;
		case INT16:
			_multiply_values_loop(dest, src, int16_t);
			break;
		case INT32:
			_multiply_values_loop(dest, src, int32_t);
			break;
		case INT64:
			_multiply_values_loop(dest, src, int64_t);
			break;
		case FLOAT:
			_multiply_values_loop(dest, src, float);
			break;
		case DOUBLE:
			_multiply_values_loop(dest, src, double);
			break;
		case STRING: // aggregates on strings not supported (at least, for now)
			break;
	}

	return DT_SUCCESS;
}

// macros to divide values across two columns for arbitrary types
#define _divide_values(dest, src, type) *(type*)dest /= *(type*)src
#define _divide_values_loop(dest, src, type) \
	for (size_t i = 0; i < dest->n_values; ++i) \
    { \
		_divide_values(get_index_ptr(dest, i), get_index_ptr(src, i), type); \
	}

enum status_code_e
dt_column_divide(
	struct DataColumn* const dest,
	const struct DataColumn* src)
{
	if (dest->n_values != src->n_values)
		return DT_SIZE_MISMATCH;

	if (dest->type != src->type)
		return DT_TYPE_MISMATCH;

	switch (dest->type)
	{
		case UINT8:
			_divide_values_loop(dest, src, uint8_t);
			break;
		case UINT16:
			_divide_values_loop(dest, src, uint16_t);
			break;
		case UINT32:
			_divide_values_loop(dest, src, uint32_t);
			break;
		case UINT64:
			_divide_values_loop(dest, src, uint64_t);
			break;
		case INT8:
			_divide_values_loop(dest, src, int8_t);
			break;
		case INT16:
			_divide_values_loop(dest, src, int16_t);
			break;
		case INT32:
			_divide_values_loop(dest, src, int32_t);
			break;
		case INT64:
			_divide_values_loop(dest, src, int64_t);
			break;
		case FLOAT:
			_divide_values_loop(dest, src, float);
			break;
		case DOUBLE:
			_divide_values_loop(dest, src, double);
			break;
		case STRING: // aggregates on strings not supported (at least, for now)
			break;
	}

	return DT_SUCCESS;
}
