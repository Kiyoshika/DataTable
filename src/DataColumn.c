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
		case INT32:
			return "INT32";
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
		case INT32:
			return sizeof(int32_t);
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

	(*column)->value = calloc(capacity * 2, get_type_size(type));
	if (!(*column)->value)
	{
		free(*column);
		*column = NULL;
		return DT_ALLOC_ERROR;
	}
	(*column)->type = type;
	(*column)->type_size = get_type_size(type);
	(*column)->n_values = capacity;
	(*column)->value_capacity = capacity * 2;

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
