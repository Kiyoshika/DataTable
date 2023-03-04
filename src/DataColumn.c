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

enum status_code_e
dt_column_create(
	struct DataColumn** column,
	const size_t capacity,
	const enum data_type_e type)
{
	(*column) = malloc(sizeof(**column));
	if (!*column)
		return DT_ALLOC_ERROR;

	(*column)->value = calloc(capacity, get_type_size(type));
	if (!(*column)->value)
	{
		free(*column);
		*column = NULL;
		return DT_ALLOC_ERROR;
	}
	(*column)->type = type;
	(*column)->n_values = 0;
	(*column)->value_capacity = capacity;

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
