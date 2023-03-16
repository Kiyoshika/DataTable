#include "DataTable.h"

struct DataTable*
dt_table_create(
	const size_t n_columns,
	const char (*column_names)[MAX_COL_LEN],
	const enum data_type_e* data_types)
{
	struct DataTable* table = malloc(sizeof(*table));
	if (!table)
		return NULL;

	table->columns = malloc(n_columns * sizeof(struct ColumnPair));
	if (!table->columns)
	{
		free(table);
		return NULL;
	}

	table->n_columns = n_columns;
	table->column_capacity = n_columns;

	for (size_t i = 0; i < n_columns; ++i)
	{
		struct ColumnPair c = {
			.name = {0},
			.column = NULL
		};

		strncat(c.name, column_names[i], MAX_COL_LEN - 1);
		dt_column_create(&c.column, 0, data_types[i]);
		if (!c.column)
		{
			for (size_t k = 0; k < i; ++k)
				dt_column_free(&table->columns[i].column);
			free(table->columns);
			free(table);
			return NULL;
		}

		memcpy(&table->columns[i], &c, sizeof(struct ColumnPair));
	}

	table->n_rows = 0;

	return table;
}

void
dt_table_free(
	struct DataTable** const table)
{
	for (size_t i = 0; i < (*table)->n_columns; ++i)
		dt_column_free(&(*table)->columns[i].column);

	free((*table)->columns);
	(*table)->columns = NULL;

	free(*table);
	*table = NULL;
}

enum status_code_e
dt_table_insert_row(
	struct DataTable* const table,
	size_t n_columns,
	...)
{
	va_list items;
	va_start(items, n_columns);
	for (size_t i = 0; i < table->n_columns; ++i)
	{
		void* value = va_arg(items, void*);
		enum status_code_e status = dt_column_append_value(table->columns[i].column, value);
		if (status != DT_SUCCESS)
			return status;
	}
	va_end(items);
	table->n_rows++;
	return DT_SUCCESS;
}

void
dt_table_set_value(
	struct DataTable* const table,
	const size_t row,
	const size_t column,
	const void* const value)
{
	dt_column_set_value(table->columns[column].column, row, value);
}

void*
dt_table_get_value(
	const struct DataTable* const table,
	const size_t row,
	const size_t column)
{
	return dt_column_get_value_ptr(table->columns[column].column, row);
}
