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

struct DataTable*
dt_table_select(
	const struct DataTable* const table,
	const size_t n_columns,
	const char (*columns)[MAX_COL_LEN])
{
	enum data_type_e* types = malloc(n_columns * sizeof(enum data_type_e));
	if (!types)
		return NULL;

	size_t* column_indices = calloc(n_columns, sizeof(size_t));
	if (!column_indices)
	{
		free(types);
		return NULL;
	}

	struct DataTable* subset = NULL;

	// fetch data types and indices from original data table.
	// this is not "optimized" but data sets will probably be small enough
	// to where this search method isn't terribly slow (unless you have
	// tens of thousands of columns which probably isn't likely)
	size_t current_idx = 0;
	for (size_t i = 0; i < n_columns; ++i)
	{
		bool found_column = false;
		// for every provided column, search the original column list
		// to find the position and type
		for (size_t k = 0; k < table->n_columns; ++k)
		{
			if (strcmp(columns[i], table->columns[k].name) == 0)
			{
				types[current_idx] = table->columns[k].column->type;
				column_indices[current_idx++] = k;
				found_column = true;
				break;
			}
		}

		// error if column is not found
		if (!found_column)
			goto err;
	}

	subset = dt_table_create(n_columns, columns, types);
	if (!subset)
		goto err;
	// create copies of each selected column
	for (size_t i = 0; i < subset->n_columns; ++i)
	{
		dt_column_free(&subset->columns[i].column);
		subset->columns[i].column = dt_column_copy(table->columns[column_indices[i]].column);
		if (!subset->columns[i].column)
			goto err;
	}

	// DONT'T free the newly-created table; skip over "err" label
	goto cleanup;


err:
	if (subset)
	{
		dt_table_free(&subset);
		subset = NULL;
	}
cleanup:
	free(types);
	free(column_indices);
	
	return subset;
}
