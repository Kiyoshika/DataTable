#include "DataTable.h"
#include "DataColumn.h"
#include "HashTable.h"

// all internal functions
#include "DataTable_Internal.c"

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

struct DataTable*
dt_table_copy_skeleton(
	const struct DataTable* const table)
{
	char(*col_names)[MAX_COL_LEN] = calloc(table->n_columns, sizeof(*col_names));
	if (!col_names)
		return NULL;

	enum data_type_e* data_types = calloc(table->n_columns, sizeof(*data_types));
	if (!data_types)
	{
		free(col_names);
		return NULL;
	}

	// fetch all the column names and column types to create the prototype
	// of new table before copying over the filtered data
	for (size_t i = 0; i < table->n_columns; ++i)
	{
		size_t len = strlen(table->columns[i].name);
		strncpy(col_names[i], table->columns[i].name, len);
		data_types[i] = table->columns[i].column->type;
	}

	struct DataTable* skeleton = dt_table_create(
		table->n_columns,
		col_names,
		data_types);

	free(col_names);
	free(data_types);

	return skeleton;
}

struct DataTable*
dt_table_filter_by_name(
	const struct DataTable* const table,
	const char* const column,
	void* user_data,
	bool (*filter_callback)(void* item, void* user_data))
{
	bool is_error = false;
	size_t column_idx = __get_column_index(table, column, &is_error);
	if (is_error)
		return NULL;
	return dt_table_filter_by_idx(table, column_idx, user_data, filter_callback);
}

struct DataTable*
dt_table_filter_by_idx(
	const struct DataTable* const table,
	const size_t column_idx,
	void* user_data,
	bool (*filter_callback)(void* item, void* user_data))
{
	size_t* filtered_idx = dt_column_filter(
		table->columns[column_idx].column,
		user_data,
		filter_callback);

	if (!filtered_idx)
		return NULL;
	
	struct DataTable* filtered_table = dt_table_copy_skeleton(table);
	
	// iterate over each column and subset
	for (size_t i = 0; i < table->n_columns; ++i)
	{
		struct DataColumn* filtered_column = dt_column_subset_by_boolean(
			table->columns[i].column,
			filtered_idx);

		dt_column_free(&filtered_table->columns[i].column);
		filtered_table->columns[i].column = filtered_column;
	}

	filtered_table->n_rows = filtered_table->columns[0].column->n_values;

	free(filtered_idx);
	return filtered_table;
}

struct DataTable*
dt_table_filter_OR_by_idx(
	const struct DataTable* const table,
	const size_t* column_indices,
	const size_t n_columns,
	void* user_data,
	bool (**filter_callback)(void* item, void* user_data))
{
	return __filter_multiple(
		table,
		column_indices,
		n_columns,
		user_data,
		filter_callback,
		&or_callback);
}

struct DataTable*
dt_table_filter_AND_by_idx(
	const struct DataTable* const table,
	const size_t* column_indices,
	const size_t n_columns,
	void* user_data,
	bool (**filter_callback)(void* item, void* user_data))
{
	return __filter_multiple(
		table,
		column_indices,
		n_columns,
		user_data,
		filter_callback,
		&and_callback);
}

struct DataTable*
dt_table_filter_OR_by_name(
	const struct DataTable* const table,
	const char(*column_names)[MAX_COL_LEN],
	const size_t n_columns,
	void* user_data,
	bool (**filter_callback)(void* item, void* user_data))
{
	size_t* column_indices = __get_multiple_column_indices(table, column_names, n_columns);
	if (!column_indices)
		return NULL;

	struct DataTable* filtered = dt_table_filter_OR_by_idx(
		table,
		column_indices,
		n_columns,
		user_data,
		filter_callback);

	free(column_indices);
	return filtered;
}

struct DataTable*
dt_table_filter_AND_by_name(
	const struct DataTable* const table,
	const char(*column_names)[MAX_COL_LEN],
	const size_t n_columns,
	void* user_data,
	bool (**filter_callback)(void* item, void* user_data))
{
	size_t* column_indices = __get_multiple_column_indices(table, column_names, n_columns);
	if (!column_indices)
		return NULL;

	struct DataTable* filtered = dt_table_filter_AND_by_idx(
		table,
		column_indices,
		n_columns,
		user_data,
		filter_callback);

	free(column_indices);
	return filtered;
}

struct DataColumn*
dt_table_get_column_ptr_by_name(
	const struct DataTable* const table,
	const char* const column_name)
{
	bool is_error = false;
	const size_t column_idx = __get_column_index(table, column_name, &is_error);

	if (is_error)
		return NULL;

	return dt_table_get_column_ptr_by_index(table, column_idx);
}

struct DataColumn*
dt_table_get_column_ptr_by_index(
	const struct DataTable* const table,
	const size_t column_idx)
{
	if (column_idx >= table->n_columns)
		return NULL;

	return table->columns[column_idx].column;
}

struct DataTable*
dt_table_copy(
	const struct DataTable* const table)
{
	struct DataTable* copy = dt_table_copy_skeleton(table);
	if (!copy)
		return NULL;

	for (size_t i = 0; i < table->n_columns; ++i)
	{
		dt_column_free(&copy->columns[i].column);
		copy->columns[i].column = dt_column_copy(table->columns[i].column);

		// cleanup on failure
		if (!copy->columns[i].column)
		{
			for (size_t k = 0; k < i; ++k)
				dt_column_free(&copy->columns[i].column);
			dt_table_free(&copy);
			return NULL;
		}
	}

	copy->n_rows = copy->columns[0].column->n_values;

	return copy;
}

enum status_code_e
dt_table_append_single(
	struct DataTable* const dest,
	const struct DataTable* const src)
{
	if (dest->n_columns != src->n_columns)
		return DT_SIZE_MISMATCH;

	for (size_t i = 0; i < dest->n_columns; ++i)
	{
		enum status_code_e status = dt_column_append(
				dest->columns[i].column,
				src->columns[i].column);

		if (status != DT_SUCCESS)
			return status;
	}

	dest->n_rows += src->n_rows;

	return DT_SUCCESS;
}

enum status_code_e
dt_table_append(
	struct DataTable* const dest,
	const size_t n_tables,
	...)
{
	va_list tables;
	va_start(tables, n_tables);

	for (size_t i = 0; i < n_tables; ++i)
	{
		enum status_code_e status_code;
		if ((status_code = dt_table_append_single(dest, va_arg(tables, const struct DataTable*))) != DT_SUCCESS)
			return status_code;
	}

	return DT_SUCCESS;
}

bool
dt_table_rows_equal(
	const struct DataTable* table1,
	const size_t row_idx_1,
	const struct DataTable* table2,
	const size_t row_idx_2)
{
	// can't be equal if column sizes are different
	if (table1->n_columns != table2->n_columns)
		return false;

	for (size_t i = 0; i < table1->n_columns; ++i)
	{
		void* value1 = dt_table_get_value(table1, row_idx_1, i);
		void* value2 = dt_table_get_value(table2, row_idx_2, i);

		if (!__two_values_equal(
					value1, 
					table1->columns[i].column->type,
					value2,
					table2->columns[i].column->type))
			return false;
	}

	return true;
}	

enum status_code_e
dt_table_insert_empty_row(
	struct DataTable* const table)
{
	for (size_t i = 0; i < table->n_columns; ++i)
	{
		enum status_code_e status = dt_column_append_value(table->columns[i].column, NULL);
		if (status != DT_SUCCESS)
			return status;
	}

	table->n_rows++;
	return DT_SUCCESS;
}

struct DataTable*
dt_table_distinct(
	const struct DataTable* table)
{
	struct DataTable* distinct = dt_table_copy_skeleton(table);

	if (!distinct)
		return NULL;

	struct HashTable* htable = hash_create(table, false);

	for (size_t i = 0; i < table->n_rows; ++i)
	{
		if (!hash_contains(htable, table, i))
		{
			if (__transfer_row(distinct, table, i) != DT_SUCCESS)
			{
				hash_free(&htable);
				dt_table_free(&distinct);
				return NULL;
			}
			hash_insert(htable, i);
		}
	}

	hash_free(&htable);
	return distinct;
}

enum status_code_e
dt_table_insert_column(
	struct DataTable* table,
	const struct DataColumn* const column,
	const char* const column_name)
{
	if (table->n_rows != column->n_values)
		return DT_SIZE_MISMATCH;

	struct DataColumn* column_copy = dt_column_copy(column);
	if (!column_copy)
		return DT_ALLOC_ERROR;

	void* alloc = realloc(table->columns, sizeof(*table->columns) * table->column_capacity + 1);
	if (!alloc)
		return DT_ALLOC_ERROR;

	if (table->n_columns == table->column_capacity)
	{
		void* alloc = realloc(table->columns, (table->column_capacity + 1) * sizeof(*table->columns));
		if (!alloc)
			return DT_ALLOC_ERROR;
		table->columns = alloc;
		table->column_capacity++;
	}

	struct ColumnPair newpair;
	memset(newpair.name, 0, MAX_COL_LEN);
	strncat(newpair.name, column_name, strlen(column_name));
	newpair.column = column_copy;

	memcpy(&table->columns[table->n_columns++], &newpair, sizeof(struct ColumnPair));

	

	return DT_SUCCESS;
}

enum status_code_e
dt_table_drop_columns(
	struct DataTable* table,
	const size_t n_columns,
	const char (*column_names)[MAX_COL_LEN])
{
	size_t* column_indices = __get_multiple_column_indices(table, column_names, n_columns);

	if (!column_indices)
		return DT_FAILURE;

	for (size_t i = 0; i < n_columns; ++i)
		__drop_column(table, i);	

	return DT_SUCCESS;
}
