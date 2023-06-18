#include "DataTable.h"
#include "DataColumn.h"
#include "HashTable.h"

// all internal functions
#include "DataTable_Internal.c"

struct DataTable*
dt_table_create(
	const size_t n_columns,
	const char (*column_names)[DT_MAX_COL_LEN],
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

		strncat(c.name, column_names[i], DT_MAX_COL_LEN);
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
	// no-op if NULL
	if (*table == NULL)
		return;

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

enum status_code_e
dt_table_insert_row_from_array(
	struct DataTable* const table,
	void* items)
{
	for (size_t i = 0; i < table->n_columns; ++i)
	{
		void* value = (char*)items + i*sizeof(void*);
		if (*(char**)value == NULL)
			value = NULL;

		enum status_code_e status = dt_column_append_value(table->columns[i].column, value);
		if (status != DT_SUCCESS)
			return status;
	}

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

const void*
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
	const char (*columns)[DT_MAX_COL_LEN])
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
	char(*col_names)[DT_MAX_COL_LEN] = calloc(table->n_columns, sizeof(*col_names));
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
	bool (*filter_callback)(void* item, void* user_data),
  void* user_data)
{
	bool is_error = false;
	size_t column_idx = __get_column_index(table, column, &is_error);
	if (is_error)
		return NULL;
	return dt_table_filter_by_index(table, column_idx, filter_callback, user_data);
}

struct DataTable*
dt_table_filter_by_index(
	const struct DataTable* const table,
	const size_t column_idx,
	bool (*filter_callback)(void* item, void* user_data),
  void* user_data)
{
	size_t* filtered_idx = dt_column_filter(
		table->columns[column_idx].column,
		filter_callback,
    user_data);

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
dt_table_filter_OR_by_index(
	const struct DataTable* const table,
	const size_t n_columns,
	const size_t* column_indices,
	bool (**filter_callback)(void* item, void* user_data),
  void* user_data)
{
	return __filter_multiple(
		table,
		n_columns,
		column_indices,
		filter_callback,
		&or_callback,
    user_data);
}

struct DataTable*
dt_table_filter_AND_by_index(
	const struct DataTable* const table,
	const size_t n_columns,
	const size_t* column_indices,
	bool (**filter_callback)(void* item, void* user_data),
  void* user_data)
{
	return __filter_multiple(
		table,
		n_columns,
		column_indices,
		filter_callback,
		&and_callback,
    user_data);
}

struct DataTable*
dt_table_filter_OR_by_name(
	const struct DataTable* const table,
	const size_t n_columns,
	const char(*column_names)[DT_MAX_COL_LEN],
	bool (**filter_callback)(void* item, void* user_data),
  void* user_data)
{
	size_t* column_indices = __get_multiple_column_indices(table, column_names, n_columns);
	if (!column_indices)
		return NULL;

	struct DataTable* filtered = dt_table_filter_OR_by_index(
		table,
		n_columns,
		column_indices,
		filter_callback,
    user_data);

	free(column_indices);
	return filtered;
}

struct DataTable*
dt_table_filter_AND_by_name(
	const struct DataTable* const table,
	const size_t n_columns,
	const char(*column_names)[DT_MAX_COL_LEN],
	bool (**filter_callback)(void* item, void* user_data),
  void* user_data)
{
	size_t* column_indices = __get_multiple_column_indices(table, column_names, n_columns);
	if (!column_indices)
		return NULL;

	struct DataTable* filtered = dt_table_filter_AND_by_index(
		table,
		n_columns,
		column_indices,
		filter_callback,
    user_data);

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
dt_table_append_by_row(
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
dt_table_append_multiple_by_row(
	struct DataTable* const dest,
	const size_t n_tables,
	...)
{
	va_list tables;
	va_start(tables, n_tables);

	for (size_t i = 0; i < n_tables; ++i)
	{
		enum status_code_e status_code;
		if ((status_code = dt_table_append_by_row(dest, va_arg(tables, const struct DataTable*))) != DT_SUCCESS)
			return status_code;
	}

	return DT_SUCCESS;
}

bool
dt_table_rows_equal(
	const struct DataTable* table1,
	const size_t row_idx_1,
  const size_t* const table1_column_indices,
	const struct DataTable* table2,
	const size_t row_idx_2,
  const size_t* const table2_column_indices,
  const size_t n_column_indices)
{
	for (size_t i = 0; i < n_column_indices; ++i)
	{
    // special case for NULL values
    if ((dt_table_check_isnull(table1, row_idx_1, i) && !dt_table_check_isnull(table2, row_idx_2, i))
        || (!dt_table_check_isnull(table1, row_idx_1, i) && dt_table_check_isnull(table2, row_idx_2, i)))
    {
      return false;
    }

    const size_t table1_check_idx = table1_column_indices[i];
    const size_t table2_check_idx = table2_column_indices[i];

		const void* value1 = dt_table_get_value(table1, row_idx_1, table1_check_idx);
		const void* value2 = dt_table_get_value(table2, row_idx_2, table2_check_idx);

		if (!__two_values_equal(
					value1, 
					table1->columns[table1_check_idx].column->type,
					value2,
					table2->columns[table2_check_idx].column->type))
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

	struct HashTable* htable = hash_create(table, false, NULL, 0);

  size_t* table_column_indices = __generate_range(table->n_columns);

	for (size_t i = 0; i < table->n_rows; ++i)
	{
		if (!hash_contains(htable, table, table_column_indices, NULL, i))
		{
			if (__transfer_row(distinct, table, i) != DT_SUCCESS)
			{
				hash_free(&htable);
				dt_table_free(&distinct);
        free(table_column_indices);
				return NULL;
			}
			hash_insert(htable, i);
		}
	}

  free(table_column_indices);
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

  bool is_success = false;
  __get_column_index(table, column_name, &is_success);
  if (!is_success)
    return DT_DUPLICATE;

	struct DataColumn* column_copy = dt_column_copy(column);
	if (!column_copy)
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
	memset(newpair.name, 0, DT_MAX_COL_LEN);
	strncat(newpair.name, column_name, strlen(column_name) + 1);
	newpair.column = column_copy;

	memcpy(&table->columns[table->n_columns++], &newpair, sizeof(struct ColumnPair));

	return DT_SUCCESS;
}

enum status_code_e
dt_table_drop_columns_by_name(
	struct DataTable* table,
	const size_t n_columns,
	const char (*column_names)[DT_MAX_COL_LEN])
{
	size_t* column_indices = __get_multiple_column_indices(table, column_names, n_columns);

	if (!column_indices)
		return DT_FAILURE;

	dt_table_drop_columns_by_index(table, n_columns, column_indices);

	free(column_indices);
	return DT_SUCCESS;
	
}

static int
size_t_compare(
  const void* a,
  const void* b)
{
  const size_t* _a = a;
  const size_t* _b = b;
  if (_a > _b)
    return -1;
  if (_a < _b)
    return 1;
  return 0;
}

enum status_code_e
dt_table_drop_columns_by_index(
	struct DataTable* table,
	const size_t n_columns,
	size_t* column_indices)
{
  // every time we drop a column, we shift the indices by one.
  // but this assumption only works if column_indices is sorted ascending
  size_t n_dropped_columns = 0;
  qsort(column_indices, n_columns, sizeof(size_t), &size_t_compare);
	for (size_t i = 0; i < n_columns; ++i)
  {
		__drop_column(table, column_indices[i] - n_dropped_columns);	
    n_dropped_columns++;
  }

	return DT_SUCCESS;
}

void
dt_table_drop_columns_with_null(
	struct DataTable* table)
{
	for (size_t i = 0; i < table->n_columns; ++i)
		if (table->columns[i].column->n_null_values > 0)
			__drop_column(table, i);
}

enum status_code_e
dt_table_drop_rows_with_null(
	struct DataTable* table)
{
	size_t n_null_columns = 0;
	size_t* null_column_indices = __get_null_column_indices(table, &n_null_columns);	
	if (!null_column_indices)
		return DT_FAILURE;

	size_t n_row_indices = 0;
	size_t* null_row_indices = __get_null_row_indices(table, null_column_indices, n_null_columns, &n_row_indices);

	for (size_t i = 0; i < table->n_columns; ++i)
	{
		struct DataColumn* subset = dt_column_drop_by_index(
				table->columns[i].column,
				null_row_indices,
				n_row_indices);
		dt_column_free(&table->columns[i].column);
		table->columns[i].column = subset;
	}

	free(null_row_indices);
	free(null_column_indices);

	table->n_rows = table->columns[0].column->n_values;

	return DT_SUCCESS;
}

enum status_code_e
dt_table_apply_column(
	struct DataTable* const table,
	const char* const column_name,
	void (*callback)(void* current_row_value, void* user_data, const void** const column_values),
	void* user_data,
	const size_t n_column_values,
	const char (*column_value_names)[DT_MAX_COL_LEN])
{
	
	bool is_error = false;
	size_t apply_column_index = __get_column_index(table, column_name, &is_error);

	// do nothing if column is not found
	if (is_error)
		return DT_FAILURE;

	// create array of pointers to each column
	void** column_values = NULL;
	if (n_column_values > 0)
		column_values = malloc(sizeof(void*) * n_column_values);

	if (!column_values)
		return DT_ALLOC_ERROR;

	size_t* column_value_indices = __get_multiple_column_indices(table, column_value_names, n_column_values);

	if (!column_value_indices)
	{
		free(column_values);
		return DT_FAILURE;
	}

	// iterate each row and call user-defined callback
	for (size_t i = 0; i < table->n_rows; ++i)
	{
		// assign pointers to column values on current row (if specified)
		if (column_value_names)
		{
			for (size_t k = 0; k < n_column_values; ++k)
			{
				size_t column_index = column_value_indices[k];
				const void* value = dt_table_get_value(table, i, column_index);
				// the cast is to disable const warning; the value won't actually change
				column_values[k] = (void*)value; 
			}
		}

		// cast to remove const-ness (this value WILL change in the callback)
		// NOTE: it's NOT advised to do this in general since changing the
		// pointer directly WILL NOT update NULL value counts. Here we
		// are doing it because we clear all NULL values at the end of
		// the function.
		void* current_row_value = (void*)dt_table_get_value(table, i, apply_column_index);
		callback(current_row_value, user_data, (const void** const)column_values);
	}

	free(column_value_indices);
	free(column_values);

	// clear any null values after applying function
	struct DataColumn* column = dt_table_get_column_ptr_by_index(table, apply_column_index);
	memset(column->null_value_indices, 0, column->n_null_values * sizeof(size_t));
	column->n_null_values = 0;

	return DT_SUCCESS;
}

void
dt_table_apply_all(
	struct DataTable* const table,
	void (*callback)(void* current_cell_value, void* user_data),
	void* user_data)
{
	for (size_t c = 0; c < table->n_columns; ++c)
	{
		for (size_t r = 0; r < table->n_rows; ++r)
		{
			void* value = (void*)dt_table_get_value(table, r, c);
			callback(value, user_data);
		}
	}

	// reset all NULL value counts in each column
	for (size_t i = 0; i < table->n_columns; ++i)
	{
		struct DataColumn* column = dt_table_get_column_ptr_by_index(table, i);
		memset(column->null_value_indices, 0, column->n_null_values * sizeof(size_t));
		column->n_null_values = 0;
	}
	

}

enum status_code_e
dt_table_fill_column_values_by_index(
	struct DataTable* table,
	const size_t column_index,
	const void* const value)
{
	if (column_index >= table->n_columns)
		return DT_INDEX_ERROR;

	struct DataColumn* column = dt_table_get_column_ptr_by_index(table, column_index);
	dt_column_fill_values(column, value);

	return DT_SUCCESS;
}

enum status_code_e
dt_table_fill_column_values_by_name(
	struct DataTable* table,
	const char* const column_name,
	const void* const value)
{
	bool is_error = false;
	size_t column_index = __get_column_index(table, column_name, &is_error);

	if (is_error)
		return DT_COLUMN_NOT_FOUND;

	return dt_table_fill_column_values_by_index(table, column_index, value);
}

void
dt_table_fill_all_values(
	struct DataTable* table,
	const void* const value)
{
	for (size_t i = 0; i < table->n_columns; ++i)
		dt_table_fill_column_values_by_index(table, i, value);
}

enum status_code_e
dt_table_replace_column_null_values_by_index(
	struct DataTable* const table,
	const size_t column_index,
	const void* const value)
{
	if (column_index >= table->n_columns)
		return DT_INDEX_ERROR;

	struct DataColumn* column = dt_table_get_column_ptr_by_index(table, column_index);

	for (size_t null_idx = 0; null_idx < column->n_null_values; ++null_idx)
		dt_column_set_value(column, column->null_value_indices[null_idx], value);

	// clear all null values
	memset(column->null_value_indices, 0, column->n_null_values * sizeof(size_t));
	column->n_null_values = 0;

	return DT_SUCCESS;
}

enum status_code_e
dt_table_replace_column_null_values_by_name(
	struct DataTable* const table,
	const char* const column_name,
	const void* const value)
{
	bool is_error = false;
	size_t column_index = __get_column_index(table, column_name, &is_error);

	if (is_error)
		return DT_COLUMN_NOT_FOUND;

	return dt_table_replace_column_null_values_by_index(table, column_index, value);
}

void
dt_table_replace_all_null_values(
	struct DataTable* const table,
	const void* const value)
{
	for (size_t i = 0; i < table->n_columns; ++i)
		dt_table_replace_column_null_values_by_index(table, i, value);
}

struct DataTable*
dt_table_sample_rows(
	const struct DataTable* const table,
	const size_t n_samples,
	const bool with_replacement)
{
	if (with_replacement)
		return __sample_with_replacement(table, n_samples);

	// can't draw samples larger than the table without replacement
	if (!with_replacement && n_samples > table->n_rows)
		return NULL;

	return __sample_without_replacement(table, n_samples);
}

enum status_code_e
dt_table_split(
	const struct DataTable* const table,
	const float proportion,
	struct DataTable** split1,
	struct DataTable** split2)
{
	if (!(proportion > 0.00f && proportion < 1.00f))
		return DT_BAD_ARG;

	if (*split1 != NULL || *split2 != NULL)
		return DT_BAD_ARG;

	size_t n_samples = (size_t)(proportion * table->n_rows);
	
	
	// create empty hashtable with current table as a reference
	struct HashTable* htable = hash_create(table, false, NULL, 0);
	if (!htable)
		return DT_ALLOC_ERROR;

	*split1 = dt_table_copy_skeleton(table);
	if (!*split1)
	{
		hash_free(&htable);
		return DT_ALLOC_ERROR;
	}

  size_t* table_column_indices = __generate_range(table->n_columns);

	// sample split1 (creating hash table to record which records were used)
	size_t sampled_rows = 0;
	while (sampled_rows < n_samples)
	{
		size_t random_idx = (size_t)__get_random_index(0, table->n_rows - 1);
		if (!hash_contains(htable, table, table_column_indices, NULL, random_idx))
		{
			__transfer_row(*split1, table, random_idx);
			hash_insert(htable, random_idx);
			sampled_rows++;
		}
	}

	// iterate over table indices and insert the ones NOT in hash map to
	// populate split2
	*split2 = dt_table_copy_skeleton(table);
	if (!*split2)
	{
		dt_table_free(split1);
		hash_free(&htable);
		return DT_ALLOC_ERROR;
	}

	for (size_t i = 0; i < table->n_rows; ++i)
	{
		if (!hash_contains(htable, table, table_column_indices, NULL, i))
		{
			__transfer_row(*split2, table, i);
			hash_insert(htable, i);
		}
	}

  free(table_column_indices);
	hash_free(&htable);

	return DT_SUCCESS;
}

struct DataTable*
dt_table_read_csv(
	const char* const filepath,
	const char delim,
	const enum data_type_e* const column_types)
{
	FILE* csv_file = fopen(filepath, "r");
	if (!csv_file)
		return NULL;

	// create table skeleton after reading header line
	struct DataTable* table = __parse_header_from_csv(csv_file, delim);
	if (!table)
	{
		fclose(csv_file);
		return NULL;
	}


	__parse_body_from_csv(csv_file, delim, table);

	// if passing custom column types, can change them from the default string
	// type and parse body with type conversion
	if (column_types)
	{
		for (size_t i = 0; i < table->n_columns; ++i)
		{
			table->columns[i].column->type = column_types[i];
			table->columns[i].column->type_size = dt_type_to_size(column_types[i]);
		}
		__convert_csv_column_types_from_string(table);
	}
	// infer column types if they're not provided by the user
	else
		__infer_csv_types(table);
	
	return table;
}

enum status_code_e
dt_table_append_by_column(
	struct DataTable* const dest,
	const struct DataTable* const src)
{
	for (size_t i = 0; i < src->n_columns; ++i)
	{
		enum status_code_e status = dt_table_insert_column(
				dest, 
				src->columns[i].column,
				src->columns[i].name);

		if (status != DT_SUCCESS)
			return status;
	}

	return DT_SUCCESS;
}

enum status_code_e
dt_table_append_multiple_by_column(
	struct DataTable* const dest,
	const size_t n_tables,
	...)
{
	va_list tables;
	va_start(tables, n_tables);

	for (size_t i = 0; i < n_tables; ++i)
	{
		enum status_code_e status_code;
		if ((status_code = dt_table_append_by_column(dest, va_arg(tables, const struct DataTable*))) != DT_SUCCESS)
			return status_code;
	}

	return DT_SUCCESS;
}

enum status_code_e
dt_table_cast_columns(
	struct DataTable* const table,
	const size_t n_columns,
	const char (*column_names)[DT_MAX_COL_LEN],
	const enum data_type_e* new_column_types)
{
	for (size_t i = 0; i < n_columns; ++i)
	{
		bool is_error = false;
		size_t column_idx = __get_column_index(table, column_names[i], &is_error);
		if (is_error)
			return DT_COLUMN_NOT_FOUND;

		dt_column_cast(table->columns[column_idx].column, new_column_types[i]);
	}

	return DT_SUCCESS;
}

static struct DataTable*
__dt_setup_join(
  const struct DataTable* const left_table,
  const struct DataTable* const right_table,
  const size_t n_join_columns,
  const char(*join_columns)[DT_MAX_COL_LEN],
  size_t** left_table_indices,
  size_t** right_table_indices,
  struct HashTable** left_table_hash,
  struct HashTable** right_table_hash)
{
  struct DataTable* join_table = NULL;
  char (*combined_columns)[DT_MAX_COL_LEN] = NULL;
  enum data_type_e* combined_types = NULL;

  *left_table_indices = __get_multiple_column_indices(left_table, join_columns, n_join_columns);
  if (!*left_table_indices)
    goto cleanup;

  *right_table_indices = __get_multiple_column_indices(right_table, join_columns, n_join_columns);
  if (!*right_table_indices)
    goto cleanup;

  *left_table_hash = hash_create(left_table, true, *left_table_indices, n_join_columns);
  if (!*left_table_hash)
    goto cleanup;

  *right_table_hash = hash_create(right_table, true, *right_table_indices, n_join_columns);
  if (!*right_table_hash)
    goto cleanup;

  combined_columns = calloc(left_table->n_columns + right_table->n_columns, sizeof(*combined_columns));
  if (!combined_columns)
    goto cleanup;

  combined_types = calloc(left_table->n_columns + right_table->n_columns, sizeof(*combined_types));
  if (!combined_types)
    goto cleanup;

  for (size_t i = 0; i < left_table->n_columns; ++i)
  {
    combined_types[i] = left_table->columns[i].column->type;
    memcpy(combined_columns[i], left_table->columns[i].name, DT_MAX_COL_LEN);
  }

  for (size_t i = left_table->n_columns; i < left_table->n_columns + right_table->n_columns; ++i)
  {
    combined_types[i] = right_table->columns[i - left_table->n_columns].column->type;
    memcpy(combined_columns[i], right_table->columns[i - left_table->n_columns].name, DT_MAX_COL_LEN);
  }

  join_table = dt_table_create(left_table->n_columns + right_table->n_columns, combined_columns, combined_types);
  if (!join_table)
    goto cleanup;

  free(combined_columns);
  free(combined_types);

  goto success;

cleanup:
  dt_table_free(&join_table);
success:
  return join_table;
}

static void
__dt_insert_row_from_two_tables(
  const struct DataTable* table1,
  const size_t table1_row_idx,
  const size_t table1_columns,
  const struct DataTable* table2,
  const size_t table2_row_idx,
  const size_t table2_columns,
  struct DataTable* target_table,
  const size_t target_row_idx)
{
  dt_table_insert_empty_row(target_table);

  // copy contents from table1 & table2 in order
  for (size_t col = 0; col < table1_columns; ++col)
  {
    const void* value = NULL;
    if (table1)
      value = dt_table_get_value(table1, table1_row_idx, col);

    if (table1 && dt_table_check_isnull(table1, table1_row_idx, col))
      dt_table_set_value(target_table, target_row_idx, col, NULL);
    else
      dt_table_set_value(target_table, target_row_idx, col, value);
  }

  for (size_t col = table1_columns; col < table1_columns + table2_columns; ++col)
  {
    const void* value = NULL;
    if (table2)
      value = dt_table_get_value(table2, table2_row_idx, col - table1_columns);

    if (table2 && dt_table_check_isnull(table2, table2_row_idx, col - table1_columns))
      dt_table_set_value(target_table, target_row_idx, col, NULL);
    else
      dt_table_set_value(target_table, target_row_idx, col, value);
  }
}

struct DataTable*
dt_table_join_inner(
  const struct DataTable* const left_table,
  const struct DataTable* const right_table,
  const size_t n_join_columns,
  const char (*join_columns)[DT_MAX_COL_LEN])
{
  if (!left_table || !right_table)
    return NULL;

  size_t* left_table_indices = NULL;
  size_t* right_table_indices = NULL;
  struct HashTable* left_table_hash = NULL;
  struct HashTable* right_table_hash = NULL;

  struct DataTable* join_table = 
    __dt_setup_join(
        left_table,
        right_table,
        n_join_columns,
        join_columns,
        &right_table_indices,
        &left_table_indices,
        &left_table_hash,
        &right_table_hash);

  if (!join_table)
    return NULL;

  size_t current_insert_row = 0;
  size_t hash_table_row_idx = 0;
  for (size_t i = 0; i < right_table->n_rows; ++i)
  {
    if (hash_contains(left_table_hash, right_table, right_table_indices, &hash_table_row_idx, i))
    {
      __dt_insert_row_from_two_tables(
          left_table, 
          hash_table_row_idx, 
          left_table->n_columns,
          right_table, 
          i, 
          right_table->n_columns,
          join_table, 
          current_insert_row);

      current_insert_row++;
    }
  }

  hash_free(&left_table_hash);
  hash_free(&right_table_hash);

  return join_table;
}

struct DataTable*
dt_table_join_left(
  const struct DataTable* const left_table,
  const struct DataTable* const right_table,
  const size_t n_join_columns,
  const char (*join_columns)[DT_MAX_COL_LEN])
{
  if (!left_table || !right_table)
    return NULL;

  size_t* left_table_indices = NULL;
  size_t* right_table_indices = NULL;
  struct HashTable* left_table_hash = NULL;
  struct HashTable* right_table_hash = NULL;

  struct DataTable* join_table = 
    __dt_setup_join(
        left_table,
        right_table,
        n_join_columns,
        join_columns,
        &right_table_indices,
        &left_table_indices,
        &left_table_hash,
        &right_table_hash);

  if (!join_table)
    return NULL;

  size_t current_insert_row = 0;
  size_t hash_table_row_idx = 0;
  for (size_t i = 0; i < left_table->n_rows; ++i)
  {
    if (hash_contains(right_table_hash, left_table, left_table_indices, &hash_table_row_idx, i))
    {
      __dt_insert_row_from_two_tables(
          left_table, 
          i, 
          left_table->n_columns,
          right_table, 
          hash_table_row_idx, 
          right_table->n_columns,
          join_table, 
          current_insert_row);
    }
    else
    {
      __dt_insert_row_from_two_tables(
          left_table, 
          i, 
          left_table->n_columns,
          NULL, 
          hash_table_row_idx, 
          right_table->n_columns,
          join_table, 
          current_insert_row);
    }
    current_insert_row++;
  }

  hash_free(&left_table_hash);
  hash_free(&right_table_hash);

  return join_table;
}

struct DataTable*
dt_table_join_right(
  const struct DataTable* const left_table,
  const struct DataTable* const right_table,
  const size_t n_join_columns,
  const char (*join_columns)[DT_MAX_COL_LEN])
{
  if (!left_table || !right_table)
    return NULL;

  size_t* left_table_indices = NULL;
  size_t* right_table_indices = NULL;
  struct HashTable* left_table_hash = NULL;
  struct HashTable* right_table_hash = NULL;

  struct DataTable* join_table = 
    __dt_setup_join(
        left_table,
        right_table,
        n_join_columns,
        join_columns,
        &right_table_indices,
        &left_table_indices,
        &left_table_hash,
        &right_table_hash);

  if (!join_table)
    return NULL;

  size_t current_insert_row = 0;
  size_t hash_table_row_idx = 0;
  for (size_t i = 0; i < right_table->n_rows; ++i)
  {
    if (hash_contains(left_table_hash, right_table, right_table_indices, &hash_table_row_idx, i))
    {
      __dt_insert_row_from_two_tables(
          left_table, 
          hash_table_row_idx, 
          left_table->n_columns,
          right_table, 
          i, 
          right_table->n_columns,
          join_table, 
          current_insert_row);
    }
    else
    {
      __dt_insert_row_from_two_tables(
          NULL, 
          hash_table_row_idx, 
          left_table->n_columns,
          right_table, 
          i, 
          right_table->n_columns,
          join_table, 
          current_insert_row);
    }
    current_insert_row++;
  }

  hash_free(&left_table_hash);
  hash_free(&right_table_hash);

  return join_table;
}

struct DataTable*
dt_table_join_full(
  const struct DataTable* const left_table,
  const struct DataTable* const right_table,
  const size_t n_join_columns,
  const char (*join_columns)[DT_MAX_COL_LEN])
{
  if (!left_table || !right_table)
    return NULL;

  struct DataTable* left_join 
    = dt_table_join_left(left_table, right_table, n_join_columns, join_columns);

  if (!left_join)
    return NULL;

  struct DataTable* right_join
    = dt_table_join_right(left_table, right_table, n_join_columns, join_columns);

  if (!right_join)
  {
    dt_table_free(&left_join);
    return NULL;
  }

  if (dt_table_append_by_row(left_join, right_join) != DT_SUCCESS)
  {
    dt_table_free(&left_join);
    dt_table_free(&right_join);
    return NULL;
  }

  dt_table_free(&right_join);

  return left_join;
}

bool
dt_table_check_isnull(
  const struct DataTable* const table,
  const size_t row_idx,
  const size_t col_idx)
{
  struct DataColumn* column = table->columns[col_idx].column;
  for (size_t i = 0; i < column->n_null_values; ++i)
    if (column->null_value_indices[i] == row_idx)
      return true;

  return false;
}

bool
dt_table_row_contains_null(
  const struct DataTable* const table,
  const size_t row_idx)
{
  for (size_t i = 0; i < table->n_columns; ++i)
    if (dt_table_check_isnull(table, row_idx, i))
      return true;

  return false;
}

bool
dt_table_column_contains_null(
  const struct DataTable* const table,
  const size_t col_idx)
{
  struct DataColumn* column = table->columns[col_idx].column;
  return column->n_null_values > 0;
}

bool
dt_table_to_csv(
  const struct DataTable* const table,
  const char* const filepath,
  const char delim)
{
  FILE* csv_file = fopen(filepath, "w+");
  char* err_msg = "Warning: couldn't write table to file: %s\n";

  if (!csv_file)
  {
    printf(err_msg, filepath);
    return false;
  }

  char write_buffer[4096] = {0};

  // buffer size is one length less to append a \n character at the end of the buffer
  __table_headers_to_string(table, write_buffer, 4095, delim);
  strncat(write_buffer, "\n", 1);
  size_t write_len = strlen(write_buffer);
  if (fwrite(write_buffer, sizeof(char), write_len, csv_file) < write_len)
  {
    fclose(csv_file);
    return false;
  }

  // write body
  for (size_t i = 0; i < table->n_rows; ++i)
  {
    __table_row_to_string(table, i, write_buffer, 4095, delim);
    strncat(write_buffer, "\n", 1);
    write_len = strlen(write_buffer);
    if (fwrite(write_buffer, sizeof(char), write_len, csv_file) < write_len)
    {
      fclose(csv_file);
      return false;
    }
  }

  fclose(csv_file);
  return true;
}
