/* 
 * all the internal functions used by DataTable.c included directly into source file
 *
 * this is mainly so it's more convenient to find internal functions instead of digging
 * through hundreds of lines of code
 */



// get the column index for a specific column name.
// if column name is not found, [is_error] is set to true.
static size_t
__get_column_index(
	const struct DataTable* const table,
	const char* const column_name,
	bool* is_error)
{
	*is_error = false;

	for (size_t i = 0; i < table->n_columns; ++i)
		if (strcmp(table->columns[i].name, column_name) == 0)
			return i;

	// column not found
	*is_error = true;
	return 0;
}

// a wrapper around __get_column_index that will return an array of
// column indices for all columns passed.
// returns NULL if a column name is not found.
static size_t* 
__get_multiple_column_indices(
	const struct DataTable* const table,
	const char (*column_names)[MAX_COL_LEN],
	const size_t n_columns)
{
	size_t* column_indices = calloc(n_columns, sizeof(size_t));
	if (!column_indices)
		return NULL;

	for (size_t i = 0; i < n_columns; ++i)
	{
		bool is_error = false;
		column_indices[i] = __get_column_index(table, column_names[i], &is_error);
		if (is_error)
		{
			free(column_indices);
			return NULL;
		}
	}

	return column_indices;
}

// take boolean arrays on each column and apply OR operator on all columns.
// returns back a single array after applying the operator on each column
// can used in __filter_multiple
static size_t* 
or_callback(
	size_t** boolean_arrays,
	const size_t n_rows,
	const size_t n_columns)
{
	size_t* filter_idx = calloc(n_rows, sizeof(size_t));
	if (!filter_idx)
		return NULL;

	for (size_t i = 0; i < n_rows; ++i)
	{
		for (size_t c = 0; c < n_columns; ++c)
		{
			if (boolean_arrays[c][i] == 1)
			{
				filter_idx[i] = 1;
				break; // continue to next row
			}
		}
	}

	return filter_idx;
}

// take boolean arrays on each column and apply AND operator on all columns.
// returns back a single array after applying the operator on each column
// can be used in __filter_multiple
static size_t*
and_callback(
	size_t** boolean_arrays, 
	const size_t n_rows,
	const size_t n_columns)
{
	size_t* filter_idx = calloc(n_rows, sizeof(size_t));
	if (!filter_idx)
		return NULL;

	for (size_t i = 0; i < n_rows; ++i)
	{
		size_t _sum = 0;
		for (size_t c = 0; c < n_columns; ++c)
			_sum += boolean_arrays[c][i];

		filter_idx[i] = _sum == n_columns ? 1 : 0;
	}

	return filter_idx;
}



// the base logic for filtering multiple columns.
// takes [or_and_callback] as a function that accepts the boolean arrays for
// each column and computes the or/and operator on all columns to return a single
// array that will be used to actually subset the original table.
static struct DataTable*
__filter_multiple(
	const struct DataTable* const table,
	const size_t* column_indices,
	const size_t n_columns,
	void* user_data,
	bool (**filter_callback)(void* item, void* user_data),
	size_t* (*or_and_callback)(size_t** boolean_arrays, size_t n_rows, size_t n_columns))
{
	// create pointer to pointer of size_t "arrays" to store the
	// boolean size_t arrays when calling column filter on each
	// provided index
	size_t** filter_idx_arrays = calloc(n_columns, sizeof(*filter_idx_arrays));
	if (!filter_idx_arrays)
		return NULL;

	// assign each index the filter for the specified column.
	// if I were a responsible human, I would check if any of these are NULL
	// and abort, but we're living on the edge here
	for (size_t i = 0; i < n_columns; ++i)
		filter_idx_arrays[i] = dt_column_filter(
			table->columns[column_indices[i]].column,
			user_data,
			filter_callback[i]);

	// create the final "boolean" size_t array which iterates all of
	// the others (row-by-row) and sets it to 1 if AT LEAST ONE of the
	// columns is 1 (OR LOGIC)
	size_t* filter_idx = or_and_callback(filter_idx_arrays, table->n_rows, n_columns);
	if (!filter_idx)
	{
		for (size_t i = 0; i < n_columns; ++i)
			free(filter_idx_arrays[i]);
		free(filter_idx_arrays);
		return NULL;
	}
	
	// create empty skeleton of original table
	struct DataTable* filtered_table = dt_table_copy_skeleton(table);

	// now finally, assign the columns according to "boolean" array
	for (size_t c = 0; c < filtered_table->n_columns; ++c)
	{
		dt_column_free(&filtered_table->columns[c].column);
		filtered_table->columns[c].column = dt_column_subset_by_boolean(
			table->columns[c].column,
			filter_idx);
	}

	filtered_table->n_rows = filtered_table->columns[0].column->n_values;

	// cleanup and return
	free(filter_idx);
	for (size_t i = 0; i < n_columns; ++i)
		free(filter_idx_arrays[i]);
	free(filter_idx_arrays);

	return filtered_table;
}

