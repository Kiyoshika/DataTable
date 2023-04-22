/* 
 * all the internal functions used by DataTable.c included directly into source file
 *
 * this is mainly so it's more convenient to find internal functions instead of digging
 * through hundreds of lines of code
 */
#include <math.h>
#include <time.h>
#include <ctype.h>


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

#define dt_int_compare(value1, value1_type, value2, value2_type, result) \
	*result = *(value1_type*)value1 == *(value2_type*)value2;

#define dt_float_compare(value1, value2, result) \
	*result = !(fabsf(*(float*)value1 - *(float*)value2) > 0.00000001f);

#define dt_double_compare(value1, value2, result) \
	*result = !(fabs(*(double*)value1 - *(double*)value2) > 0.00000001);

#define dt_string_compare(value1, value2, result) \
	*result = strcmp(*(char**)value1, *(char**)value2) == 0;

// Internal function that checks if two values (fetched from a table)
// are equal.
//
// This is more tricky than it sounds since we could be comparing a
// uint8_t versus int64_t. Something like memcmp doesn't work for
// different-sized types.
//
// First we make some general type checks about special cases (strings
// and floats) then proceed to make integer-based comparisons (which are
// the easiest types to compare).
//
// To handle the different types, we use a couple macros defined above.
static bool
__two_values_equal(
	const void* value1,
	enum data_type_e value1_type,
	const void* value2,
	enum data_type_e value2_type)
{
	// if either is a string type, BOTH must be strings.
	if ((value1_type == STRING && value2_type != STRING)
		|| (value1_type != STRING && value2_type == STRING))
		return false;
	
	bool is_equal = false;

	// if both are a string, use the string comparison
	if (value1_type == STRING && value2_type == STRING)
	{
		dt_string_compare(value1, value2, &is_equal);
		return is_equal;
	}

	// if either is a float, use the float comparison
	if (value1_type == FLOAT || value2_type == FLOAT)
	{
		dt_float_compare(value1, value2, &is_equal);
		return is_equal;
	}
	
	// if either is a double, use the double comparison
	if (value1_type == DOUBLE || value2_type == DOUBLE)
	{
		dt_double_compare(value1, value2, &is_equal);
		return is_equal;
	}
	
	// otherwise, we can default to integer comparison
	// (although, have to do this DISGUSTING switch statement
	// to handle different combinations of int types)
	switch (value1_type)
	{
		case INT8:
			switch (value2_type)
			{
				case INT8:
					dt_int_compare(value1, int8_t, value2, int8_t, &is_equal);
					break;
				case INT16:
					dt_int_compare(value1, int8_t, value2, int16_t, &is_equal);
					break;
				case INT32:
					dt_int_compare(value1, int8_t, value2, int32_t, &is_equal);
					break;
				case INT64:
					dt_int_compare(value1, int8_t, value2, int64_t, &is_equal);
					break;
				case UINT8:
					dt_int_compare(value1, int8_t, value2, uint8_t, &is_equal);
					break;
				case UINT16:
					dt_int_compare(value1, int8_t, value2, uint16_t, &is_equal);
					break;
				case UINT32:
					dt_int_compare(value1, int8_t, value2, uint32_t, &is_equal);
					break;
				case UINT64:
					dt_int_compare(value1, int8_t, value2, uint64_t, &is_equal);
					break;
				// ignore types (DO NOT use default, it will prevent warnings if we add new types)
				case FLOAT:
				case DOUBLE:
				case STRING:
					break;
			}
			break;

		case INT16:
			switch (value2_type)
			{
				case INT8:
					dt_int_compare(value1, int16_t, value2, int8_t, &is_equal);
					break;
				case INT16:
					dt_int_compare(value1, int16_t, value2, int16_t, &is_equal);
					break;
				case INT32:
					dt_int_compare(value1, int16_t, value2, int32_t, &is_equal);
					break;
				case INT64:
					dt_int_compare(value1, int16_t, value2, int64_t, &is_equal);
					break;
				case UINT8:
					dt_int_compare(value1, int16_t, value2, uint8_t, &is_equal);
					break;
				case UINT16:
					dt_int_compare(value1, int16_t, value2, uint16_t, &is_equal);
					break;
				case UINT32:
					dt_int_compare(value1, int16_t, value2, uint32_t, &is_equal);
					break;
				case UINT64:
					dt_int_compare(value1, int16_t, value2, uint64_t, &is_equal);
					break;
				// ignore types (DO NOT use default, it will prevent warnings if we add new types)
				case FLOAT:
				case DOUBLE:
				case STRING:
					break;
			}
			break;

		case INT32:
			switch (value2_type)
			{
				case INT8:
					dt_int_compare(value1, int32_t, value2, int8_t, &is_equal);
					break;
				case INT16:
					dt_int_compare(value1, int32_t, value2, int16_t, &is_equal);
					break;
				case INT32:
					dt_int_compare(value1, int32_t, value2, int32_t, &is_equal);
					break;
				case INT64:
					dt_int_compare(value1, int32_t, value2, int64_t, &is_equal);
					break;
				case UINT8:
					dt_int_compare(value1, int32_t, value2, uint8_t, &is_equal);
					break;
				case UINT16:
					dt_int_compare(value1, int32_t, value2, uint16_t, &is_equal);
					break;
				case UINT32:
					dt_int_compare(value1, int32_t, value2, uint32_t, &is_equal);
					break;
				case UINT64:
					dt_int_compare(value1, int32_t, value2, uint64_t, &is_equal);
					break;
				// ignore types (DO NOT use default, it will prevent warnings if we add new types)
				case FLOAT:
				case DOUBLE:
				case STRING:
					break;
			}
			break;

		case INT64:
			switch (value2_type)
			{
				case INT8:
					dt_int_compare(value1, int64_t, value2, int8_t, &is_equal);
					break;
				case INT16:
					dt_int_compare(value1, int64_t, value2, int16_t, &is_equal);
					break;
				case INT32:
					dt_int_compare(value1, int64_t, value2, int32_t, &is_equal);
					break;
				case INT64:
					dt_int_compare(value1, int64_t, value2, int64_t, &is_equal);
					break;
				case UINT8:
					dt_int_compare(value1, int64_t, value2, uint8_t, &is_equal);
					break;
				case UINT16:
					dt_int_compare(value1, int64_t, value2, uint16_t, &is_equal);
					break;
				case UINT32:
					dt_int_compare(value1, int64_t, value2, uint32_t, &is_equal);
					break;
				case UINT64:
					dt_int_compare(value1, int64_t, value2, uint64_t, &is_equal);
					break;
				// ignore types (DO NOT use default, it will prevent warnings if we add new types)
				case FLOAT:
				case DOUBLE:
				case STRING:
					break;
			}
			break;

		case UINT8:
			switch (value2_type)
			{
				case INT8:
					dt_int_compare(value1, uint8_t, value2, int8_t, &is_equal);
					break;
				case INT16:
					dt_int_compare(value1, uint8_t, value2, int16_t, &is_equal);
					break;
				case INT32:
					dt_int_compare(value1, uint8_t, value2, int32_t, &is_equal);
					break;
				case INT64:
					dt_int_compare(value1, uint8_t, value2, int64_t, &is_equal);
					break;
				case UINT8:
					dt_int_compare(value1, uint8_t, value2, uint8_t, &is_equal);
					break;
				case UINT16:
					dt_int_compare(value1, uint8_t, value2, uint16_t, &is_equal);
					break;
				case UINT32:
					dt_int_compare(value1, uint8_t, value2, uint32_t, &is_equal);
					break;
				case UINT64:
					dt_int_compare(value1, uint8_t, value2, uint64_t, &is_equal);
					break;
				// ignore types (DO NOT use default, it will prevent warnings if we add new types)
				case FLOAT:
				case DOUBLE:
				case STRING:
					break;
			}
			break;

		case UINT16:
			switch (value2_type)
			{
				case INT8:
					dt_int_compare(value1, uint16_t, value2, int8_t, &is_equal);
					break;
				case INT16:
					dt_int_compare(value1, uint16_t, value2, int16_t, &is_equal);
					break;
				case INT32:
					dt_int_compare(value1, uint16_t, value2, int32_t, &is_equal);
					break;
				case INT64:
					dt_int_compare(value1, uint16_t, value2, int64_t, &is_equal);
					break;
				case UINT8:
					dt_int_compare(value1, uint16_t, value2, uint8_t, &is_equal);
					break;
				case UINT16:
					dt_int_compare(value1, uint16_t, value2, uint16_t, &is_equal);
					break;
				case UINT32:
					dt_int_compare(value1, uint16_t, value2, uint32_t, &is_equal);
					break;
				case UINT64:
					dt_int_compare(value1, uint16_t, value2, uint64_t, &is_equal);
					break;
				// ignore types (DO NOT use default, it will prevent warnings if we add new types)
				case FLOAT:
				case DOUBLE:
				case STRING:
					break;
			}
			break;

		case UINT32:
			switch (value2_type)
			{
				case INT8:
					dt_int_compare(value1, uint32_t, value2, int8_t, &is_equal);
					break;
				case INT16:
					dt_int_compare(value1, uint32_t, value2, int16_t, &is_equal);
					break;
				case INT32:
					dt_int_compare(value1, uint32_t, value2, int32_t, &is_equal);
					break;
				case INT64:
					dt_int_compare(value1, uint32_t, value2, int64_t, &is_equal);
					break;
				case UINT8:
					dt_int_compare(value1, uint32_t, value2, uint8_t, &is_equal);
					break;
				case UINT16:
					dt_int_compare(value1, uint32_t, value2, uint16_t, &is_equal);
					break;
				case UINT32:
					dt_int_compare(value1, uint32_t, value2, uint32_t, &is_equal);
					break;
				case UINT64:
					dt_int_compare(value1, uint32_t, value2, uint64_t, &is_equal);
					break;
				// ignore types (DO NOT use default, it will prevent warnings if we add new types)
				case FLOAT:
				case DOUBLE:
				case STRING:
					break;
			}
			break;

		case UINT64:
			switch (value2_type)
			{
				case INT8:
					dt_int_compare(value1, uint64_t, value2, int8_t, &is_equal);
					break;
				case INT16:
					dt_int_compare(value1, uint64_t, value2, int16_t, &is_equal);
					break;
				case INT32:
					dt_int_compare(value1, uint64_t, value2, int32_t, &is_equal);
					break;
				case INT64:
					dt_int_compare(value1, uint64_t, value2, int64_t, &is_equal);
					break;
				case UINT8:
					dt_int_compare(value1, uint64_t, value2, uint8_t, &is_equal);
					break;
				case UINT16:
					dt_int_compare(value1, uint64_t, value2, uint16_t, &is_equal);
					break;
				case UINT32:
					dt_int_compare(value1, uint64_t, value2, uint32_t, &is_equal);
					break;
				case UINT64:
					dt_int_compare(value1, uint64_t, value2, uint64_t, &is_equal);
					break;
				// ignore types (DO NOT use default, it will prevent warnings if we add new types)
				case FLOAT:
				case DOUBLE:
				case STRING:
					break;
			}
			break;
		// ignore types (DO NOT use default, it will prevent warnings if we add new types)
		case FLOAT:
		case DOUBLE:
		case STRING:
			break;
	}

	return is_equal;
	
	// NOTE: eventually we'd need to support custom comparison
	// when we build in user-defined type support in DataTables.
}

// take a row from src table (at src_row_idx) and insert it (append) into
// dest table.
// NOTE: this makes the assumption that number of columns AND column types
// are the same.
static enum status_code_e
__transfer_row(
	struct DataTable* const dest,
	const struct DataTable* const src,
	const size_t src_row_idx)
{
	enum status_code_e status = dt_table_insert_empty_row(dest);
	if (status != DT_SUCCESS)
		return status;

	for (size_t i = 0; i < dest->n_columns; ++i)
	{
		const void* value = dt_table_get_value(src, src_row_idx, i);
		// be careful with unsigned subtraction...
		size_t dest_row = dest->n_rows == 0 ? 0 : dest->n_rows - 1;
		dt_table_set_value(dest, dest_row, i, value);
	}

	return DT_SUCCESS;
}

static void
__drop_column(
	struct DataTable* table,
	const size_t column_index)
{
	if (table->n_columns == 0)
		return;

	struct DataColumn* column = dt_table_get_column_ptr_by_index(
			table, 
			column_index);

	dt_column_free(&column);
	for (size_t i = column_index; i < table->n_columns - 1; ++i)
		memcpy(&table->columns[i], &table->columns[i + 1], sizeof(*table->columns));
	memset(&table->columns[table->n_columns - 1], 0, sizeof(*table->columns));

	if (table->n_columns > 0)
		table->n_columns--;
}

static size_t*
__get_null_column_indices(
	const struct DataTable* const table,
	size_t* n_null_columns)
{
	bool contains_null = false;
	
	// could be wrapped in a struct but not bothering with that right now
	size_t* null_column_indices = calloc(1, sizeof(size_t));
	*n_null_columns = 0;
	size_t null_column_capacity = 1;

	if (!null_column_indices)
		return NULL;

	for (size_t i = 0; i < table->n_columns; ++i)
	{
		if (table->columns[i].column->n_null_values > 0)
		{
			contains_null = true;
			null_column_indices[(*n_null_columns)++] = i;
			if (*n_null_columns == null_column_capacity)
			{
				size_t new_capacity = null_column_capacity *= 2;
				void* alloc = realloc(null_column_indices, new_capacity * sizeof(size_t));
				if (!alloc)
				{
					free(null_column_indices);
					return NULL;
				}

				null_column_indices = alloc;
				null_column_capacity = new_capacity;
			}
		}
	}

	if (!contains_null)
	{
		free(null_column_indices);
		return NULL;
	}

	return null_column_indices;
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

static size_t*
__get_distinct_indices(
	size_t* indices,
	const size_t n_indices,
	size_t* n_distinct_indices)
{
	qsort(indices, n_indices, sizeof(size_t), &sizet_compare);

	// initially allocate to same size but reallocate to proper size afterwards
	size_t* distinct_indices = calloc(n_indices, sizeof(size_t));
	*n_distinct_indices = 0;

	distinct_indices[(*n_distinct_indices)++] = indices[0];
	for (size_t i = 1; i < n_indices; ++i)
		if (indices[i] != indices[i - 1])
			distinct_indices[(*n_distinct_indices)++] = indices[i];

	void* alloc = realloc(distinct_indices, *n_distinct_indices * sizeof(size_t));
	if (!alloc)
	{
		free(distinct_indices);
		return NULL;
	}
	distinct_indices = alloc;
	return distinct_indices;
}

static size_t*
__get_null_row_indices(
	const struct DataTable* const table,
	const size_t* const null_column_indices,
	const size_t n_null_columns,
	size_t* n_row_indices)
{
	// iterate over each null column indices
	// fetch the array of row indices of null values
	// merge them all together
	// sort the array
	// grab distinct value in new size_t array which is then returned
	
	size_t n_candidate_row_indices = 0;
	for (size_t i = 0; i < n_null_columns; ++i)
	{
		size_t column_index = null_column_indices[i];
		n_candidate_row_indices += table->columns[column_index].column->n_null_values;
	}

	size_t* candidate_row_indices = calloc(n_candidate_row_indices, sizeof(size_t));
	if (!candidate_row_indices)
		return NULL;

	// copy ALL the row indices from every column into one large array
	size_t current_idx = 0;
	for (size_t i = 0; i < n_null_columns; ++i)
	{
		size_t column_index = null_column_indices[i];
		struct DataColumn* column = dt_table_get_column_ptr_by_index(table, column_index);
		for (size_t k = 0; k < column->n_null_values; ++k)
			candidate_row_indices[current_idx++] = column->null_value_indices[k];
	}

	size_t* distinct_indices = __get_distinct_indices(candidate_row_indices, n_candidate_row_indices, n_row_indices);

	free(candidate_row_indices);
	return distinct_indices;
}

// utility function to get a random size_t in a range.
// this uses a "modified" version of RAND_MAX to support the upper bound
// of an unsigned 64-bit integer (2^64 - 1).
// this assumes srand(time(NULL)) has been called before generating these ints.
static uint64_t
__get_random_index(uint64_t lower_bound, uint64_t upper_bound)
{
	uint64_t new_randmax = ((uint64_t)rand() << (64 - 15)) | ((uint64_t)rand() << (64 - 30)) | ((uint64_t)rand() << (64 - 45)) | (uint64_t)rand();
	return (new_randmax % (upper_bound - lower_bound + 1)) + lower_bound;
}

static struct DataTable*
__sample_with_replacement(
	const struct DataTable* const table,
	const size_t n_samples)
{
	// nothing to sample
	if (table->n_rows == 0)
		return NULL;

	// create new data table with size of n_samples
	struct DataTable* samples = dt_table_copy_skeleton(table);
	if (!samples)
		return NULL;
	
	// generate n_samples of random indices from original table and 
	// transfer rows over
	srand(time(NULL)); // set seed
	for (size_t i = 0; i < n_samples; ++i)
	{
		size_t random_idx = (size_t)__get_random_index(0, table->n_rows - 1);
		__transfer_row(samples, table, random_idx);
	}

	return samples;
}

static struct DataTable*
__sample_without_replacement(
	const struct DataTable* const table,
	const size_t n_samples)
{
	// nothing to sample
	if (table->n_rows == 0)
		return NULL;

	// create empty hashtable with current table as a reference
	struct HashTable* htable = hash_create(table, false);
	if (!htable)
		return NULL;

	// create new data table with size of n_samples
	struct DataTable* samples = dt_table_copy_skeleton(table);
	if (!samples)
		return NULL;

	size_t sampled_rows = 0;
	while (sampled_rows < n_samples)
	{
		size_t random_idx = (size_t)__get_random_index(0, table->n_rows - 1);
		if (!hash_contains(htable, table, random_idx))
		{
			__transfer_row(samples, table, random_idx);
			hash_insert(htable, random_idx);
			sampled_rows++;
		}
	}

	hash_free(&htable);

	return samples;
}

static void
__tokenize_line(
	const char* const line,
	const char delim,
	char* tokens,
	size_t* n_tokens)
{
	*n_tokens = 0;
	memset(tokens, 0, 4096);

	const size_t len = strlen(line);

	bool inside_quotes = false;
	size_t tokens_idx = 0;
	char quote_start_char = 0;

	for (size_t i = 0; i < len; ++i)
	{
		// hitting delimiter or newline character
		if ((line[i] == delim && !inside_quotes) || line[i] == '\n')
		{
			tokens[tokens_idx++] = '\0';
			(*n_tokens)++;
			continue;
		}

		if (line[i] == '\'' || line[i] == '\"')
		{
			if (!inside_quotes)
			{
				quote_start_char = line[i];
				inside_quotes = true;
			}
			else if (inside_quotes && quote_start_char == line[i])
				inside_quotes = false;
		}

		tokens[tokens_idx++] = line[i];
	}
}

// this makes a huge assumption that the caller knows what they're doing.
//
// takes a character buffer with multiple NULL characters separating tokens.
// finds the n-th NULL terminating character to extract a pointer to that specific token
//
// EXAMPLE:
// tokens = "value1\0value2\0value3\0"
// if we search for the second token, we will find the first NULL
// character and return a pointer to the beginning of "value2"
static const char* const
__get_token(
	const char* const tokens,
	const size_t token_idx)
{
	// if getting first token, it's the beginning of the buffer
	if (token_idx == 0)
		return tokens;

	size_t idx = 0;
	size_t current_token_idx = 0;

	while (true)
	{

		if (current_token_idx == token_idx)
			return &tokens[idx];

		if (tokens[idx++] == '\0')
			current_token_idx++;
	}

	// unreachable
	return NULL;
}

static struct DataTable*
__parse_header_from_csv(
	FILE* csv_file,
	const char delim)
{
	char current_line[4096] = {0};
	
	char tokens[4096] = {0};
	size_t n_tokens = 0;

	// read header line
	fgets(current_line, 4096, csv_file);
	__tokenize_line(current_line, delim, tokens, &n_tokens);

	// create array of column names after tokenizing header
	char (*column_names)[MAX_COL_LEN] = calloc(n_tokens, sizeof(*column_names));
	if (!column_names)
		return NULL;
	for (size_t i = 0; i < n_tokens; ++i)
	{
		// ensure column name is NOT NULL
		const char* const token = __get_token(tokens, i);
		if (strlen(token) == 0)
			return NULL;

		strcpy(column_names[i], __get_token(tokens, i));
	}

	// create array of STRING types
	enum data_type_e* dtypes = calloc(n_tokens, sizeof(*dtypes));
	if (!dtypes)
	{
		free(column_names);
		return NULL;
	}
	for (size_t i = 0; i < n_tokens; ++i)
		dtypes[i] = STRING;

	struct DataTable* table = dt_table_create(n_tokens, column_names, dtypes);
	free(column_names);
	free(dtypes);

	return table;
}

static void
__parse_body_from_csv(
	FILE* csv_file,
	const char delim,
	struct DataTable* const table)
{
	char current_line[4096] = {0};
	
	char tokens[4096] = {0};
	size_t n_tokens = 0;

	void* items = NULL;

	// read next line in body
	while (fgets(current_line, 4096, csv_file))
	{
		__tokenize_line(current_line, delim, tokens, &n_tokens);
		if (!items)
			items = calloc(n_tokens, sizeof(void*));

		// create "shared" pointers to insert into table
		for (size_t i = 0; i < table->n_columns; ++i)
		{
			char* value = strdup(__get_token(tokens, i));
			// insert NULL value if string is empty
			if (strlen(value) == 0)
			{
				memset((char*)items + i*sizeof(void*), 0, sizeof(void*));
				free(value);
			}
			else
				memcpy((char*)items + i*sizeof(void*), &value, sizeof(void*));
		}

		// insert values into table
		dt_table_insert_row_from_array(table, items);

		// cleanup values after inserting (copies are made at insert time)
		for (size_t i = 0; i < table->n_columns; ++i)
		{
			char** value_addr = (char**)((char*)items + i*sizeof(void*));
			free(*value_addr);
			*value_addr = NULL;
		}
	}

	free(items);
}

static void
__convert_column_type_from_string(
	struct DataColumn* const column)
{
	// if the desired type is a string, no need for conversion
	if (column->type == STRING)
		return;

	// iterate over each item in column and convert from string to
	// appropriate type
	//
	// all of the current values in the column should be string addresses
	//
	// we have to do some voodoo magic to extract the string addresses
	// because at this point we have already changed the column types and
	// sizes. It will be a little gross
	for (size_t i = 0; i < column->n_values; ++i)
	{
		char** value_addr = (char**)((char*)column->value + i*sizeof(char**));
		char* value_str = *value_addr;
		char* endptr = NULL;

		switch (column->type)
		{
			case UINT8:
			{
				uint8_t value = (uint8_t)strtoul(value_str, &endptr, 10);
				dt_column_set_value(column, i, &value);
				break;
			}

			case UINT16:
			{
				uint16_t value = (uint16_t)strtoul(value_str, &endptr, 10);
				dt_column_set_value(column, i, &value);
				break;
			}

			case UINT32:
			{
				uint32_t value = (uint32_t)strtoul(value_str, &endptr, 10);
				dt_column_set_value(column, i, &value);
				break;
			}

			case UINT64:
			{
				uint64_t value = (uint64_t)strtoull(value_str, &endptr, 10);
				dt_column_set_value(column, i, &value);
				break;
			}

			case INT8:
			{
				int8_t value = (int8_t)strtol(value_str, &endptr, 10);
				dt_column_set_value(column, i, &value);
				break;
			}

			case INT16:
			{
				int16_t value = (int16_t)strtol(value_str, &endptr, 10);
				dt_column_set_value(column, i, &value);
				break;
			}

			case INT32:
			{
				int32_t value = (int32_t)strtol(value_str, &endptr, 10);
				dt_column_set_value(column, i, &value);
				break;
			}

			case INT64:
			{
				int64_t value = (int64_t)strtoll(value_str, &endptr, 10);
				dt_column_set_value(column, i, &value);
				break;
			}

			case FLOAT:
			{
				float value = strtof(value_str, &endptr);
				dt_column_set_value(column, i, &value);
				break;
			}

			case DOUBLE:
			{
				double value = strtod(value_str, &endptr);
				dt_column_set_value(column, i, &value);
				break;
			}

			// do nothing, just here to avoid compiler warning
			case STRING:
				break;
		}

		// strings are heap allocated so we free them after converting
		free(value_str);

		// disable deallocator since it's no longer heap allocated
		column->deallocator = NULL;
	}
}

static void
__convert_csv_column_types_from_string(
	struct DataTable* const table)
{
	for (size_t i = 0; i < table->n_columns; ++i)
		__convert_column_type_from_string(table->columns[i].column);
}

// perform naive type inference which only determines:
// STRING, DOUBLE, INT64 to UINT64
// if user wants smaller types, they will have to convert separately
static enum data_type_e
__infer_column_type(
	struct DataColumn* const column)
{
	// below are the rules (in order) to determine type
	
	// check for any alpha characters --> STRING
	//     can immediately terminate after we find a single alpha char
	// check if any contain '.' --> DOUBLE
	//     need to make sure NO alpha characters are present, so we must
	//     still iterate over entire column
	// check if first character is '-' to determine negativity
	//     if not STRING or DOUBLE after iterating entire column, we can
	//     choose either UINT64 or INT64 depending on presence of negation
	
	bool contains_decimal = false;
	bool contains_negative = false;

	for (size_t i = 0; i < column->n_values; ++i)
	{
		char** value_addr = (char**)((char*)column->value + i*sizeof(char**));
		size_t len = strlen(*value_addr);
		for (size_t k = 0; k < len; ++k)
		{
			char current_char = (*value_addr)[k];

			if (k == 0 && current_char == '-')
				contains_negative = true;

			// if contains a non-digit value (besides '.' and '-') it is a string
			if (!isdigit(current_char) && current_char != '.' && current_char != '-')
				goto is_string;

			if (current_char == '.')
				contains_decimal = true;
		}
	}

	// at this point, it's impossible to be a string, so numeric types are left

	if (contains_decimal)
		return DOUBLE;

	if (contains_negative)
		return INT64;

	return UINT64;

	// special case, this will only be reached from inside the loops
is_string:
	return STRING;
}

static void
__infer_csv_types(
	struct DataTable* const table)
{
	for (size_t i = 0; i < table->n_columns; ++i)
	{
		enum data_type_e inferred_dtype = __infer_column_type(table->columns[i].column);
		table->columns[i].column->type = inferred_dtype;
		table->columns[i].column->type_size = dt_type_to_size(inferred_dtype);
	}

	// take inferred types and cast entire table
	__convert_csv_column_types_from_string(table);
}
