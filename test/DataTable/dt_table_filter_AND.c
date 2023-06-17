#include "DataTable.h"
#include <math.h>
#include <stdio.h>

bool col1_filter(void* item, void* user_data)
{
	(void)user_data; // unused
	int32_t* _item = item;
	return *_item >= 10;
}

bool col2_filter(void* item, void* user_data)
{
	(void)user_data; // unused
	float* _item = item;
	return *_item > 5.0f;
}

int main()
{
	int status = -1;

	char colnames[2][MAX_COL_LEN] = { "col1", "col2" };
	enum data_type_e types[2] = { INT32, FLOAT };
	struct DataTable* table = dt_table_create(2, colnames, types);

	int32_t set1 = 10;
	float set2 = 5.5f;
	dt_table_insert_row(table, 2, &set1, &set2);

	set1 = 20;
	set2 = 12.52f;
	dt_table_insert_row(table, 2, &set1, &set2);

	set1 = 5;
	set2 = 21.21f;
	dt_table_insert_row(table, 2, &set1, &set2);

	size_t filter_idx[2] = { 0, 1 }; // both columns
	bool (*callbacks[2])(void*, void*) = { &col1_filter, &col2_filter };

	// filter for col1 >= 10 && col2 > 5.0f
	struct DataTable* filtered = dt_table_filter_AND_by_idx(
		table,
		2,
		filter_idx,
		callbacks,
    NULL);

	if (filtered->n_rows != 2)
	{
		fprintf(stderr, "Expected n_rows in filtered to be 2 but got %zu.\n", filtered->n_rows);
		goto cleanup;
	}

	const int32_t* get_int = dt_table_get_value(filtered, 0, 0);
	if (*get_int != 10)
	{
		fprintf(stderr, "Expected value at (0, 0) in filtered to be 10 but got %d.\n", *get_int);
		goto cleanup;
	}

	get_int = dt_table_get_value(filtered, 1, 0);
	if (*get_int != 20)
	{
		fprintf(stderr, "Expected value at (1, 0) in filtered to be 20 but got %d.\n", *get_int);
		goto cleanup;
	}

	const float* get_float = dt_table_get_value(filtered, 0, 1);
	if (fabsf(*get_float - 5.5f) > 0.0001f)
	{
		fprintf(stderr, "Expected value at (0, 1) in filtered to be 5.5 but got %f.\n", *get_float);
		goto cleanup;
	}

	get_float = dt_table_get_value(filtered, 1, 1);
	if (fabsf(*get_float - 12.52f) > 0.0001f)
	{
		fprintf(stderr, "Expected value at (1, 1) in filtered to be 12.52 but got %f.\n", *get_float);
		goto cleanup;
	}

	// exact same tests but passing column names instead of indices
	const char filter_names[2][MAX_COL_LEN] = { "col1", "col2" };

	// filter for col1 >= 20 && col2 > 15.0f
	struct DataTable* filtered2 = dt_table_filter_AND_by_name(
		table,
		2,
		filter_names,
		callbacks,
    NULL);

	if (filtered2->n_rows != 2)
	{
		fprintf(stderr, "Expected n_rows in filtered2 to be 2 but got %zu.\n", filtered2->n_rows);
		goto cleanup;
	}

	get_int = dt_table_get_value(filtered2, 0, 0);
	if (*get_int != 10)
	{
		fprintf(stderr, "Expected value at (0, 0) in filtered2 to be 10 but got %d.\n", *get_int);
		goto cleanup;
	}

	get_int = dt_table_get_value(filtered2, 1, 0);
	if (*get_int != 20)
	{
		fprintf(stderr, "Expected value at (1, 0) in filtered2 to be 20 but got %d.\n", *get_int);
		goto cleanup;
	}

	get_float = dt_table_get_value(filtered2, 0, 1);
	if (fabsf(*get_float - 5.5f) > 0.0001f)
	{
		fprintf(stderr, "Expected value at (0, 1) in filtered2 to be 5.5 but got %f.\n", *get_float);
		goto cleanup;
	}

	get_float = dt_table_get_value(filtered2, 1, 1);
	if (fabsf(*get_float - 12.52f) > 0.0001f)
	{
		fprintf(stderr, "Expected value at (1, 1) in filtered2 to be 12.52 but got %f.\n", *get_float);
		goto cleanup;
	}


	status = 0;
cleanup:
	dt_table_free(&table);
	dt_table_free(&filtered);
	dt_table_free(&filtered2);
	return status;
}
