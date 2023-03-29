#include "DataColumn.h"
#include <stdio.h>

int main()
{
	int status = -1;

	struct DataColumn* column = NULL;
	dt_column_create(&column, 5, INT32);

	int32_t set = 1;
	dt_column_set_value(column, 0, &set);

	set = 2;
	dt_column_set_value(column, 1, &set);

	set = 3;
	dt_column_set_value(column, 2, &set);

	set = 4;
	dt_column_set_value(column, 3, &set);

	set = 5;
	dt_column_set_value(column, 4, &set);

	size_t subset_idx[2] = { 2, 3 };

	struct DataColumn* subset = dt_column_subset_by_index(column, subset_idx, 2);
	
	if (subset->n_values != 2)
	{
		fprintf(stderr, "Expected subset to have 2 values but has %zu.\n", subset->n_values);
		goto cleanup;
	}

	int32_t get = 0;
	dt_column_get_value(subset, 0, &get);

	if (get != 3)
	{
		fprintf(stderr, "Expected first value in subset to be 3 but got %d.\n", get);
		goto cleanup;
	}

	dt_column_get_value(subset, 1, &get);

	if (get != 4)
	{
		fprintf(stderr, "Expected second value in subset to be 4 but got %d.\n", get);
		goto cleanup;
	}

	size_t subset_idx2[5] = { 0, 0, 1, 1, 0 };
	struct DataColumn* subset2 = dt_column_subset_by_boolean(column, subset_idx2);

	// apply same exact tests as above since they should be the same values
	if (subset2->n_values != 2)
	{
		fprintf(stderr, "Expected subset2 to have 2 values but has %zu.\n", subset2->n_values);
		goto cleanup;
	}

	get = 0;
	dt_column_get_value(subset2, 0, &get);

	if (get != 3)
	{
		fprintf(stderr, "Expected first value in subset2 to be 3 but got %d.\n", get);
		goto cleanup;
	}

	dt_column_get_value(subset2, 1, &get);

	if (get != 4)
	{
		fprintf(stderr, "Expected second value in subset2 to be 4 but got %d.\n", get);
		goto cleanup;
	}

	status = 0;
cleanup:
	dt_column_free(&column);
	dt_column_free(&subset);
	dt_column_free(&subset2);
	return status;
}
