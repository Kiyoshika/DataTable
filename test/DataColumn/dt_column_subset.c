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

	struct DataColumn* subset = dt_column_subset(column, subset_idx, 2);
	
	if (subset->n_values != 2)
	{
		fprintf(stderr, "Expected subset to have 2 values but has %zu.\n", subset->n_values);
		goto cleanup;
	}

	int32_t get = 0;
	dt_column_get_value(subset, 0, &get);

	if (get != 3)
	{
		fprintf(stderr, "Expected first value to be 3 but got %d.\n", get);
		goto cleanup;
	}

	dt_column_get_value(subset, 1, &get);

	if (get != 4)
	{
		fprintf(stderr, "Expected second value to be 4 but got %d.\n", get);
		goto cleanup;
	}

	status = 0;
cleanup:
	dt_column_free(&column);
	dt_column_free(&subset);
	return status;
}
