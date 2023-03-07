#include "DataColumn.h"
#include <stdio.h>

int main()
{
	int status = -1;

	struct DataColumn* column = NULL;
	dt_column_create(&column, 5, INT32);

	struct DataColumn* column2 = NULL;
	dt_column_create(&column2, 1, INT64); // NOTE: different type
	
	struct DataColumn* column3 = NULL;
	dt_column_create(&column3, 3, INT32);

	if (dt_column_union(column, column2) != DT_TYPE_MISMATCH)
	{
		fprintf(stderr, "Was expecting DT_TYPE_MISMATCH when unioning column2 into column1.\n");
		goto cleanup;
	}

	int32_t fill = 10;
	dt_column_fill_values(column, &fill);

	fill = 20;
	dt_column_fill_values(column3, &fill);

	dt_column_union(column, column3);

	if (column->n_values != 8)
	{
		fprintf(stderr, "Was expecting n_values to be 8 but got %zu.\n", column->n_values);
		goto cleanup;
	}

	// capacity should still be 11 (initial capacity of column) since the union
	// did not go over capacity limit
	if (column->value_capacity != 11)
	{
		fprintf(stderr, "Was expecting capacity to be 11 but got %zu.\n", column->value_capacity);
		goto cleanup;
	}

	// check to make sure the values are in correct places
	for (size_t i = 0; i < 5; ++i)
	{
		int32_t get = 0;
		dt_column_get_value(column, i, &get);
		if (get != 10)
		{
			fprintf(stderr, "Was expecting value of 10 at index %zu but got %d.\n", i, get);
			goto cleanup;
		}
	}

	for (size_t i = 5; i < 8; ++i)
	{
		int32_t get = 0;
		dt_column_get_value(column, i, &get);
		if (get != 20)
		{
			fprintf(stderr, "Was expecting value of 20 at index %zu but got %d.\n", i, get);
			goto cleanup;
		}
	}

	// union column two more times to make sure capacity increases properly
	dt_column_union(column, column3);
	dt_column_union(column, column3);

	// now both the n_values and capacity should be 14
	if (column->n_values != 14)
	{
		fprintf(stderr, "Was expecting n_values to be 14 but got %zu.\n", column->n_values);
		goto cleanup;
	}

	if (column->value_capacity != 14)
	{
		fprintf(stderr, "Was expecting capacity to be 14 but got %zu.\n", column->value_capacity);
		goto cleanup;
	}
	status = 0;
cleanup:
	dt_column_free(&column);
	dt_column_free(&column2);
	dt_column_free(&column3);
	return status;
}
