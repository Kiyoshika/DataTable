#include "DataColumn.h"
#include <stdio.h>

int main()
{
	int status = -1;

	struct DataColumn* column = NULL;
	dt_column_create(&column, 1, INT32);

	int32_t set = 10;
	dt_column_set_value(column, 0, &set);

	set = 20;
	dt_column_append_value(column, &set);

	dt_column_resize(column, 3);

	// column should be: 10 20 0
	
	if (column->n_values != 3)
	{
		fprintf(stderr, "Expected n_values to be 3 but is %zu.\n", column->n_values);
		goto cleanup;
	}

	// NOTE: capacity is three because of the initial capacity rule: init_size * 2 + 1
	if (column->value_capacity != 3)
	{
		fprintf(stderr, "Expected capacity to be 3 but is %zu.\n", column->value_capacity);
		goto cleanup;
	}

	int32_t get = 0;

	dt_column_get_value(column, 0, &get);
	if (get != 10)
	{
		fprintf(stderr, "Expected first value to be 10 but is %d.\n", get);
		goto cleanup;
	}

	dt_column_get_value(column, 1, &get);
	if (get != 20)
	{
		fprintf(stderr, "Expected second value to be 20 but is %d.\n", get);
		goto cleanup;
	}

	dt_column_get_value(column, 2, &get);
	if (get != 0)
	{
		fprintf(stderr, "Expected third value to be 0 but is %d.\n", get);
		goto cleanup;
	}

	// resize again to a value beyond current capacity
	dt_column_resize(column, 10);

	if (column->n_values != 10)
	{
		fprintf(stderr, "Expected n_values to be 10 after resizing column to 10 but is %zu\n", column->n_values);
		goto cleanup;
	}

	if (column->value_capacity != 10)
	{
		fprintf(stderr, "Expected capacity to be 10 but is %zu\n", column->value_capacity);
		goto cleanup;
	}

	set = 30;
	dt_column_set_value(column, 9, &set);
	dt_column_get_value(column, 9, &get);

	if (get != 30)
	{
		fprintf(stderr, "Expected last item to be 30 but is %d.\n", get);
		goto cleanup;
	}
	status = 0;
cleanup:
	dt_column_free(&column);
	return status;
}
