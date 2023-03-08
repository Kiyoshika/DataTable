#include "DataColumn.h"
#include <stdio.h>

int main()
{
	int status = -1;

	struct DataColumn* column1 = NULL;
	dt_column_create(&column1, 5, INT32);
	int32_t set = 10;
	dt_column_fill_values(column1, &set);

	struct DataColumn* column2 = NULL;
	dt_column_create(&column2, 5, INT32);
	set = 20;
	dt_column_fill_values(column2, &set);

	struct DataColumn* column3 = NULL;
	dt_column_create(&column3, 5, INT32);
	set = 30;
	dt_column_fill_values(column3, &set);

	dt_column_union_multiple(column1, 2, column2, column3);

	if (column1->n_values != 15)
	{
		fprintf(stderr, "Expected n_values to be 15 but got %zu.\n", column1->n_values);
		goto cleanup;
	}

	if (column1->value_capacity != 15)
	{
		fprintf(stderr, "Expected capacity to be 15 but got %zu.\n", column1->value_capacity);
		goto cleanup;
	}

	int32_t get = 0;

	// check first chunk of values
	for (size_t i = 0; i < 5; ++i)
	{
		dt_column_get_value(column1, i, &get);
		if (get != 10)
		{
			fprintf(stderr, "Expected value 10 at index %zu but got %d.\n", i, get);
			goto cleanup;
		}
	}

	// check second chunk of values
	for (size_t i = 5; i < 10; ++i)
	{
		dt_column_get_value(column1, i, &get);
		if (get != 20)
		{
			fprintf(stderr, "Expected value 20 at index %zu but got %d.\n", i, get);
			goto cleanup;
		}
	}

	// check third chunk of values
	for (size_t i = 10; i < 15; ++i)
	{
		dt_column_get_value(column1, i, &get);
		if (get != 30)
		{
			fprintf(stderr, "Expected value 30 at index %zu but got %d.\n", i, get);
			goto cleanup;
		}
	}

	status = 0;
cleanup:
	dt_column_free(&column1);
	dt_column_free(&column2);
	dt_column_free(&column3);
	return status;
}
