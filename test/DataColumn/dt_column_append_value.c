#include "DataColumn.h"
#include <stdio.h>

int main()
{
	int status = -1;

	struct DataColumn* column = NULL;
	dt_column_create(&column, 5, INT32);

	int32_t set = 25;
	dt_column_append_value(column, &set);

	// column should look like: 0 0 0 0 0 25
	int32_t get = 0;
	dt_column_get_value(column, 5, &get);

	if (get != set)
	{
		fprintf(stderr, "Was expecting value %d but got %d instead.\n", set, get);
		goto cleanup;
	}

	// append NULL value
	dt_column_append_value(column, NULL);
	
	dt_column_get_value(column, 6, &get);
	if (get != 0)
	{
		fprintf(stderr, "Was expecting value %d but got %d instead.\n", 0, get);
		goto cleanup;
	}

	if (column->n_null_values != 1)
	{
		fprintf(stderr, "Was expecting null values to be 1 but god %zu instead.\n", column->n_null_values);
		goto cleanup;
	}

	// test appending with empty column
	struct DataColumn* empty = NULL;
	dt_column_create(&empty, 0, INT32);
	dt_column_append_value(empty, &set);
	dt_column_get_value(empty, 0, &get);

	if (get != set)
	{
		fprintf(stderr, "Was expecting value %d but got %d instead.\n", set, get);
		goto cleanup;
	}

	status = 0;
cleanup:
	dt_column_free(&column);
	dt_column_free(&empty);
	return status;
}
