#include "DataColumn.h"
#include <stdio.h>

int main()
{
	int status = -1;

	struct DataColumn* column = NULL;
	struct DataColumn* copy = NULL;
	dt_column_create(&column, 5, STRING);

	char* set = strdup("hello there");
	dt_column_set_value(column, 0, &set);

	char** get = dt_column_get_value_ptr(column, 0);

	if (strcmp(*get, "hello there") != 0)
	{
		fprintf(stderr, "Expected value at index 0 to be 'hello there' but got '%s'.\n", *get);
		goto cleanup;
	}

	// append value will create a new heap copy for heap-allocated objects
	// so we manually have to free these local copies)
	set = strdup("this is appended");
	dt_column_append_value(column, &set);
	free(set);

	get = dt_column_get_value_ptr(column, 5);
	if (strcmp(*get, "this is appended") != 0)
	{
		fprintf(stderr, "Expected value at index 5 to be 'this is appended' but got '%s'.\n", *get);
		goto cleanup;
	}

	set = strdup("these values are filled");
	dt_column_fill_values(column, &set);
	free(set);

	for (size_t i = 0; i < column->n_values; ++i)
	{
		get = dt_column_get_value_ptr(column, i);
		if (strcmp(*get, "these values are filled") != 0)
		{
			fprintf(stderr, "Expected value at index %zu to be 'these values are filled' but got '%s'.\n", i, *get);
			goto cleanup;
		}
	}

	// when creating a copy, it should be a deep copy, so the addresses
	// of the strings should all be different
	copy = dt_column_copy(column);
	for (size_t i = 0; i < column->n_values; ++i)
	{
		get = dt_column_get_value_ptr(column, i);
		char** get2 = dt_column_get_value_ptr(copy, i);
		if (get == get2)
		{
			fprintf(stderr, "Addresses of strings should be different when creating a copy.\n");
			goto cleanup;
		}
	}

	status = 0;
cleanup:
	dt_column_free(&column);
	if (copy)
		dt_column_free(&copy);
	return status;
}
