#include "DataColumn.h"
#include <stdio.h>

int main()
{
	int status = -1;

	struct DataColumn* column = NULL;
	dt_column_create(&column, 5, INT32);

	int32_t set = 25;
	dt_column_fill_values(column, &set);

	for (size_t i = 0; i < column->n_values; ++i)
	{
		int32_t get = 0;
		dt_column_get_value(column, i, &get);
		if (get != set)
		{
			fprintf(stderr, "Expected value %d at index %zu but got %d instead.\n", set, i, get);
			goto cleanup;
		}
	}
	status = 0;
cleanup:
	dt_column_free(&column);
	return status;
}
