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
		fprintf(stderr, "Was expected value %d but got %d instead.\n", set, get);
		goto cleanup;
	}

	status = 0;
cleanup:
	dt_column_free(&column);
	return status;
}
