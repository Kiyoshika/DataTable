#include "DataColumn.h"
#include <stdio.h>

int main()
{
	int status = -1;

	struct DataColumn* column = NULL;
	dt_column_create(&column, 5, INT32);
	
	int32_t fill = 10;
	dt_column_fill_values(column, &fill);

	struct DataColumn* column_copy = dt_column_copy(column);

	// make sure values are copied and not references (e.g.,
	// changes in one should NOT reflect in the other)
	int32_t set = 25;
	dt_column_set_value(column, 0, &set);

	int32_t get = 0;
	dt_column_get_value(column_copy, 0, &get);

	if (get == 25)
	{
		fprintf(stderr, "References were copied instead of values.\n");
		goto cleanup;
	}

	if (get != 10)
	{
		fprintf(stderr, "Expected value 10 but got %d instead.\n", get);
		goto cleanup;
	}
	status = 0;
cleanup:
	dt_column_free(&column);
	dt_column_free(&column_copy);
	return status;
}
