#include "DataColumn.h"
#include <stdio.h>

int main()
{
	int status = -1;
	
	struct DataColumn* column = NULL;
	dt_column_create(&column, 5, INT32);

	int32_t set = 11;
	dt_column_fill_values(column, &set);

	set = 22;
	dt_column_set_value(column, 0, &set);

	set = 33;
	dt_column_set_value(column, 1, &set);

	// 22 33 11 11 11
	int32_t avg = 0;
	dt_column_avg(column, &avg);

	if (avg != 17) // is 17.6 but truncated due to integer
	{
		fprintf(stderr, "Expected (integer) average to be 17 but got %d.\n", avg);
		goto cleanup;
	}
	status = 0;
cleanup:
	dt_column_free(&column);
	return status;
}
