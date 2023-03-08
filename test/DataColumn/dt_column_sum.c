#include "DataColumn.h"
#include <stdio.h>

int main()
{
	int status = -1;
	
	struct DataColumn* column = NULL;
	dt_column_create(&column, 5, INT32);

	int32_t set = 11;
	dt_column_fill_values(column, &set);

	int32_t sum = 0;
	dt_column_sum(column, &sum);

	if (sum != 55)
	{
		fprintf(stderr, "Expected sum to be 55 but got %d.\n", sum);
		goto cleanup;
	}
	status = 0;
cleanup:
	dt_column_free(&column);
	return status;
}
