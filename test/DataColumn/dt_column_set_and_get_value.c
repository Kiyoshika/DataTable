#include "DataColumn.h"
#include "StatusCodes.h"
#include <stdio.h>

int main()
{
	int status = -1;
	
	struct DataColumn* column = NULL;
	dt_column_create(&column, 5, INT32);

	/* set and get values inside range */
	int32_t set = 25;
	dt_column_set_value(column, 0, &set);

	int32_t get = 0;
	dt_column_get_value(column, 0, &get);

	if (get != set)
	{
		fprintf(stderr, "Was expecting value %d but got %d instead.\n", set, get);
		goto cleanup;
	}

	/* attempt to get values outside of range (should be DT_INDEX_ERROR) */
	enum status_code_e status_code;
	status_code = dt_column_set_value(column, 5, &set);

	if (status_code != DT_INDEX_ERROR)
	{
		fprintf(stderr, "Was expecting a DT_INDEX_ERROR when setting a value out of bounds but instead got status code %d.\n", status_code);
		goto cleanup;
	}

	status_code = dt_column_get_value(column, 5, &get);
	if (status_code != DT_INDEX_ERROR)
	{
		fprintf(stderr, "Was expecting a DT_INDEX_ERROR error when getting value out of bounds but instead got status code %d.\n", status_code);
		goto cleanup;
	}

status = 0;
cleanup:
	dt_column_free(&column);
	return status;
}
