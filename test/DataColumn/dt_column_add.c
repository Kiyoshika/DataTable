#include "DataColumn.h"
#include <stdio.h>

int main()
{
	int status = -1;
	
	struct DataColumn* dest = NULL;
	dt_column_create(&dest, 5, INT32);

	struct DataColumn* src = NULL;
	dt_column_create(&src, 5, INT32);

	struct DataColumn* difftype = NULL;
	dt_column_create(&difftype, 5, FLOAT);

	struct DataColumn* diffsize = NULL;
	dt_column_create(&diffsize, 10, INT32);

	int32_t set = 11;
	dt_column_fill_values(dest, &set);

	set = 22;
	dt_column_fill_values(src, &set);

	dt_column_add(dest, src);

	for (size_t i = 0; i < 5; ++i)
	{
		int32_t get = 0;
		dt_column_get_value(dest, i, &get);
		if (get != 33)
		{
			fprintf(stderr, "Expected value at index %zu to be 33 but got %d.\n", i, get);
			goto cleanup;
		}
	}

	if (dt_column_add(dest, difftype) != DT_TYPE_MISMATCH)
	{
		fprintf(stderr, "Expected DT_TYPE_MISMATCH when adding [dest] and [difftype].\n");
		goto cleanup;
	}

	if (dt_column_add(dest, diffsize) != DT_SIZE_MISMATCH)
	{
		fprintf(stderr, "Expected DT_SIZE_MISMATCH when adding [dest] and [diffsize].\n");
		goto cleanup;
	}
	status = 0;
cleanup:
	dt_column_free(&dest);
	dt_column_free(&src);
	dt_column_free(&difftype);
	dt_column_free(&diffsize);
	return status;
}
