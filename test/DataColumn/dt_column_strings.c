#include "DataColumn.h"
#include <stdio.h>

int main()
{
	int status = -1;

	struct DataColumn* column = NULL;
	dt_column_create(&column, 5, STRING);

	char* set = strdup("hello there");
	dt_column_set_value(column, 0, &set);

	char** get = dt_column_get_value_ptr(column, 0);

	if (strcmp(*get, "hello there") != 0)
	{
		fprintf(stderr, "Expected value at index 0 to be 'hello there' but got '%s'.\n", *get);
		goto cleanup;
	}

	status = 0;
cleanup:
	dt_column_free(&column);
	return status;
}
