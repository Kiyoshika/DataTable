#include "DataColumn.h"
#include <stdio.h>

int main()
{
	int status = -1;
	
	struct DataColumn* column = NULL;

	/* 
	 * test three random properties. we don't need
	 * to test every type since it's controlled by an enum
	 * and would be redundant.
	 *
	 * one could argue to test get_type_size but I've explicitly
	 * left the default case out to generate a compiler warning if
	 * a type is not defined in there so it would be captured anyways
	 * (as long as you're building in debug mode with warnings).
	 */	

	if (!dt_column_create(&column, 5, FLOAT))
	{
		fprintf(stderr, "Couldn't allocate memory for DataColumn.\n");
		return -1;
	}

	// the internal capacity is actually doubled (and incremented by 1) but the "logical" size is still 5
	if (column->value_capacity != 11)
	{
		fprintf(stderr, "Expecting capacity of 10 but got %zu.\n", column->value_capacity);
		goto cleanup;
	}

	if (column->n_values != 5)
	{
		fprintf(stderr, "Expecting n_values to be 5 but got %zu.\n", column->n_values);
		goto cleanup;
	}

	if (column->type != FLOAT)
	{
		fprintf(stderr, "Expecting FLOAT type but got %s.\n", dt_type_to_str(column->type));
		goto cleanup;
	}

	if (column->type_size != sizeof(float))
	{
		fprintf(stderr, "Expecting type_size of %zu but got %zu.\n", sizeof(float), column->type_size);
		goto cleanup;
	}

	status = 0; // all tests pass
cleanup:
	dt_column_free(&column);

	return status;
}
