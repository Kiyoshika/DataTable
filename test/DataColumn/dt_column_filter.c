#include "DataColumn.h"
#include <stdio.h>

// callback to filter for even numbers
bool filter_even(
	void* item,
	void* user_data)
{
	// not using user data
	(void)user_data;

	int32_t* _item = item;

	return *_item % 2 == 0;
}

// dummy callback that should return no results
bool dummy(
	void* item,
	void* user_data)
{
	(void)item;
	(void)user_data;
	return false;
}

int main()
{
	int status = -1;

	struct DataColumn* column = NULL;
	dt_column_create(&column, 5, INT32);

	int32_t set = 1;
	dt_column_set_value(column, 0, &set);

	set = 2;
	dt_column_set_value(column, 1, &set);

	set = 3;
	dt_column_set_value(column, 2, &set);

	set = 4;
	dt_column_set_value(column, 3, &set);

	set = 5;
	dt_column_set_value(column, 4, &set);

	size_t* filtered_idx = dt_column_filter(column, &filter_even, NULL);

	size_t expected[5] = { 0, 1, 0, 1, 0 };

	for (size_t i = 0; i < 5; ++i)
	{
		if (filtered_idx[i] != expected[i])
		{
			fprintf(stderr, "Expected value at index %zu to be %zu but got %zu.\n", i, expected[i], filtered_idx[i]);
			goto cleanup;
		}
	}	

	status = 0;
cleanup:
	dt_column_free(&column);
	free(filtered_idx);
	return status;
}
