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

	ssize_t n_filtered_items = 0;
	size_t* filtered_idx = dt_column_filter(column, NULL, &filter_even, &n_filtered_items);

	if (n_filtered_items != 2)
	{
		fprintf(stderr, "Expected # of filtered items to be 2 but got %zu instead.\n", n_filtered_items);
		goto cleanup;
	}

	// reminder that it returns INDICES of the items, not the actual items themselves
	if (filtered_idx[0] != 1)
	{
		fprintf(stderr, "Expected first item in filter to be 1 but got %zu instead.\n", filtered_idx[0]);
		goto cleanup;
	}

	if (filtered_idx[1] != 3)
	{
		fprintf(stderr, "Expected second item in filter to be 3 but got %zu instead.\n", filtered_idx[1]);
		goto cleanup;
	}

	free(filtered_idx);
	filtered_idx = dt_column_filter(column, NULL, &dummy, &n_filtered_items);
	if (filtered_idx != NULL)
	{
		fprintf(stderr, "Expected filter_idx to be NULL after finding 0 items but contains an address.\n");
		goto cleanup;
	}

	status = 0;
cleanup:
	dt_column_free(&column);
	free(filtered_idx);
	return status;
}
