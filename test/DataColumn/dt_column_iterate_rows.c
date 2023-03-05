#include "DataColumn.h"
#include <stdio.h>

// this is a sample callback that doubles the value of the row inplace
void double_values_inplace(
	void* item,
	void* user_data)
{
	// doing nothing with user data
	(void)user_data;

	int32_t* _item = item;
	*_item *= 2;
}

// this is a sample callback that adds the value of each row into a different
// variable passed by the user
void sum_variable(
	void* item,
	void* user_data)
{
	int32_t* data = user_data;
	int32_t* _item = item;

	*data += *_item;
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

	dt_column_iterate_rows(column, NULL, &double_values_inplace);

	int32_t get = 0;
	dt_column_get_value(column, 0, &get);
	if (get != 2)
	{
		fprintf(stderr, "Was expecting index 0 to be 2 but got %d instead.\n", get);
		goto cleanup;
	}

	dt_column_get_value(column, 1, &get);
	if (get != 4)
	{
		fprintf(stderr, "Was expecting index 1 to be 4 but got %d instead.\n", get);
		goto cleanup;
	}

	dt_column_get_value(column, 2, &get);
	if (get != 6)
	{
		fprintf(stderr, "Was expecting index 2 to be 6 but got %d instead.\n", get);
		goto cleanup;
	}

	dt_column_get_value(column, 3, &get);
	if (get != 8)
	{
		fprintf(stderr, "Was expecting index 3 to be 8 but got %d instead.\n", get);
		goto cleanup;
	}

	dt_column_get_value(column, 4, &get);
	if (get != 10)
	{
		fprintf(stderr, "Was expecting index 4 to be 10 but got %d instead.\n", get);
		goto cleanup;
	}

	int32_t sum = 0;
	dt_column_iterate_rows(column, &sum, &sum_variable);

	if (sum != 30)
	{
		fprintf(stderr, "Was expecting sum to be 30 but got %d instead.\n", sum);
		goto cleanup;
	}

	status = 0;
cleanup:
	dt_column_free(&column);
	return status;
}
