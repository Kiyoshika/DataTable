#include "DataTable.h"
#include <stdio.h>

int main()
{
	int status = -1;

	char colnames[2][MAX_COL_LEN] = { "col1", "col2" };
	enum data_type_e types[2] = { INT32, INT32 };
	struct DataTable* table = dt_table_create(2, colnames, types);

	int32_t set1 = 10;
	dt_table_insert_row(table, 2, &set1, NULL);

	set1 = 20;
	int32_t set2 = 12;
	dt_table_insert_row(table, 2, &set1, &set2);

	set2 = 21;
	dt_table_insert_row(table, 2, NULL, &set2);

	int32_t fill = 55;
	dt_table_replace_all_null_values(table, &fill);

	// check that there are no more NULL values
	struct DataColumn* column = dt_table_get_column_ptr_by_index(table, 0);
	if (column->n_null_values > 0)
	{
		fprintf(stderr, "Expected no NULL values in column 0 but got %zu.\n", column->n_null_values);
		goto cleanup;
	}

	column = dt_table_get_column_ptr_by_index(table, 1);
	if (column->n_null_values > 0)
	{
		fprintf(stderr, "Expected no NULL values in column 1 but got %zu.\n", column->n_null_values);
		goto cleanup;

	}

	const int32_t* get = dt_table_get_value(table, 0, 1);
	if (*get != 55)
	{
		fprintf(stderr, "Expected replaced value to be 55 but got %d.\n", *get);
		goto cleanup;
	}

	get = dt_table_get_value(table, 2, 0);
	if (*get != 55)
	{
		fprintf(stderr, "Expected replaced value to be 55 but got %d.\n", *get);
		goto cleanup;
	}

	status = 0;
cleanup:
	dt_table_free(&table);
	return status;
}
