#include "DataTable.h"
#include <stdio.h>

int main()
{
	int status = -1;

	char colnames[1][DT_MAX_COL_LEN] = { "col1" };
	enum data_type_e types[1] = { INT32 };
	struct DataTable* table = dt_table_create(1, colnames, types);

	// insert 5 blank rows
	for (size_t i = 0; i < 5; ++i)
		dt_table_insert_row(table, 1, NULL);

	// fill values by index
	int32_t fill = 10;
	dt_table_fill_column_values_by_index(table, 0, &fill);
	for (size_t i = 0; i < 5; ++i)
	{
		const int32_t* get = dt_table_get_value(table, i, 0);
		if (*get != 10)
		{
			fprintf(stderr, "Expected value 10 but got %d.\n", *get);
			goto cleanup;
		}
	}

	// fill values by name
	fill = 20;
	dt_table_fill_column_values_by_name(table, "col1", &fill);
	for (size_t i = 0; i < 5; ++i)
	{
		const int32_t* get = dt_table_get_value(table, i, 0);
		if (*get != 20)
		{
			fprintf(stderr, "Expected value 20 but got %d.\n", *get);
			goto cleanup;
		}
	}

	status = 0;
cleanup:
	dt_table_free(&table);
	return status;
}
