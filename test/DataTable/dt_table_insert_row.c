#include "DataTable.h"
#include <stdio.h>
#include <math.h>

int main()
{
	int status = -1;

	char colnames[2][DT_MAX_COL_LEN] = { "col1", "col2" };
	enum data_type_e types[2] = { INT32, FLOAT };
	struct DataTable* table = dt_table_create(2, colnames, types);

	int32_t set1 = 10;
	float set2 = 5.5f;
	dt_table_insert_row(table, 2, &set1, &set2);

	set1 = 20;
	set2 = 12.52f;
	dt_table_insert_row(table, 2, &set1, &set2);

	set2 = 21.21f;
	dt_table_insert_row(table, 2, NULL, &set2);

	if (table->n_rows != 3)
	{
		fprintf(stderr, "Expected n_rows to be 3 but got %zu.\n", table->n_rows);
		goto cleanup;
	}
	
	int32_t get1 = 0;
	dt_column_get_value(table->columns[0].column, 0, &get1);
	if (get1 != 10)
	{
		fprintf(stderr, "Expected value at (0, 0) to be %d but got %d.\n", 10, get1);
		goto cleanup;
	}

	float get2 = 0.0f;
	dt_column_get_value(table->columns[1].column, 0, &get2);
	if (fabsf(get2 - 5.5f) > 0.0001f)
	{
		fprintf(stderr, "Expected value at (0, 1) to be %f but got %f.\n", 5.5f, get2);
		goto cleanup;
	}

	dt_column_get_value(table->columns[0].column, 1, &get1);
	if (get1 != 20)
	{
		fprintf(stderr, "Expected value at (1, 0) to be %d but got %d.\n", 20, get1);
		goto cleanup;
	}

	dt_column_get_value(table->columns[1].column, 1, &get2);
	if (fabsf(get2 - 12.52f) > 0.0001f)
	{
		fprintf(stderr, "Expected value at (1, 1) to be %f but got %f.\n", 12.52f, get2);
		goto cleanup;
	}

	dt_column_get_value(table->columns[0].column, 2, &get1);
	if (get1 != 0)
	{
		fprintf(stderr, "Expected value at (2, 0) to be %d but got %d.\n", 0, get1);
		goto cleanup;
	}

	dt_column_get_value(table->columns[1].column, 2, &get2);
	if (fabsf(get2 - 21.21f) > 0.0001f)
	{
		fprintf(stderr, "Expected value at (2, 1) to be %f but got %f.\n", 21.21f, get2);
		goto cleanup;
	}

	status = 0;
cleanup:
	dt_table_free(&table);
	return status;
}
