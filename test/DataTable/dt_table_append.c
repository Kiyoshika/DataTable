#include "DataTable.h"
#include <stdio.h>
#include <math.h>

int main()
{
	int status = -1;

	// TABLE ONE
	char colnames[2][MAX_COL_LEN] = { "col1", "col2" };
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

	// TABLE TWO
	struct DataTable* table2 = dt_table_create(2, colnames, types);

	set1 = 32;
	set2 = 34.2f;
	dt_table_insert_row(table2, 2, &set1, &set2);

	set1 = 99;
	set2 = 62.39f;
	dt_table_insert_row(table2, 2, &set1, &set2);

	set1 = 8;
	set2 = 8.21f;
	dt_table_insert_row(table2, 2, &set1, &set2);

	// APPEND
	dt_table_append_multiple_by_row(table, 1, table2);
	if (table->n_rows != 6)
	{
		fprintf(stderr, "Expected 6 rows in table after appending but got %zu.\n", table->n_rows);
		goto cleanup;
	}

	// VALIDATE COL1
	const int32_t* get;
	if ((get = dt_table_get_value(table, 0, 0)) && *get != 10)
	{
		fprintf(stderr, "Expected value at (0, 0) to be 10 but got %d.\n", *get);
		goto cleanup;
	}

	if ((get = dt_table_get_value(table, 1, 0)) && *get != 20)
	{
		fprintf(stderr, "Expected value at (1, 0) to be 20 but got %d.\n", *get);
		goto cleanup;
	}

	if ((get = dt_table_get_value(table, 2, 0)) && *get != 0)
	{
		fprintf(stderr, "Expected value at (2, 0) to be 0 but got %d.\n", *get);
		goto cleanup;
	}

	if ((get = dt_table_get_value(table, 3, 0)) && *get != 32)
	{
		fprintf(stderr, "Expected value at (3, 0) to be 32 but got %d.\n", *get);
		goto cleanup;
	}

	if ((get = dt_table_get_value(table, 4, 0)) && *get != 99)
	{
		fprintf(stderr, "Expected value at (4, 0) to be 99 but got %d.\n", *get);
		goto cleanup;
	}

	if ((get = dt_table_get_value(table, 5, 0)) && *get != 8)
	{
		fprintf(stderr, "Expected value at (5, 0) to be 8 but got %d.\n", *get);
		goto cleanup;
	}

	// VALIDATE COL2
	const float* get2;
	if ((get2 = dt_table_get_value(table, 0, 1)) && fabsf(*get2 - 5.5f) > 0.0001f)
	{
		fprintf(stderr, "Expected value at (0, 1) to be 5.5 but got %f.\n", *get2);
		goto cleanup;
	}

	if ((get2 = dt_table_get_value(table, 1, 1)) && fabsf(*get2 - 12.52f) > 0.0001f)
	{
		fprintf(stderr, "Expected value at (1, 1) to be 12.52 but got %f.\n", *get2);
		goto cleanup;
	}

	if ((get2 = dt_table_get_value(table, 2, 1)) && fabsf(*get2 - 21.21f) > 0.0001f)
	{
		fprintf(stderr, "Expected value at (2, 1) to be 21.21 but got %f.\n", *get2);
		goto cleanup;
	}

	if ((get2 = dt_table_get_value(table, 3, 1)) && fabsf(*get2 - 34.2f) > 0.0001f)
	{
		fprintf(stderr, "Expected value at (3, 1) to be 34.2 but got %f.\n", *get2);
		goto cleanup;
	}

	if ((get2 = dt_table_get_value(table, 4, 1)) && fabsf(*get2 - 62.39f) > 0.0001f)
	{
		fprintf(stderr, "Expected value at (4, 1) to be 62.39 but got %f.\n", *get2);
		goto cleanup;
	}

	if ((get2 = dt_table_get_value(table, 5, 1)) && fabsf(*get2 - 8.21f) > 0.0001f)
	{
		fprintf(stderr, "Expected value at (5, 1) to be 8.21 but got %f.\n", *get2);
		goto cleanup;
	}

	// TABLE THREE (SHOULD FAIL FROM HAVING MORE COLUMNS)
	char colnames2[3][MAX_COL_LEN] = { "col1", "col2", "col3" };
	enum data_type_e types2[3] = { INT32, FLOAT, INT32 };
	struct DataTable* table3 = dt_table_create(3, colnames2, types2);

	enum status_code_e status_code = DT_SUCCESS;
	if ((status_code = dt_table_append_multiple_by_row(table, 1, table3)) == DT_SUCCESS)
	{
		fprintf(stderr, "Was expecting appending table3 into table to fail but it succeeded.\n");
		goto cleanup;
	}

	// TABLE FOUR (SHOULD FAIL FROM HAVING DIFFERENT TYPES)
	char colnames3[2][MAX_COL_LEN] = { "col1", "col2" };
	enum data_type_e types3[2] = { INT32, INT32 };
	struct DataTable* table4 = dt_table_create(2, colnames3, types3);

	if ((status_code = dt_table_append_multiple_by_row(table, 1, table4)) == DT_SUCCESS)
	{
		fprintf(stderr, "Was expecting appending table4 into table to fail but it succeeded.\n");
		goto cleanup;
	}

	status = 0;
cleanup:
	dt_table_free(&table);
	dt_table_free(&table2);
	dt_table_free(&table3);
	dt_table_free(&table4);
	return status;
}
