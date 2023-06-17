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

	char select_cols[1][DT_MAX_COL_LEN] = { "col1" };
	struct DataTable* subset = dt_table_select(table, 1, select_cols);

	if (subset->n_columns != 1)
	{
		fprintf(stderr, "Expected subset to have 1 column but has %zu.\n", subset->n_columns);
		goto cleanup;
	}

	if (subset->columns[0].column->n_values != 3)
	{
		fprintf(stderr, "Expected column in subset to have 3 rows but has %zu.\n", subset->columns[0].column->n_values);
		goto cleanup;
	}

	if (subset->columns[0].column->type != INT32)
	{
		fprintf(stderr, "Expected column type for subset to be INT32.\n");
		goto cleanup;
	}

	const int32_t* get = dt_table_get_value(subset, 0, 0);
	if (*get != 10)
	{
		fprintf(stderr, "Expected first value in subset to be 10 but got %d.\n", *get);
		goto cleanup;
	}

	get = dt_table_get_value(subset, 1, 0);
	if (*get != 20)
	{
		fprintf(stderr, "Expected second value in subset to be 20 but got %d.\n", *get);
		goto cleanup;
	}

	get = dt_table_get_value(subset, 2, 0);
	if (*get != 0)
	{
		fprintf(stderr, "Expected third value in subset to be NULL but got %d.\n", *get);
		goto cleanup;
	}

	

	status = 0;
cleanup:
	dt_table_free(&table);
	dt_table_free(&subset);
	return status;
}
