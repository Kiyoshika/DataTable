#include "DataTable.h"
#include <stdio.h>

int main()
{
	int status = -1;

	char colnames[2][MAX_COL_LEN] = { "col1", "col2" };
	enum data_type_e types[2] = { INT32, FLOAT };
	struct DataTable* table = dt_table_create(2, colnames, types);

	int32_t set1 = 10;
	float set2 = 5.5f;
	dt_table_insert_row(table, 2, &set1, NULL);

	set1 = 20;
	set2 = 12.52f;
	dt_table_insert_row(table, 2, &set1, &set2);

	set2 = 21.21f;
	dt_table_insert_row(table, 2, &set1, NULL);

	struct DataTable* table2 = dt_table_copy(table);
	dt_table_drop_columns_with_null(table2);

	if (table2->n_columns != 1)
	{
		fprintf(stderr, "Expected 1 column after droping columns but have %zu.\n", table2->n_columns);
		goto cleanup;
	}

	if (strcmp(table2->columns[0].name, "col1") != 0)
	{
		fprintf(stderr, "Expected column name to be 'col1' but got '%s'.\n", table2->columns[0].name);
		goto cleanup;
	}

	if (table2->columns[0].column->type != INT32)
	{
		fprintf(stderr, "Expected column type to be INT32\n");
		goto cleanup;
	}

	struct DataTable* table3 = dt_table_copy(table);
	dt_table_drop_rows_with_null(table3);

	if (table3->n_rows != 1)
	{
		fprintf(stderr, "Expected 1 row after dropping rows but have %zu.\n", table3->n_rows);
		goto cleanup;
	}

	status = 0;
cleanup:
	dt_table_free(&table);
	dt_table_free(&table2);
	dt_table_free(&table3);
	return status;
}
