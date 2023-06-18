#include "DataTable.h"
#include <stdio.h>

int main()
{
	int status = -1;

	char colnames[3][DT_MAX_COL_LEN] = { "col1", "col2", "col3" };
	enum data_type_e types[3] = { INT32, FLOAT, UINT8 };
	struct DataTable* table = dt_table_create(3, colnames, types);

	int32_t set1 = 10;
	float set2 = 5.5f;
	uint8_t set3 = 1;
	dt_table_insert_row(table, 3, &set1, &set2, &set3);

	set1 = 20;
	set2 = 12.52f;
	set3 = 2;
	dt_table_insert_row(table, 3, &set1, &set2, &set3);

	set2 = 21.21f;
	set3 = 3;
	dt_table_insert_row(table, 2, NULL, &set2, &set3);

	const char drop_cols[2][DT_MAX_COL_LEN] = { "col3", "col1" };
	dt_table_drop_columns_by_name(table, 2, drop_cols);

	if (table->n_columns != 1)
	{
		fprintf(stderr, "Was expecting n_columns to be 1 but got %zu.\n", table->n_columns);
		goto cleanup;
	}

	if (strcmp(table->columns[0].name, "col2") != 0)
	{
		fprintf(stderr, "Was expecting column name to be 'col2' but got '%s'.\n", table->columns[0].name);
		goto cleanup;
	}

	if (table->columns[0].column->type != FLOAT)
	{
		fprintf(stderr, "Was expecting column type by FLOAT.\n");
		goto cleanup;
	}

	status = 0;
cleanup:
	dt_table_free(&table);
	return status;
}
