#include "DataTable.h"
#include <stdio.h>

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

	struct DataTable* table2 = dt_table_copy(table);
	struct DataColumn* column = dt_table_get_column_ptr_by_index(table, 1);
	
	dt_table_insert_column(table2, column, "new_col");

	if (table2->n_columns != 3)
	{
		fprintf(stderr, "Expected table2 to have 3 columns but has %zu.\n", table2->n_columns);
		goto cleanup;
	}

	if (table2->column_capacity != 3)
	{
		fprintf(stderr, "Expected table2 to have capacity of 3 but has %zu.\n", table2->column_capacity);
		goto cleanup;
	}

	if (table2->columns[2].column->type != FLOAT)
	{
		fprintf(stderr, "Expected new column type to be FLOAT.\n");
		goto cleanup;
	}

	if (table2->columns[2].column->n_values != table2->n_rows)
	{
		fprintf(stderr, "Expected new columns n_values to be the same as the the number of table rows.\n");
		goto cleanup;
	}

	if (strcmp(table2->columns[2].name, "new_col") != 0)
	{
		fprintf(stderr, "Expected new column's name to be 'new_col' but was '%s'.\n", table2->columns[2].name);
		goto cleanup;
	}

	status = 0;
cleanup:
	dt_table_free(&table);
	dt_table_free(&table2);
	return status;
}
