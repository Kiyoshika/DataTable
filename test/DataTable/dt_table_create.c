#include "DataTable.h"
#include <stdio.h>

int main()
{
	int status = -1;

	enum data_type_e dtypes[3] = { INT32, FLOAT, STRING };
	char colnames[3][MAX_COL_LEN] = { "col1", "col2", "col3" };

	struct DataTable* table = dt_table_create(3, colnames, dtypes);

	if (table->n_columns != 3)
	{
		fprintf(stderr, "Expected n_columns to be 3 but got %zu.\n", table->n_columns);
		goto cleanup;
	}

	if (strcmp(table->columns[0].name, "col1") != 0)
	{
		fprintf(stderr, "Expected first column name to be 'col1' but got '%s'.\n", table->columns[0].name);
		goto cleanup;
	}

	if (table->columns[0].column->type != INT32)
	{
		fprintf(stderr, "Expected first column type to be INT32.\n");
		goto cleanup;
	}

	if (strcmp(table->columns[1].name, "col2") != 0)
	{
		fprintf(stderr, "Expected second column name to be 'col2' but got '%s'.\n", table->columns[1].name);
		goto cleanup;
	}

	if (table->columns[1].column->type != FLOAT)
	{
		fprintf(stderr, "Expected second column type to be FLOAT.\n");
		goto cleanup;
	}

	if (strcmp(table->columns[2].name, "col3") != 0)
	{
		fprintf(stderr, "Expected third column name to be 'col3' but got '%s'.\n", table->columns[2].name);
		goto cleanup;
	}

	if (table->columns[2].column->type != STRING)
	{
		fprintf(stderr, "Expected third column type to be STRING.\n");
		goto cleanup;
	}



	status = 0;
cleanup:
	dt_table_free(&table);
	return status;
}
