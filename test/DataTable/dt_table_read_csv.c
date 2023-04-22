#include "DataTable.h"
#include <stdio.h>

int main()
{
	int status = -1;

	// read CSV without providing column types (last args)
	struct DataTable* default_table = dt_table_read_csv("./mytable.csv", ',', NULL);

	// verify number of columns
	if (default_table->n_columns != 3)
	{
		fprintf(stderr, "Expected three columns but got %zu.\n", default_table->n_columns);
		goto cleanup;
	}

	// verify column names
	if (strcmp(default_table->columns[0].name, "col1") != 0)
	{
		fprintf(stderr, "Expected first column to be named 'col1' but got '%s'.\n", default_table->columns[0].name);
		goto cleanup;
	}

	if (strcmp(default_table->columns[1].name, "col2") != 0)
	{
		fprintf(stderr, "Expected first column to be named 'col2' but got '%s'.\n", default_table->columns[1].name);
		goto cleanup;
	}

	if (strcmp(default_table->columns[2].name, "col3") != 0)
	{
		fprintf(stderr, "Expected first column to be named 'col3' but got '%s'.\n", default_table->columns[2].name);
		goto cleanup;
	}
	
	// verify number of rows
	if (default_table->n_rows != 3)
	{
		fprintf(stderr, "Expected three rows but got %zu.\n", default_table->n_rows);
		goto cleanup;
	}	

	// verify NULL value counts
	if (default_table->columns[0].column->n_null_values != 0)
	{
		fprintf(stderr, "Expected 0 NULL values in col1 but got %zu.\n", default_table->columns[0].column->n_null_values);
		goto cleanup;
	}

	if (default_table->columns[1].column->n_null_values != 1)
	{
		fprintf(stderr, "Expected 1 NULL value in col2 but got %zu.\n", default_table->columns[1].column->n_null_values);
		goto cleanup;
	}	

	if (default_table->columns[2].column->n_null_values != 0)
	{
		fprintf(stderr, "Expected 0 NULL values in col3 but got %zu.\n", default_table->columns[2].column->n_null_values);
		goto cleanup;
	}
	status = 0;
cleanup:
	dt_table_free(&default_table);
	return status;
}
