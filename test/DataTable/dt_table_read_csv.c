#include "DataTable.h"
#include <stdio.h>

int main()
{
	int status = -1;

	// read CSV without providing column types (last args)
	// this will infer the types to one of DOUBLE, STRING, UINT64, or INT64
	struct DataTable* inferred_table = dt_table_read_csv("./mytable.csv", ',', NULL);

	// verify number of columns
	if (inferred_table->n_columns != 3)
	{
		fprintf(stderr, "Expected three columns but got %zu.\n", inferred_table->n_columns);
		goto cleanup;
	}

	// verify column names
	if (strcmp(inferred_table->columns[0].name, "col1") != 0)
	{
		fprintf(stderr, "Expected first column to be named 'col1' but got '%s'.\n", inferred_table->columns[0].name);
		goto cleanup;
	}

	if (strcmp(inferred_table->columns[1].name, "col2") != 0)
	{
		fprintf(stderr, "Expected first column to be named 'col2' but got '%s'.\n", inferred_table->columns[1].name);
		goto cleanup;
	}

	if (strcmp(inferred_table->columns[2].name, "col3") != 0)
	{
		fprintf(stderr, "Expected first column to be named 'col3' but got '%s'.\n", inferred_table->columns[2].name);
		goto cleanup;
	}
	
	// verify number of rows
	if (inferred_table->n_rows != 3)
	{
		fprintf(stderr, "Expected three rows but got %zu.\n", inferred_table->n_rows);
		goto cleanup;
	}	

	// verify NULL value counts
	if (inferred_table->columns[0].column->n_null_values != 0)
	{
		fprintf(stderr, "Expected 0 NULL values in col1 but got %zu.\n", inferred_table->columns[0].column->n_null_values);
		goto cleanup;
	}

	if (inferred_table->columns[1].column->n_null_values != 1)
	{
		fprintf(stderr, "Expected 1 NULL value in col2 but got %zu.\n", inferred_table->columns[1].column->n_null_values);
		goto cleanup;
	}	

	if (inferred_table->columns[2].column->n_null_values != 0)
	{
		fprintf(stderr, "Expected 0 NULL values in col3 but got %zu.\n", inferred_table->columns[2].column->n_null_values);
		goto cleanup;
	}

	// verify inferred types
	if (inferred_table->columns[0].column->type != DOUBLE)
	{
		fprintf(stderr, "Expected first inferred column to be DOUBLE.\n");
		goto cleanup;
	}

	if (inferred_table->columns[1].column->type != STRING)
	{
		fprintf(stderr, "Expected second inferred column to be STRING.\n");
		goto cleanup;
	}

	if (inferred_table->columns[2].column->type != UINT64)
	{
		fprintf(stderr, "Expected third inferred column to be UINT64.\n");
		goto cleanup;
	}

	// read CSV with custom column types
	struct DataTable* custom_table = dt_table_read_csv("./mytable.csv", ',', (enum data_type_e[3]){DOUBLE, STRING, UINT8});

	// check the first row of each to verify
	const double* value1 = dt_table_get_value(custom_table, 0, 0);
	if (abs(*value1 - 12.52) > 0.0001)
	{
		fprintf(stderr, "Expected value at (0, 0) to be 12.52 but got %f.\n", *value1);
		goto cleanup;
	}

	// cast to ignore const warning, evening though I'm using const O.o)
	const char** value2 = (const char**)dt_table_get_value(custom_table, 0, 1);
	if (strcmp(*value2, "hello \"this,\'is,\" a string") != 0)
	{
		fprintf(stderr, "Expected value at (0, 1) to be 'hello \"this,\'is,\" a string' but got '%s'.\n", *value2);
		goto cleanup;
	}

	const uint8_t* value3 = dt_table_get_value(custom_table, 0, 2);
	if (*value3 != 12)
	{
		fprintf(stderr, "Expected value at (0, 2) to be 12 but got %d.\n", *value3);
		goto cleanup;
	}

	status = 0;
cleanup:
	dt_table_free(&inferred_table);
	dt_table_free(&custom_table);
	return status;
}
