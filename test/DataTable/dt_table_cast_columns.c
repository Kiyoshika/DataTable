#include "DataTable.h"
#include <stdio.h>

int main()
{
	int status = -1;
	
	char colnames[3][MAX_COL_LEN] = { "col1", "col2", "col3" };
	enum data_type_e types[3] = { INT32, FLOAT, STRING };
	struct DataTable* table = dt_table_create(3, colnames, types);

	int32_t set1 = 10;
	float set2 = 5.5f;
	char* set3 = strdup("50.322");
	dt_table_insert_row(table, 3, &set1, &set2, &set3);

	set1 = 20;
	set2 = 12.52f;
	free(set3);
	set3 = strdup("12.123");
	dt_table_insert_row(table, 3, &set1, &set2, &set3);

	set2 = 21.21f;
	free(set3);
	set3 = strdup("1.245");
	dt_table_insert_row(table, 3, NULL, &set2, &set3);
	free(set3);

	// cast INT32 -> STRING 
	// cast FLOAT -> UINT8 
	// cast STRING -> DOUBLE
	dt_table_cast_columns(table, 3,
			(const char[3][MAX_COL_LEN]){ "col1", "col2", "col3" },
			(const enum data_type_e[3]){ STRING, UINT8, DOUBLE });

	// VERIFY NEW COLUMN TYPES
	
	if (table->columns[0].column->type != STRING)
	{
		fprintf(stderr, "Expected first column type to be STRING.\n");
		goto cleanup;
	}

	if (table->columns[1].column->type != UINT8)
	{
		fprintf(stderr, "Expected second column type to be UINT8.\n");
		goto cleanup;
	}

	if (table->columns[2].column->type != DOUBLE)
	{
		fprintf(stderr, "Expected third column type to be DOUBLE.\n");
		goto cleanup;
	}

	// VERIFY SAMPLE VALUES FROM THE FIRST ROW
	
	const char** value1 = (const char**)dt_table_get_value(table, 0, 0);
	if (strcmp(*value1, "10") != 0)
	{
		fprintf(stderr, "Expected value at (0, 0) to be '10' but got '%s'.\n", *value1);
		goto cleanup;
	}

	const uint8_t* value2 = dt_table_get_value(table, 0, 1);
	if (*value2 != 5)
	{
		fprintf(stderr, "Expected value at (0, 1) to be 5 but got %d.\n", *value2);
		goto cleanup;
	}

	const double* value3 = dt_table_get_value(table, 0, 2);
	if (abs(*value3 - 50.322) > 0.0001)
	{
		fprintf(stderr, "Expected value at (0, 2) to be 50.322 but got %f.\n", *value3);
		goto cleanup;
	}

	status = 0;
cleanup:
	dt_table_free(&table);
	return status;
}
