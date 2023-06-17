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

	struct DataTable* table2 = dt_table_create(
			2, 
			(const char[2][DT_MAX_COL_LEN]){ "col3", "col4" }, 
			(const enum data_type_e[2]){ DOUBLE, INT8 });

	double set3 = 1.21;
	int8_t set4 = -4;
	dt_table_insert_row(table2, 2, &set3, &set4);

	set3 = 2.156;
	set4 = 33;
	dt_table_insert_row(table2, 2, &set3, &set4);

	set4 = 11;
	dt_table_insert_row(table2, 2, NULL, &set4);

	dt_table_append_by_column(table, table2);

	if (table->n_columns != 4)
	{
		fprintf(stderr, "Expected table to have 4 columns but has %zu.\n", table->n_columns);
		goto cleanup;
	}

	if (strcmp(table->columns[2].name, "col3") != 0)
	{
		fprintf(stderr, "Expected third column to be 'col3' but is '%s'.\n", table->columns[2].name);
		goto cleanup;
	}

	if (strcmp(table->columns[3].name, "col4") != 0)
	{
		fprintf(stderr, "Expected fourth column to be 'col4' but is '%s'.\n", table->columns[3].name);
		goto cleanup;
	}

	// TESTING DUPLICATE NAMES (should fail)
	// "col1" is the duplicate name in this case
	struct DataTable* table3 = dt_table_create(
			2, 
			(const char[2][DT_MAX_COL_LEN]){ "col5", "col1" }, 
			(const enum data_type_e[2]){ DOUBLE, INT8 });

	set3 = 1.21;
	set4 = -4;
	dt_table_insert_row(table3, 2, &set3, &set4);

	set3 = 2.156;
	set4 = 33;
	dt_table_insert_row(table3, 2, &set3, &set4);

	set4 = 11;
	dt_table_insert_row(table3, 2, NULL, &set4);

	if (dt_table_append_by_column(table3, table) == DT_SUCCESS)
	{
		fprintf(stderr, "Expected appending with duplicate column to fail but it succeeded.\n");
		goto cleanup;
	}

	
	// TESTING MISMATCHED ROW SIZES (should fail)
	struct DataTable* table4 = dt_table_create(
			2, 
			(const char[2][DT_MAX_COL_LEN]){ "col5", "col1" }, 
			(const enum data_type_e[2]){ DOUBLE, INT8 });

	if (dt_table_append_by_column(table, table4) == DT_SUCCESS)
	{
		fprintf(stderr, "Expected appending with mismatched row sizes to fail but it succeeded.\n");
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
