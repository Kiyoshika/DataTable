#include "DataTable.h"
#include <stdio.h>

void
add_columns(
	void* current_row_value,
	void* user_data,
	const void** const column_values)
{
	int32_t* value = current_row_value;
	const int32_t* const col1 = column_values[0];
	const int32_t* const col2 = column_values[1];

	*value = *col1 + *col2;
}

int main()
{
	int status = -1;
	
	char colnames[3][MAX_COL_LEN] = { "col1", "col2", "col3" };
	enum data_type_e types[3] = { INT32, INT32, INT32 };
	struct DataTable* table = dt_table_create(3, colnames, types);

	int32_t set1 = 10;
	int32_t set2 = 12;
	int32_t set3 = 0;
	dt_table_insert_row(table, 3, &set1, &set2, &set3);

	set1 = 20;
	set2 = 22;
	set3 = 0;
	dt_table_insert_row(table, 3, &set1, &set2, &set3);

	set1 = 30;
	set2 = 32;
	set3 = 0;
	dt_table_insert_row(table, 3, &set1, &set2, &set3);

	// pass these columns to the callback function
	const char column_value_names[2][MAX_COL_LEN] = { "col1", "col2" };
	dt_table_apply_column(table, "col3", &add_columns, NULL, column_value_names, 2);

	int32_t* get = NULL;
	get = dt_table_get_value(table, 0, 2);
	if (*get != 22)
	{
		fprintf(stderr, "Was expecting value at (0, 2) to be 22 but got %d instead.\n", *get);
		goto cleanup;	
	}

	get = dt_table_get_value(table, 1, 2);
	if (*get != 42)
	{
		fprintf(stderr, "Was expecting value at (1, 2) to be 42 but got %d instead.\n", *get);
		goto cleanup;
	}

	get = dt_table_get_value(table, 2, 2);
	if (*get != 62)
	{
		fprintf(stderr, "Was expecting value at (2, 2) to be 62 but got %d instead.\n", *get);
		goto cleanup;
	}

	status = 0;
cleanup:
	dt_table_free(&table);
	return status;
}
