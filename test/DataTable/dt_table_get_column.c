#include "DataTable.h"
#include <stdio.h>

int main()
{
	int status = -1;

	char colnames[2][DT_MAX_COL_LEN] = { "col1", "col2" };
	enum data_type_e types[2] = { INT32, FLOAT };
	struct DataTable* table = dt_table_create(2, colnames, types);

	int32_t set1 = 30;
	float set2 = 5.5f;
	dt_table_insert_row(table, 2, &set1, &set2);

	set1 = 10;
	set2 = 12.52f;
	dt_table_insert_row(table, 2, &set1, &set2);

	set1 = 20;
	set2 = 21.21f;
	dt_table_insert_row(table, 2, &set1, &set2);

	struct DataColumn* get = dt_table_get_column_ptr_by_index(table, 5);
	if (get != NULL)
	{
		fprintf(stderr, "Expected NULL after fetching column index 5.\n");
		goto cleanup;
	}

	get = dt_table_get_column_ptr_by_name(table, "aliens");
	if (get != NULL)
	{
		fprintf(stderr, "Expected NULL after fetching column name 'aliens'.\n");
		goto cleanup;
	}

	get = dt_table_get_column_ptr_by_name(table, "col2");
	// compare addresses
	if (get != table->columns[1].column)
	{
		fprintf(stderr, "Address of 'col2' does not match what's in table.\n");
		goto cleanup;
	}

	status = 0;
cleanup:
	dt_table_free(&table);
	return status;
}
