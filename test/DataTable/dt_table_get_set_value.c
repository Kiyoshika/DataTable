#include "DataTable.h"
#include <stdio.h>

int main()
{
	int status = -1;
	
	char colnames[2][MAX_COL_LEN] = { "col1", "col2" };
	enum data_type_e types[2] = { INT32, INT32 };
	struct DataTable* table = dt_table_create(2, colnames, types);

	int32_t set1 = 10;
	int32_t set2 = 20;
	dt_table_insert_row(table, 2, &set1, &set2);

	// should now be { 10, 30 }
	set1 = 30;
	dt_table_set_value(table, 0, 1, &set1);

	const int32_t* get = dt_table_get_value(table, 0, 1);
	if (*get != 30)
	{
		fprintf(stderr, "Expected value to be 30 but got %d.\n", *get);
		goto cleanup;
	}

	status = 0;
cleanup:
	dt_table_free(&table);
	return status;
}
