#include "DataTable.h"
#include <stdio.h>

bool my_callback(void* item, void* user_data)
{
	// unused
	(void)user_data;

	int32_t* _item = item;
	return *_item < 20;
}

int main()
{
	int status = -1;

	char colnames[2][MAX_COL_LEN] = { "col1", "col2" };
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

	// filter col1 for values < 20
	struct DataTable* filtered_table = dt_table_filter_by_name(
		table,
		"col1",
		&my_callback,
    NULL);

	if (filtered_table->n_rows != 1)
	{
		fprintf(stderr, "Expected n_rows to be 1 but got %zu.\n", filtered_table->n_rows);
		goto cleanup;
	}

	const int32_t* get = dt_table_get_value(filtered_table, 0, 0);
	if (*get != 10)
	{
		fprintf(stderr, "Expected value at (0, 0) to be 10 but got %d.\n", *get);
		goto cleanup;
	}

	status = 0;
cleanup:
	dt_table_free(&table);
	dt_table_free(&filtered_table);
	return status;
}
