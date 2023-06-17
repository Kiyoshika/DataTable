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

	struct DataTable* copy = dt_table_copy(table);

	// comparing addresses of tables
	if (copy == table)
	{
		fprintf(stderr, "Addresses of table and its copy are the same.\n");
		goto cleanup;
	}
	
	// comparing addresses of columns
	for (size_t i = 0; i < table->n_columns; ++i)
	{
		if (table->columns[i].column == copy->columns[i].column)
		{
			fprintf(stderr, "Addresses of columns between table and its copy are the same.\n");
			goto cleanup;
		}
	}
	
	status = 0;
cleanup:
	dt_table_free(&table);
	dt_table_free(&copy);
	return status;
}
