#include "DataTable.h"
#include <stdio.h>

int main()
{
	int status = -1;
	
	char colnames[2][MAX_COL_LEN] = { "col1", "col2" };
	enum data_type_e types[2] = { INT32, FLOAT };
	struct DataTable* table = dt_table_create(2, colnames, types);

	int32_t set1 = 10;
	float set2 = 5.5f;
	dt_table_insert_row(table, 2, &set1, &set2);

	set1 = 20;
	set2 = 12.52f;
	dt_table_insert_row(table, 2, &set1, &set2);

	set1 = 30;
	set2 = 21.21f;
	dt_table_insert_row(table, 2, &set1, &set2);

	set1 = 40;
	set2 = 3.33f;
	dt_table_insert_row(table, 2, &set1, &set2);

	set1 = 50;
	set2 = 4.44f;
	dt_table_insert_row(table, 2, &set1, &set2);

	set1 = 60;
	set2 = 5.55f;
	dt_table_insert_row(table, 2, &set1, &set2);

	set1 = 70;
	set2 = 6.66f;
	dt_table_insert_row(table, 2, &set1, &set2);

	set1 = 80;
	set2 = 7.77f;
	dt_table_insert_row(table, 2, &set1, &set2);

	// split tables MUST be NULL, otherwise DT_BAD_ARG is returned
	struct DataTable* split1 = NULL;
	struct DataTable* split2 = NULL;
	dt_table_split(table, 0.75f, &split1, &split2);

	if (split1->n_rows != 6)
	{
		fprintf(stderr, "Expected split1 to have 6 rows but got %zu.\n", split1->n_rows);
		goto cleanup;
	}

	if (split2->n_rows != 2)
	{
		fprintf(stderr, "Expected split2 to have 2 rows but got %zu.\n", split2->n_rows);
		goto cleanup;
	}

	status = 0;
cleanup:
	dt_table_free(&table);
	dt_table_free(&split1);
	dt_table_free(&split2);
	return status;
}
