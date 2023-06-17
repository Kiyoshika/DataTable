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

	set1 = 30;
	set2 = 21.21f;
	dt_table_insert_row(table, 2, &set1, &set2);

	// sample 10 rows with replacement
	struct DataTable* samples_with_replace = dt_table_sample_rows(table, 10, true);

	if (samples_with_replace->n_rows != 10)
	{
		fprintf(stderr, "Expected samples_with_replace table to have 10 records but has %zu.\n", samples_with_replace->n_rows);
		goto cleanup;
	}

	// sample 2 rows without replacement
	struct DataTable* samples_without_replace_1 = dt_table_sample_rows(table, 2, false);
	
	if (samples_without_replace_1->n_rows != 2)
	{
		fprintf(stderr, "Expected samples_without_replace_1 table to have 2 records but has %zu.\n", samples_without_replace_1->n_rows);
		goto cleanup;
	}	

	// attempt to sample more rows than we have (should return NULL)
	struct DataTable* samples_without_replace_2 = dt_table_sample_rows(table, 10, false);

	if (samples_without_replace_2 != NULL)
	{
		fprintf(stderr, "Expected samples_without_replace_2 table to be NULL but wasn't...\n");
		goto cleanup;
	}
	

	status = 0;
cleanup:
	dt_table_free(&table);
	dt_table_free(&samples_with_replace);
	dt_table_free(&samples_without_replace_1);
	return status;
}
