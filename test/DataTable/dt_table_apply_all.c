#include "DataTable.h"
#include <stdio.h>

// user callback to double all values in table
// NOTE: all values are int32_t
void
double_values(
	void* current_cell_value,
	void* user_data)
{
	(void)user_data; // unused

	int32_t* value = current_cell_value;
	*value *= 2;
}

int main()
{
	int status = -1;

	char colnames[2][DT_MAX_COL_LEN] = { "col1", "col2" };
	enum data_type_e types[2] = { INT32, INT32 };
	struct DataTable* table = dt_table_create(2, colnames, types);

	int32_t set1 = 10;
	int32_t set2 = 10;
	dt_table_insert_row(table, 2, &set1, &set2);

	set1 = 10;
	set2 = 10;
	dt_table_insert_row(table, 2, &set1, &set2);

	set1 = 10;
	set2 = 10;
	dt_table_insert_row(table, 2, &set1, &set2);

	dt_table_apply_all(table, &double_values, NULL);

	for (size_t c = 0; c < table->n_columns; ++c)
	{
		for (size_t r = 0; r < table->n_rows; ++r)
		{
			const int32_t* get = dt_table_get_value(table, r, c);
			if (*get != 20)
			{
				fprintf(stderr, "Expected value to be 20 but got %d.\n", *get);
				goto cleanup;
			}
		}
	}

	status = 0;
cleanup:
	dt_table_free(&table);
	return status;
}
