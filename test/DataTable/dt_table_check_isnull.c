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
	dt_table_insert_row(table, 2, &set1, NULL);

	set1 = 20;
	set2 = 21.21f;
	dt_table_insert_row(table, 2, &set1, &set2);

  if (!dt_table_check_isnull(table, 1, 1))
  {
    fprintf(stderr, "Expected value to be NULL.\n");
    goto cleanup;
  }

  dt_table_set_value(table, 1, 1, &set2);

  if (dt_table_check_isnull(table, 1, 1))
  {
    fprintf(stderr, "Expected value to NOT be NULL.\n");
    goto cleanup;
  }

	status = 0;
cleanup:
	dt_table_free(&table);
	return status;
}
