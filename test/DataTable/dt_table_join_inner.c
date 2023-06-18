#include "DataTable.h"
#include "HashTable.h"
#include <stdio.h>
#include <math.h>

int main()
{
  int status = -1;

  // TABLE ONE
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

	// TABLE TWO
  char colnames2[1][DT_MAX_COL_LEN] = { "col1" };
  enum data_type_e types2[1] = { INT32 };
	struct DataTable* table2 = dt_table_create(1, colnames2, types2);

	set1 = 55;
	dt_table_insert_row(table2, 1, &set1);

	set1 = 10;
	dt_table_insert_row(table2, 1, &set1);

	set1 = 20;
	dt_table_insert_row(table2, 1, &set1);

  char join_columns[1][DT_MAX_COL_LEN] = { "col1" };
  struct DataTable* inner_join = dt_table_join_inner(table, table2, 1, join_columns);

  if (inner_join->n_rows != 2)
  {
    fprintf(stderr, "Expected joined table to have two rows.\n");
    goto cleanup;
  }

  if (inner_join->n_columns != 3)
  {
    fprintf(stderr, "Expected joined table to have three columns.\n");
    goto cleanup;
  }

  const int32_t* get;
  if ((get = dt_table_get_value(inner_join, 0, 0)) && *get != 10)
  {
    fprintf(stderr, "Incorrect value for inner_join[0, 0].\n");
    goto cleanup;
  }

  if ((get = dt_table_get_value(inner_join, 1, 0)) && *get != 20)
  {
    fprintf(stderr, "Incorrect value for inner_join[1, 0].\n");
    goto cleanup;
  }

  const float* get2;
  if ((get2 = dt_table_get_value(inner_join, 0, 1)) && fabsf(*get2 - 5.5f) > 0.0001f)
  {
    fprintf(stderr, "Incorrect value for inner_join[0, 1].\n");
    goto cleanup;
  }

  if ((get2 = dt_table_get_value(inner_join, 1, 1)) && fabsf(*get2 - 12.52f) > 0.0001f)
  {
    fprintf(stderr, "Incorrect value for inner_join[1, 1].\n");
    goto cleanup;
  }

  status = 0;
cleanup:
  dt_table_free(&table);
  dt_table_free(&table2);
  dt_table_free(&inner_join);
  return status;
}
