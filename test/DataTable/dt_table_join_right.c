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

	set2 = 12.52f;
	dt_table_insert_row(table, 2, NULL, &set2);

  set1 = 122;
	set2 = 21.21f;
	dt_table_insert_row(table, 2, &set1, &set2);

	// TABLE TWO
  char colnames2[2][DT_MAX_COL_LEN] = { "col1", "col3" };
  enum data_type_e types2[2] = { INT32, FLOAT };
	struct DataTable* table2 = dt_table_create(2, colnames2, types2);

	set1 = 55;
  set2 = 12.12f;
	dt_table_insert_row(table2, 2, &set1, &set2);

	set1 = 10;
  set2 = 99.35f;
	dt_table_insert_row(table2, 2, &set1, &set2);

	set1 = 20;
  set2 = 32.95f;
	dt_table_insert_row(table2, 2, &set1, &set2);

  char join_columns[1][DT_MAX_COL_LEN] = { "col1" };
  struct DataTable* right_join = dt_table_join_right(table, table2, 1, join_columns);

  if (right_join->n_rows != 3)
  {
    fprintf(stderr, "Expected joined table to have three rows.\n");
    goto cleanup;
  }

  if (right_join->n_columns != 4)
  {
    fprintf(stderr, "Expected joined table to have four columns.\n");
    goto cleanup;
  }

  /* ROW 0 */
  const int32_t* get1;
  if (!dt_table_check_isnull(right_join, 0, 0))
  {
    fprintf(stderr, "Expected right_join[0, 0] to be NULL.\n");
    goto cleanup;
  }

  const float* get2;
  if (!dt_table_check_isnull(right_join, 0, 1))
  {
    fprintf(stderr, "Expected right_join[0, 1] to be NULL.\n");
    goto cleanup;
  } 

  const int32_t* get3;
  if ((get3 = dt_table_get_value(right_join, 0, 2)) && *get3 != 55)
  {
    fprintf(stderr, "Incorrect value for right_join[0, 2].\n");
    goto cleanup;
  }

  const float* get4;
  if ((get4 = dt_table_get_value(right_join, 0, 3)) && fabsf(*get4 - 12.12f) > 0.0001f)
  {
    fprintf(stderr, "Incorrect value for right_join[1, 3].\n");
    goto cleanup;
  }

  /* ROW 1 */
  if ((get1 = dt_table_get_value(right_join, 1, 0)) && *get1 != 10)
  {
    fprintf(stderr, "Incorrect value for right_join[1, 0].\n");
    goto cleanup;
  }

  if ((get2 = dt_table_get_value(right_join, 1, 1)) && fabsf(*get2 - 5.5f) > 0.0001f)
  {
    fprintf(stderr, "Incorrect value for right_join[1, 1].\n");
    goto cleanup;
  }

  if ((get3 = dt_table_get_value(right_join, 1, 2)) && *get3 != 10)
  {
    fprintf(stderr, "Incorrect value for right_join[1, 2].\n");
    goto cleanup;
  }

  if ((get4 = dt_table_get_value(right_join, 1, 3)) && fabsf(*get4 - 99.35f) > 0.0001f)
  {
    fprintf(stderr, "Incorrect value for right_join[1, 3].\n");
    goto cleanup;
  }

  /* ROW 2 */
  if (!dt_table_check_isnull(right_join, 2, 0))
  {
    fprintf(stderr, "Expected right_join[2, 0] to be NULL.\n");
    goto cleanup;
  }

  if (!dt_table_check_isnull(right_join, 2, 1))
  {
    fprintf(stderr, "Expected right_join[2, 1] to be NULL.\n");
    goto cleanup;
  }

  if ((get3 = dt_table_get_value(right_join, 2, 2)) && *get3 != 20)
  {
    fprintf(stderr, "Incorrect value for right_join[2, 2].\n");
    goto cleanup;
  }

  if ((get4 = dt_table_get_value(right_join, 2, 3)) && fabsf(*get4 - 32.95f) > 0.0001f)
  {
    fprintf(stderr, "Incorrect value for right_join[2, 3].\n");
    goto cleanup;
  }

  status = 0;
cleanup:
  dt_table_free(&table);
  dt_table_free(&table2);
  dt_table_free(&right_join);
  return status;
}
