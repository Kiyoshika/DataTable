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

	int32_t set1 = 33;
	float set2 = 5.5f;
	dt_table_insert_row(table, 2, &set1, &set2);

	set1 = 20;
	set2 = 12.52f;
	dt_table_insert_row(table, 2, &set1, &set2);

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
  struct DataTable* full_join = dt_table_join_full(table, table2, 1, join_columns);

  if (full_join->n_rows != 6)
  {
    fprintf(stderr, "Expected joined table to have six rows.\n");
    goto cleanup;
  }

  if (full_join->n_columns != 4)
  {
    fprintf(stderr, "Expected joined table to have four columns.\n");
    goto cleanup;
  }

  /* ROW 0 */
  const int32_t* get1;
  if ((get1 = dt_table_get_value(full_join, 0, 0)) && *get1 != 33)
  {
    fprintf(stderr, "Incorrect value for full_join[0, 0].\n");
    goto cleanup;
  }

  const float* get2;
  if ((get2 = dt_table_get_value(full_join, 0, 1)) && fabsf(*get2 - 5.5f) > 0.0001f)
  {
    fprintf(stderr, "Incorrect value for full_join[0, 1].\n");
    goto cleanup;
  }

  const int32_t* get3;
  if ((get3 = dt_table_get_value(full_join, 0, 2)) && *get3 != 0) // "NULL"
  {
    fprintf(stderr, "Incorrect value for full_join[0, 2].\n");
    goto cleanup;
  }

  const float* get4;
  if ((get4 = dt_table_get_value(full_join, 0, 3)) && fabsf(*get4 - 0.00f) > 0.0001f) // "NULL"
  {
    fprintf(stderr, "Incorrect value for full_join[1, 3].\n");
    goto cleanup;
  }

  /* ROW 1 */
  if ((get1 = dt_table_get_value(full_join, 1, 0)) && *get1 != 20)
  {
    fprintf(stderr, "Incorrect value for full_join[1, 0].\n");
    goto cleanup;
  }

  if ((get2 = dt_table_get_value(full_join, 1, 1)) && fabsf(*get2 - 12.52f) > 0.0001f)
  {
    fprintf(stderr, "Incorrect value for full_join[1, 1].\n");
    goto cleanup;
  }

  if ((get3 = dt_table_get_value(full_join, 1, 2)) && *get3 != 20)
  {
    fprintf(stderr, "Incorrect value for full_join[1, 2].\n");
    goto cleanup;
  }

  if ((get4 = dt_table_get_value(full_join, 1, 3)) && fabsf(*get4 - 32.95f) > 0.0001f)
  {
    fprintf(stderr, "Incorrect value for full_join[1, 3].\n");
    goto cleanup;
  }

  /* ROW 2 */
  if ((get1 = dt_table_get_value(full_join, 2, 0)) && *get1 != 122)
  {
    fprintf(stderr, "Incorrect value for full_join[2, 0].\n");
    goto cleanup;
  }

  if ((get2 = dt_table_get_value(full_join, 2, 1)) && fabsf(*get2 - 21.21f) > 0.0001f)
  {
    fprintf(stderr, "Incorrect value for full_join[2, 1].\n");
    goto cleanup;
  }

  if ((get3 = dt_table_get_value(full_join, 2, 2)) && *get3 != 0) // "NULL"
  {
    fprintf(stderr, "Incorrect value for full_join[2, 2].\n");
    goto cleanup;
  }

  if ((get4 = dt_table_get_value(full_join, 2, 3)) && fabsf(*get4 - 0.00f) > 0.0001f) // "NULL"
  {
    fprintf(stderr, "Incorrect value for full_join[2, 3].\n");
    goto cleanup;
  }

  /* ROW 3 */
  if ((get1 = dt_table_get_value(full_join, 3, 0)) && *get1 != 0) // "NULL"
  {
    fprintf(stderr, "incorrect value for full_join[3, 0].\n");
    goto cleanup;
  }

  if ((get2 = dt_table_get_value(full_join, 3, 1)) && fabsf(*get2 - 0.00f) > 0.0001f) // "NULL"
  {
    fprintf(stderr, "incorrect value for full_join[3, 1].\n");
    goto cleanup;
  }

  if ((get3 = dt_table_get_value(full_join, 3, 2)) && *get3 != 55)
  {
    fprintf(stderr, "incorrect value for full_join[3, 2].\n");
    goto cleanup;
  }

  if ((get4 = dt_table_get_value(full_join, 3, 3)) && fabsf(*get4 - 12.12f) > 0.0001f)
  {
    fprintf(stderr, "incorrect value for full_join[3, 3].\n");
    goto cleanup;
  }

  /* ROW 4 */
  if ((get1 = dt_table_get_value(full_join, 4, 0)) && *get1 != 0) // "NULL" 
  {
    fprintf(stderr, "incorrect value for full_join[4, 0].\n");
    goto cleanup;
  }

  if ((get2 = dt_table_get_value(full_join, 4, 1)) && fabsf(*get2 - 0.00f) > 0.0001f) // "NULL"
  {
    fprintf(stderr, "incorrect value for full_join[4, 1].\n");
    goto cleanup;
  }

  if ((get3 = dt_table_get_value(full_join, 4, 2)) && *get3 != 10)
  {
    fprintf(stderr, "incorrect value for full_join[4, 2].\n");
    goto cleanup;
  }

  if ((get4 = dt_table_get_value(full_join, 4, 3)) && fabsf(*get4 - 99.35f) > 0.0001f)
  {
    fprintf(stderr, "incorrect value for full_join[4, 3].\n");
    goto cleanup;
  }

  /* ROW 5 */
  if ((get1 = dt_table_get_value(full_join, 5, 0)) && *get1 != 20)
  {
    fprintf(stderr, "incorrect value for full_join[5, 0].\n");
    goto cleanup;
  }

  if ((get2 = dt_table_get_value(full_join, 5, 1)) && fabsf(*get2 - 0.00f) > 12.52f)
  {
    fprintf(stderr, "incorrect value for full_join[5, 1].\n");
    goto cleanup;
  }

  if ((get3 = dt_table_get_value(full_join, 5, 2)) && *get3 != 20)
  {
    fprintf(stderr, "incorrect value for full_join[5, 2].\n");
    goto cleanup;
  }

  if ((get4 = dt_table_get_value(full_join, 5, 3)) && fabsf(*get4 - 32.95f) > 0.0001f)
  {
    fprintf(stderr, "incorrect value for full_join[5, 3].\n");
    goto cleanup;
  }

  status = 0;
cleanup:
  dt_table_free(&table);
  dt_table_free(&table2);
  dt_table_free(&full_join);
  return status;
}
