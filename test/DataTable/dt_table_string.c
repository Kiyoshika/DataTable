#include "DataTable.h"
#include <stdio.h>

int main()
{
  int status = -1;

  const char names[1][DT_MAX_COL_LEN] = { "col1" };
  const enum data_type_e types[1] = { STRING };
  struct DataTable* table = dt_table_create(1, names, types);

  dt_table_insert_row(table, 1, "hello");

  const char** get = (const char**)dt_table_get_value(table, 0, 0);
  if (strcmp(*get, "hello") != 0)
  {
    fprintf(stderr, "Incorrect value for table[0, 0].\n");
    goto cleanup;
  }

  dt_table_insert_row(table, 1, "chickens");

  get = (const char**)dt_table_get_value(table, 1, 0);
  if (strcmp(*get, "chickens") != 0)
  {
    fprintf(stderr, "Incorrect value for table[1, 0].\n");
    goto cleanup;
  }

  dt_table_insert_row(table, 1, "some other really long value");
  get = (const char**)dt_table_get_value(table, 2, 0);
  if (strcmp(*get, "some other really long value") != 0)
  {
    fprintf(stderr, "Incorrect value for table[2, 0].\n");
    goto cleanup;
  }

  status = 0;
cleanup:
  dt_table_free(&table);
  return status;
}
