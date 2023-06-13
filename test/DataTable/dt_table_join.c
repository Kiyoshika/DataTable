#include "DataTable.h"
#include "HashTable.h"
#include <stdio.h>

int main()
{
  int status = -1;

  // TABLE ONE
	char colnames[2][MAX_COL_LEN] = { "col1", "col2" };
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
  char colnames2[1][MAX_COL_LEN] = { "col1" };
  enum data_type_e types2[1] = { INT32 };
	struct DataTable* table2 = dt_table_create(1, colnames2, types2);

	set1 = 55;
	dt_table_insert_row(table2, 1, &set1);

	set1 = 10;
	dt_table_insert_row(table2, 1, &set1);

	set1 = 20;
	dt_table_insert_row(table2, 1, &set1);

  char join_columns[1][MAX_COL_LEN] = { "col1" };
  dt_table_join(table, table2, join_columns, 1);

  status = 0;
cleanup:
  dt_table_free(&table);
  return status;
}
