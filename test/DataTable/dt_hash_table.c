#include "DataTable.h"
#include "HashTable.h"
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

	struct HashTable* htable = hash_create(table);

	struct DataTable* table2 = dt_table_copy_skeleton(table);

	set1 = 55;
	set2 = 10.10f;
	dt_table_insert_row(table2, 2, &set1, &set2);

	set1 = 20;
	set2 = 12.52f;
	dt_table_insert_row(table2, 2, &set1, &set2);

	// THIS ROW SHOULD NOT BE FOUND IN HASHTABLE
	

	// THIS ROW SHOULD BE FOUND
	

	status = 0;
cleanup:
	dt_table_free(&table);
	dt_table_free(&table2);
	hash_free(&htable);

	return status;
}
