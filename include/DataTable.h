#ifndef DATA_TABLE_H
#define DATA_TABLE_H

#include <stdlib.h>
#include <string.h>
#include <stddef.h>

#include "DataColumn.h"
#include "StatusCodes.h"

#define MAX_COL_LEN 101

struct ColumnPair
{
	char name[MAX_COL_LEN];
	struct DataColumn* column;
};

struct DataTable
{
	struct ColumnPair* columns;
	size_t n_columns;
	size_t column_capacity;
};

// create a new empty table with n_columns by passing and array of
// column names and data types.
// returns NULL on failure (e.g., couldn't allocate enough memory)
struct DataTable*
dt_table_create(
	const size_t n_columns,
	const char (*column_names)[MAX_COL_LEN],
	const enum data_type_e* data_types);

// free memory allocated by table (also frees the underlying columns)
void
dt_table_free(
	struct DataTable** const table);

#endif
