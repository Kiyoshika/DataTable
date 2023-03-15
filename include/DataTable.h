#ifndef DATA_TABLE_H
#define DATA_TABLE_H

#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include <stdarg.h>

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
	size_t n_rows;
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

// insert a row by passing n_column arguments by pointer (be sure you are using correct types).
// returns DT_ALLOC_ERROR if there was a problem resizing, DT_SUCCESS otherwise.
enum status_code_e
dt_table_insert_row(
	struct DataTable* const table,
	size_t n_columns,
	...);

#endif
