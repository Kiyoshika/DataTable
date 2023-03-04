#ifndef DATA_TABLE_H
#define DATA_TABLE_H

#include <stdlib.h>
#include <string.h>
#include <stddef.h>

// forward declaration
struct DataColumn;

struct DataTable
{
	struct DataColumn* column;
	size_t n_columns;
	size_t column_capacity;
};
#endif
