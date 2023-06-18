# DataTable
This is a work in progress library to build a data table/data set library similar to "Pandas" from Python but for C.

Eventually I want to wrap all of this code in a Python and perhaps Julia wrapper and make it available through pip or other package managers.

## Supported Datatypes
All of the basic types from `stdint.h` are supported (e.g., `uint8_t`, `int64_t`, etc.) as well as `char*`, `float` and `double`.

## Supported Features
* Reading/Writing from/to files
* Getting/Setting values
* Selecting columns (similar to SQL)
* Dropping columns
* Filtering rows
* Handling NULL values
* Joining tables (similar to SQL)
* Casting datatypes

## Components
### DataTable
This is a data structure composed of `DataColumn`s (see below). All the columns are allocated contiguously and are tied with a `char*` for its column name. We can select, filter, create, manipulate, etc. columns within the table.
### DataColumn
This is a data structure that represents a single column. All of its members are allocated contiguously and supports all the typical types (`int32_t`, `uint8_t`, `char*`, etc.) and also custom user types (so we can have tables of more complex structures if required).

## Building from Source
TODO: rewrite this

## Quick Start Guide
There are a lot of functions provided, but the [header](include/DataTable.h) provides a lot of comments around usage. Here we just quickly demonstrate some useful features.

Include the header to use the library (after installing)
```c
#include <datatable/DataTable.h>
```

## Contents
* [Data Type Enum](#data-type-enum)
* [Reading/Writing to File](#reading-and-writing-to-file)
* [Creating Table Manually](#creating-table-manually)
* [Data Access](#data-access)
* [Null Values](#null-values)
* [Selecting Columns](#selecting-columns)
* [Dropping Columns](#dropping-columns)
* [Distinct Rows](#distinct-rows)
* [Copying Tables](#copying-tables)
* [Filtering](#filtering)
* [Joining Tables](#joining-tables)
* [Casting Datatypes](#casting-datatypes)

### Data Type Enum
In some cases you may want/need to specify specific types (e.g., when reading a csv). There is a `enum data_type_e` that defines all the data types with the following values:
* `UINT8`
* `UINT16`
* `UINT32`
* `UINT64`
* `INT8`
* `INT16`
* `INT32`
* `INT64`
* `FLOAT`
* `DOUBLE`
* `STRING`

I won't bother listing what they map to since it should be obvious.

### Reading and Writing to File
When reading from a file, the user can either specify the column types manually or have the library perform naive inference. The naive inference will assume `int64_t` for all "int-like" values, `double` for all decimal values and `char*` otherwise.

Reading from a file:
```c
// NULL to perform type inference
struct DataTable* table = dt_table_read_csv("my_table.csv", ',', NULL);
if (!table)
{
  // handle error ...
}

// otherwise, we can specify the types
enum data_type_e types[3] = { UINT8, INT16, INT16 };
struct DataTable* table2 = dt_table_read_csv("my_table.csv", ',', types);
if (!table2)
{
  // handle error ...
}

// assuming neither of these had an error
dt_table_free(&table);
dt_table_free(&table2);
```

Writing to a file:
```c
struct DataTable* table = ...;
if (!dt_table_to_csv(table, "output.csv", ','))
{
  // handle error ...
}

dt_table_free(&table);
```

### Creating Table Manually
If you don't want/need to read from a file, you can also create a table manually, although it's a bit more annoying.

```c
// specify the # of columns, column names and column types
// NOTE: use DT_MAX_COL_LEN (included in the header) as the size of each column
const char names[2][DT_MAX_COL_LEN] = { "col1", "col2", "col3" };
const enum data_type_e types[2] = { INT8, FLOAT, STRING };
struct DataTable* table = dt_table_create(2, names, types);
if (!table)
{
  // handle error ...
}

// you can now insert rows by passing pointers.
// this uses dynamic arg length, so specify the # of columns.
// note, for strings, addresses should NOT be passed; a heap-copy is made for the string
// and the address is stored internally
int8_t value1 = 1;
float value2 = 3.0f;
dt_table_insert_row(table, 3, &value1, &value2, "hello");

value1 = 12;
value2 = 64.23f;
dt_table_insert_row(table, 3, &value1, &value2, "chickens");

// you can also insert nulls by passing NULL
dt_table_insert_row(table, 3, NULL, NULL, "some long string value");

dt_table_free(&table);
```

### Data Access

### Null Values

### Selecting Columns

### Dropping Columns

### Distinct Rows

### Copying Tables

### Filtering

### Joining Tables

### Casting Datatypes
