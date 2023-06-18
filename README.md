# DataTable
This is a work in progress library to build a data table/data set library similar to "Pandas" from Python but for C.

Eventually I want to wrap all of this code in a Python and perhaps Julia wrapper and make it available through pip or other package managers.

## Supported Datatypes
All of the basic types from `stdint.h` are supported (e.g., `uint8_t`, `int64_t`, etc.) as well as `char*`, `float` and `double`.

## Supported Features
* Reading/Writing from/to files
* Getting/Setting values
* Appending tables
* Filling values
* Selecting columns (similar to SQL)
* Dropping columns
* Filtering rows
* Applying function to values
* Handling NULL values
* Joining tables (similar to SQL)
* Casting datatypes
* Randomly Sampling rows
* Randomly splitting table

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
* [Appending Tables](#appending-tables)
* [Data Access](#data-access)
* [Fill Values](#fill-values)
* [Null Values](#null-values)
* [Selecting Columns](#selecting-columns)
* [Dropping Columns](#dropping-columns)
* [Distinct Rows](#distinct-rows)
* [Copying Tables](#copying-tables)
* [Filtering](#filtering)
* [Applying Function to Values](#applying-function-to-values)
* [Joining Tables](#joining-tables)
* [Casting Datatypes](#casting-datatypes)
* [Random Sample](#random-sample)
* [Random Split](#random-split)

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

### Appending Tables
You can append tables row-wise or column-wise. The table being appened to (referred to by "dest") is modified in-place.

Assume we have the following two tables created from somewhere:
```c
struct DataTable* dest = ...; // destination table
struct DataTable* src = ...; // source table
```

If appending row-wise, it will expect the number of columns as well as the column types to be the same.
```c
// append single table
if (dt_table_append_by_row(dest, src) != DT_SUCCESS)
{
  // handle error ...
}

// append multiple tables
if (dt_table_append_multiple_by_row(dest, 3, src1, src2, src3) != DT_SUCCESS)
{
  // handle error ...
}

```

If appending column-wise, it will expect the number of rows to be the same.
```c
// append single table
if (dt_table_append_by_column(dest, src) != DT_SUCCESS)
{
  // handle error ...
}

// append multiple tables
if (dt_table_append_multiple_by_column(dest, 3, src1, src2, src3) != DT_SUCCESS)
{
  // handle error ...
}
```

### Data Access
When accessing a value, a pointer to constant data is returned. It's advised to NOT work around this and modify data via pointers. This can case bugs with NULL values. It's recommended to use the dedicated setters instead.

Getting data:
```c
// assume this has a float and int32_t column
struct DataTable* table = ...;
const float* value1 = dt_table_get_value(table, row, column);
const int32_t* value2 = dt_table_get_value(table, row2, column2);
```

Updating data:
```c
// assume this has a float and int32_t column
struct DataTable* table = ...;
const float* new_value1 = 3.14159f;
const int32_t* new_value2 = 22;
dt_table_set_value(table, row, column, &new_value1);
dt_table_set_value(table, row2, column2, &new_value2);

// you can also set a value to be null
dt_table_set_value(table, row3, column3, NULL);
```

When using `set`, any heap-allocated data (i.e., string) will be deallocated before being overwritten. If that value was previously `NULL`, that null flag will be cleared (or vice versa, the NULL flag will be set if passing NULL).

### Fill Values
You can fill an entire column by name or index. Alternatively you can fill an entire table.

Specific Column:
```c
// assume the column we want to fill is a uint8_t type
uint8_t value = 12;

// by name
dt_table_fill_column_values_by_name(table, "col1", &value);

// by index
dt_table_fill_column_values_by_index(table, 0, &value);

// if entire table is a uint8_t type
dt_table_fill_all_values(table, &value);
```

### Null Values
You can check if a value is null with `dt_table_check_isnull()`:
```c
if (dt_table_check_isnull(table, row, column))
{
  // this value is null
}
```

Alternately you can check if an entire row or column contains at least one null value.
```c
if (dt_table_row_contains_null(table, row))
{
  // this row has at least one null value
}

if (dt_table_column_contains_null(table, column))
{
  // this column has at least one null value
}
```

You can also drop any rows or columns that contain at least one null value.
```c
dt_table_drop_columns_with_null(table);
dt_table_drop_rows_with_null(table);
```

### Selecting Columns
Similar to how SQL can select columns, we can do the same here.

When selecting columns, a copy is made of the subset. DO NOT reassign or you will case a memory leak.
```c
// assume this has col1, col2, col3
struct DataTable* main = ...;

const char select_names[2][DT_MAX_COL_LEN] = { "col1", "col3" };
struct DataTable* subset = dt_table_select(main, 2, select_names);
if (!subset)
{
  // handle error ...
}
```

In some cases you may want to replace all null values with a different value:
```c
// assume our column/table is uint8_t values
uint8_t new_value = 12;

// specify column by name
dt_table_replace_column_null_values_by_name(table, "col1", &new_value);

// specify column by index
dt_table_replace_column_null_values_by_index(table, 0, &new_value);

// replace every null value in table
dt_table_replace_all_null_values(table, &new_value);
```

### Dropping Columns
If columns are not needed, they can be dropped. Note that the dropping happens in-place.

You can drop columns either by index or by name.

```c
// drop by name
const char drop_names[2][DT_MAX_COL_LEN] = { "col1", "col3" };
dt_table_drop_columns_by_name(table, 2, drop_names);

// drop by index
const size_t drop_idx[2] = { 0, 2 };
dt_table_drop_columns_by_index(table, 2, drop_idx);
```

### Distinct Rows
This will modify the table in-place to get distinct rows. Under the hood a hashmap is created to quickly determine if a row exists or not.

```c
dt_table_distinct(table);
```

### Copying Tables
There are two main ways of copying a table. A full (deep) copy which will copy all of the contents of a table or a "skeleton" copy which ONLY copies the column names and types but does not copy any rows.

```c
struct DataTable* main = ...;

// rows will match
struct DataTable* full_copy = dt_table_copy(main); 

// rows = 0 but column names & types are the same
struct DataTable* skeleton = dt_table_copy_skeleton(main); 
```

### Filtering
You can filter single or multiple columns by name or index. When filtering multiple columns, there are built-in `AND` and `OR` predicates which will evaluate if ALL columns match the predicate (`AND`) or if AT LEAST ONE column matches the predicate (`OR`)

If filtering by multiple columns, you pass an array of predicates equal to the number of columns you're filtering. Each column will be compared to the corresponding predicate.

The filter callbacks also allow the user to pass in custom data that they can either read from or write to. The callback is called on every row for the specified column(s).

A new table will be returned from the filter. If no rows match the predicate, a table with 0 rows will be returned. The filter will only return NULL if there's some internal failure (e.g., out of memory.)

Filter single columns:
```c
// assume our table is dealing with uint8_t data

bool
is_even(
  void* current_row_value, 
  void* user_data)
{
  // check if the current row value + our value is even
  return *(uint8_t*)current_row_value + *(uint8_t*)user_data % 2 == 0;
}

uint8_t user_data = 11;

// filter by column name
struct DataTable* filtered 
  = dt_table_filter_by_name(table, "col1", &is_even, &user_data);

// filter by column index
struct DataTable* filtered 
  = dt_table_filter_by_index(table, 0, &is_even, &user_data);
```

Filter multiple columns where ALL columns must match the predicates.

In the below example, this will return every row where `col1` AND `col3`'s row value + 12 is even.
```c
bool
is_even(
  void* current_row_value, 
  void* user_data)
{
  // check if the current row value + our value is even
  return *(uint8_t*)current_row_value + *(uint8_t*)user_data % 2 == 0;
}

// in this case we are just going to use the same predicate for each
// column but you can specify any different predicates

uint8_t user_data = 12;

bool (*predicates[2])(void*, void*) = { &is_even, &is_even };

// filter by name
const char filter_names[2][DT_MAX_COL_LEN] = { "col1", "col3" };
struct DataTable* filtered
  = dt_table_filter_AND_by_name(table, 2, filter_names, predicates, &user_data);

// filter by index
const size_t filter_idx[2] = { 0, 2 };
struct DataTable* filtered
  = dt_table_filter_AND_by_name(table, 2, filter_idx, predicates, &uer_data);
```

Filter multiple columns where AT LEAST ONE column must match predicate:

In the below example, this will return every row where `col1` OR `col3`'s row value + 12 is even.
```c
bool
is_even(
  void* current_row_value, 
  void* user_data)
{
  // check if the current row value + our value is even
  return *(uint8_t*)current_row_value + *(uint8_t*)user_data % 2 == 0;
}

// in this case we are just going to use the same predicate for each
// column but you can specify any different predicates

uint8_t user_data = 12;

bool (*predicates[2])(void*, void*) = { &is_even, &is_even };

// filter by name
const char filter_names[2][DT_MAX_COL_LEN] = { "col1", "col3" };
struct DataTable* filtered
  = dt_table_filter_OR_by_name(table, 2, filter_names, predicates, &user_data);

// filter by index
const size_t filter_idx[2] = { 0, 2 };
struct DataTable* filtered
  = dt_table_filter_OR_by_name(table, 2, filter_idx, predicates, &uer_data);
```

### Applying Function to Values
Users can also apply a callback function to every row within a column (or an entire table).

One caveat is that, currently, this DOES NOT work if the column contains a NULL value. To workaround this, you can do a replace or fill to remove null values.

Within the callback function, we can also reference other columns we care about.

In the below example, assume we have col1 and col2 with `uint8_t` values and a col3 that is zero. Let's set col3 to be the sum of col1 and col2:

```c
void
sum_col1_col2(
  void* current_row_value,
  void* user_data,
  const void** const column_values)
{
  // not using user_data in this callback
  (void)user_data;

  // current value of col3
  uint8_t* col3 = current_row_value;

  // current value of col1
  const uint8_t* const col1 = column_values[0];
  
  // current value of col2
  const uint8_t* const col2 = column_values[1];

  // set col3 = col1 + col2
  *col3 = *col1 + *col2;
}

// these are the columns to pass into the callback, 
// mapped to column_values[0], column_values[1], etc. for however many
const char fetch_columns[2][DT_MAX_COL_LEN] = { "col1", "col2" };

// the NULL is the user_data parameter; not using user_data.
// the 2 is the # of fetch_columns.
//
// if not referencing any columns within the table, you can pass 0, NULL
// to the last two parameters
dt_table_apply_column(table, "col3", &sum_col1_col2, NULL, 2, fetch_columns);
```

If applying a callback to ALL cells within a table, there is only the callback and user data parameters.

In the below example, let's say we want to add every cell in our table with some custom value passed by user data.
```c

// assume our entire table contains uint8_t values

void
double_values(
  void* current_cell_value,
  void* user_data)
{
  uint8_t* cell = cell_value;
  uint8_t* data = user_data;

  *cell += *data;
}

// every cell in the table will be added by 5
uint8_t user_data = 5;
dt_table_apply_all(table, &double_values, &user_data);
```

### Joining Tables
We can perform SQL-like joins. We have left, right, inner and full joins supported. They behave just like SQL joins so I will not explain that in detail (if you're unfamiliar with SQL joins, please read on them separately.)

When joining tables, we provide a "left" and a "right" table. This is analogous to:
```sql
select * from left_table
join right_table
on ...
```
A new table is returned as the result of the join. If no matches are found, a table with 0 rows is returned. NULL is returned if there's an internal error (e.g., out of memory.) The new table's columns are currently naively appended together (causing duplicate column names). E.g., if `left_table` has `{col1, col2, col3}` and `right_table` has `{col1,col5}` and we join on `col1`, the resulting table will have `{col1, col2, col3, col1, col5}`. Ideally this behaviour will change in a future version so we don't waste memory.

Note that in our version of joins, NULL values are matched (i.e., if `left_table.col1 = right_table.col1 = NULL` this evaluates to true). I believe this is similar to how PostgreSQL functions.

Currently we only support equality joins. Although other logicals may be implemented in the future.

```c
struct DataTable* left_table = ...;
struct DataTable* right_table = ...;

// specify the columns to join on
const char join_columns[2][DT_MAX_COL_LEN] = { "col1", "col3" };

struct DataTable* left_join
  = dt_table_join_left(left_table, right_table, 2, join_columns);

struct DataTable* right_join
  = dt_table_join_right(left_table, right_table, 2, join_columns);

struct DataTable* full_join
  = dt_table_join_full(left_table, right_table, 2, join_columns);

struct DataTable* inner_join
  = dt_table_join_inner(left_table, right_table, 2, join_columns);
```

### Casting Datatypes
We can cast a column's datatype to another type. Let's say we have a `double` column and want to convert it to a `string` column.

```c
// specify column names and new types to setup casting
// in our case we're only doing 1, but you can cast as many as desired
const char column[1][DT_MAX_COL_LEN] = { "col1" };
const enum data_type_e new_type[1] = { STRING };

// internally, col1 (which originally was a DOUBLE) will be converted to a string
// and the column type will be updated
dt_table_cast_columns(table, 1, column, new_type);
```

### Random Sample
We can randomly sample rows with or without replacement.

If NOT sampling with replacement, then `n_samples` must be less than or equal to `n_rows`.

Returns a newly-allocated table with the sampled rows.
```c
// assume our table has 250 rows

// sample without replacement.
// in this case, we must have at least 100 rows
dt_table_sample_rows(table, 100, false);

// sample with replacement.
// in this case, the number of samples can be arbitrarily large
dt_table_sample_rows(table, 50000, true);
```

### Random Split
We can randomly split a table according to a proportion, mostly useful in machine learning applications.
```c
struct DataTable* main = ...;
struct DataTable* split1 = NULL;
struct DataTable* split2 = NULL;

// split1 will contain 75% of the original rows from main
// split2 will contain remaining 25% of original rows from main
dt_table_split(main, 0.75f, &split1, &split2);
```
