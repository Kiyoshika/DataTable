#ifndef HASH_TABLE_H
#define HASH_TABLE_H

#include <stddef.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include "StatusCodes.h"

// forward declarations
struct DataTable;

/* this is a statically-sized hash table that stores size_t values */

struct Bin
{
	size_t* value;
	size_t n_values;
	size_t capacity;
};

struct HashTable
{
	// hold a (shared) pointer to table for reference
	// this does NOT get free'd
	struct DataTable* table;

	bool is_empty;

	struct Bin* bin;
	size_t n_bins;

  // the column indices to apply the hash function on for the table
  size_t* column_indices;
  size_t n_column_indices;
};

// create new statically-sized hash table (does NOT resize).
//
// [insert_all_rows] determines whether or not to auto insert all rows or have the user
// insert rows manually (they serve different use-cases).
//
// [column_indices] is a heap-allocated array of column indices to apply
// the hash function on. If NULL is provided, it defaults to all columns
// (and is allocated internally). Note that if you provide the heap-allocated
// array, the ownership is transferred and you SHOULD NOT free this yourself.
//
// returns NULL on failure (e.g., couldn't allocate enough memory)
struct HashTable*
hash_create(
	const struct DataTable* const table,
	const bool insert_all_rows,
  size_t* column_indices,
  size_t n_column_indices);

// insert a row from the internal table (htable->table).
// returns DT_INDEX_ERROR if row_idx is out of bounds
// returns DT_ALLOC_ERROR if there's a problem allocating memory
// returns DT_SUCCESS otherwise
enum status_code_e
hash_insert(
	struct HashTable* const htable,
	const size_t row_idx);

// check if hashtable contains a particular row passed from another table.
//
// [table_column_indices] must match the size of htable->n_column_indices. This is
// used to only compare (with dt_table_rows_equal) a subset of columns (or all columns)
// which is used particularly for computing joins between tables (but may have other use cases).
//
// returns true if row is found and all (specified) columns are equal.
// returns false otherwise.
bool
hash_contains(
	const struct HashTable* const htable,
	const struct DataTable* const table,
  const size_t* const table_column_indices,
	const size_t row_idx);

void
hash_free(
	struct HashTable** table);	

#endif
