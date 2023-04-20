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
};

// create new statically-sized hash table (does NOT resize).
// the boolean determines whether or not to auto insert all rows or have the user
// insert rows manually (they serve different use-cases).
// returns NULL on failure (e.g., couldn't allocate enough memory)
struct HashTable*
hash_create(
	const struct DataTable* const table,
	const bool insert_all_rows);

// insert a row from the internal table (htable->table).
// returns DT_INDEX_ERROR if row_idx is out of bounds
// returns DT_ALLOC_ERROR if there's a problem allocating memory
// returns DT_SUCCESS otherwise
enum status_code_e
hash_insert(
	struct HashTable* const htable,
	const size_t row_idx);

// check if hashtable contains a particular row passed from another table.
// returns true if row is found and all columns are equal.
// returns false otherwise.
bool
hash_contains(
	const struct HashTable* const htable,
	const struct DataTable* const table,
	const size_t row_idx);

void
hash_free(
	struct HashTable** table);	

#endif
