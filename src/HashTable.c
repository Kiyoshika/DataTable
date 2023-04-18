#include "HashTable.h"
#include "DataTable.h"
#include <stdio.h>

#define generic_add(hash_value_ptr, value_ptr, type) \
	*hash_value_ptr += *(type*)value_ptr;

static size_t 
hash_function(
	const struct HashTable* const htable,	
	const struct DataTable* const table,
	const size_t row_idx)
{
	size_t hash_value = 0;

	for (size_t i = 0; i < table->n_columns; ++i)
	{
		const void* value = dt_table_get_value(table, row_idx, i);

		switch (table->columns[i].column->type)
		{
			case UINT8:
				generic_add(&hash_value, value, uint8_t);
				break;
			case UINT16:
				generic_add(&hash_value, value, uint16_t);
				break;
			case UINT32:
				generic_add(&hash_value, value, uint32_t);
				break;	
			case UINT64:
				generic_add(&hash_value, value, uint64_t);
				break;
			case INT8:
				generic_add(&hash_value, value, int8_t);
				break;
			case INT16:
				generic_add(&hash_value, value, int16_t);
				break;
			case INT32:
				generic_add(&hash_value, value, int32_t);
				break;
			case INT64:
				generic_add(&hash_value, value, int64_t);
				break;
			case DOUBLE:	
			{
				// would be better to use both sides of the decimal but
				// for now in this naive case i'll just downcast to an int64
				int64_t _value = (int64_t)*(double*)value;
				generic_add(&hash_value, &_value, int64_t);
			}
			break;
			case FLOAT:
			{
				// would be better to use both sides of the decimal but
				// for now in this naive case i'll just downcast to an int64
				int64_t _value = (int64_t)*(float*)value;
				generic_add(&hash_value, &_value, int64_t);
			}	
			break;
			case STRING:
			{
				char* value_str = *(char**)value;
				size_t len = strlen(value_str);

				for (size_t c = 0; c < len; ++c)
					generic_add(&hash_value, &value_str[c], char);
				break;
			}	
		}
	}

	hash_value = (hash_value ^ (hash_value >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
	hash_value = (hash_value ^ (hash_value >> 27)) * UINT64_C(0x94d049bb133111eb);
	hash_value = hash_value ^ (hash_value >> 31);

	return hash_value % htable->table->n_rows;
}

enum status_code_e
hash_insert(
	struct HashTable* const htable,
	const size_t row_idx)
{
	if (row_idx >= htable->table->n_rows)
		return DT_INDEX_ERROR;

	size_t hash_idx = hash_function(htable, htable->table, row_idx);
	struct Bin* bin = &htable->bin[hash_idx];
	bin->value[bin->n_values++] = row_idx;
	if (bin->n_values == bin->capacity)
	{
		void* alloc = realloc(bin->value, (bin->capacity + 1) * sizeof(size_t));
		if (!alloc)
			return DT_ALLOC_ERROR;
		bin->value = alloc;
		bin->capacity++;
		memset(&bin->value[bin->n_values], 0, sizeof(size_t));
	}
	return DT_SUCCESS;
}

struct HashTable*
hash_create(
	const struct DataTable* const table,
	const bool insert_all_rows)
{
	struct HashTable* htable = malloc(sizeof(*htable));
	if (!htable)
		return NULL;

	// set a shared pointer (this does NOT get free'd)
	// only used as a reference when adding items to bins etc.
	htable->table = table;

	htable->bin = calloc(table->n_rows, sizeof(*htable->bin));
	htable->n_bins = table->n_rows;

	if (!htable->bin)
	{
		free(htable);
		return NULL;
	}

	// by default, assign one slot per bin (will reallocate if necessary on collision) 
	for (size_t i = 0; i < table->n_rows; ++i)
	{
		htable->bin[i].value = calloc(1, sizeof(size_t));
		htable->bin[i].n_values = 0;
		htable->bin[i].capacity = 1;

		// cleanup all memory allocated so far if one of them fails
		if (!htable->bin[i].value)
		{
			for (size_t k = 0; k < i; ++k)
				free(htable->bin[i].value);
			free(htable->bin);
			free(htable);
			return NULL;
		}
	}

	if (insert_all_rows)
	{
		for (size_t r = 0; r < table->n_rows; ++r)
		{
			if (hash_insert(htable, r) != DT_SUCCESS)
			{
				hash_free(&htable);
				return NULL;
			}
		}
	}

	return htable;
}	

bool
hash_contains(
	const struct HashTable* const htable,
	const struct DataTable* const table,
	const size_t row_idx)
{
	size_t hash_idx = hash_function(htable, table, row_idx);
	struct Bin* bin = &htable->bin[hash_idx];
	
	for (size_t i = 0; i < bin->n_values; ++i)
		if (dt_table_rows_equal(table, row_idx, htable->table, bin->value[i]))
			return true;

	return false;
}	
		

void
hash_free(
	struct HashTable** htable)
{
	for (size_t i = 0; i < (*htable)->n_bins; ++i)
	{
		free((*htable)->bin[i].value);
		(*htable)->bin[i].value = NULL;
	}	

	free((*htable)->bin);
	(*htable)->bin = NULL;

	free(*htable);
	*htable = NULL;
}	
