#include <stdio.h>

/* all the internal functions for DataColumn.c that will be included
 * directly into the source file */



// macro to convert arbitrary integer to string
// assumed you have a pre-allocated (and zeroed) character buffer
// e.g., char something[100] = {0};

#define integer_to_string(number, char_buff) \
    while (number >= 10) { \
        char num_char = (number % 10) + '0'; \
        strncat(char_buff, &num_char, 1); \
        number /= 10; \
    } \
    { \
        char num_char = number + '0'; \
        strncat(char_buff, &num_char, 1); \
    }

// a macro the wraps the above macro to take
// a column int value and convert & set to string

#define cast_column_int_to_string(column, i, numeric_string, type) \
	{ \
	type* value = (char*)column->value + i*sizeof(type); \
	integer_to_string(*value, numeric_string); \
	dt_column_set_value(column, i, &numeric_string); \
	}

// a macro to convert one int type to another int type
// e.g., uint8_t to int32_t

#define cast_column_int_to_int(column, i, from_type, to_type) \
	{ \
	from_type value = 0; \
	dt_column_get_value(column, i, &value); \
	to_type set_value = (to_type)value; \
	dt_column_set_value(column, i, &set_value); \
	}

static void
__reverse_string(
	char* str)
{
    size_t len = strlen(str);
    if (len < 2)
        return;
    
    for (size_t i = 0; i < len/2; ++i)
    {
        char temp = str[i];
        str[i] = str[len - i - 1];
        str[len - i - 1] = temp;
    }
}	

static void
__cast_column_string_to_numeric(
	struct DataColumn* const column,
	enum data_type_e new_type)
{
	// TODO: finish this logic for other types
	switch (new_type)
	{
		case DOUBLE:
		{
			// create copies to use later
			char** old_values = calloc(column->n_values, sizeof(*old_values));
			for (size_t i = 0; i < column->n_values; ++i)
				old_values[i] = strdup(*(char**)((char*)column->value + i*sizeof(char**)));

			// free all original strings before clearing value buffer
			// (or it would lead to memory leaks)
			for (size_t i = 0; i < column->n_values; ++i)
				free(*(char**)((char*)column->value + i*sizeof(char**)));

			memset(column->value, 0, column->value_capacity * column->type_size);
			// convert copies to appropriate type
			for (size_t i = 0; i < column->n_values; ++i)
			{
				char* endptr = NULL;
				double new_value = strtod(old_values[i], &endptr);
				dt_column_set_value(column, i, &new_value);
				free(old_values[i]);
			}

			free(old_values);
			
			// disable deallocator since it's not a string type anymore
			column->deallocator = NULL;

			break;
		}
	}
}

static void
__cast_column_numeric_to_string(
	struct DataColumn* const column,
	enum data_type_e old_type)
{
	switch (old_type)
	{
		// TODO: finish this logic for all other int types (turn this into a macro)
		case INT32:
			// the size of the column is different, record all OLD values
			// to replace (otherwise we'll have a bunch of overlapping
			// memory addresses which will overwrite each other)
			int32_t* old_values = calloc(column->n_values, sizeof(int32_t));
			for (size_t i = 0; i < column->n_values; ++i)
				old_values[i] = *(int32_t*)((char*)column->value + i*sizeof(int32_t));

			// clear value buffer before re-writing string addresses
			memset(column->value, 0, column->type_size * column->value_capacity);
			for (size_t i = 0; i < column->n_values; ++i)
			{
				char* numeric_string = calloc(25, sizeof(char));
				integer_to_string(old_values[i], numeric_string);
				// the string is actually reversed and it'll be too annoying to update the macro
				// so I'm taking the easy way out
				__reverse_string(numeric_string);
				dt_column_set_value(column, i, &numeric_string);
				// NOTE: we're not freeing numeric_string since the column type as of now
				// is technically not STRING yet so a copy is NOT made
			}

			free(old_values);

			// enable deallocator since we have a string type now
			column->deallocator = &dt_string_dealloc;
			break;

		// do nothing, but here to avoid compiler warning
		case STRING:
			break;
	}
}

static void
__cast_column_int_to_int(
	struct DataColumn* const column,
	enum data_type_e from_type,
	enum data_type_e to_type)
{
	// column types can be different sizes, so record OLD values,
	// clear the value buffer, and rewrite with the new types
	//
	// TODO: finish logic for all the other types (turn this into a macro)
	switch (from_type)
	{
		case FLOAT:
		{
			switch (to_type)
			{
				case UINT8:
				{
					float* old_values = calloc(column->n_values, sizeof(float));
					for (size_t i = 0; i < column->n_values; ++i)
						old_values[i] = *(float*)((char*)column->value + i*sizeof(float));

					memset(column->value, 0, column->value_capacity * column->type_size);
					for (size_t i = 0; i < column->n_values; ++i)
					{
						uint8_t new_value = (uint8_t)old_values[i];
						dt_column_set_value(column, i, &new_value);
					}

					free(old_values);
					break;
				}
				
			}
			break;
		}
	}	
}
