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

/*
 * a macro that takes creates an array of all the OLD string values
 * in a column, clears the buffer, converts the string into a numeric 
 * and stores the value back into the buffer.
 */
#define convert_string_to_numeric(column, type_enum, type) \
	{ \
	char** old_values = calloc(column->n_values, sizeof(*old_values)); \
	for (size_t i = 0; i < column->n_values; ++i) \
		old_values[i] = strdup(*(char**)((char*)column->value + i*sizeof(char**))); \
	for (size_t i = 0; i < column->n_values; ++i) \
		free(*(char**)((char*)column->value + i*sizeof(char**))); \
	memset(column->value, 0, column->value_capacity * column->type_size); \
	for (size_t i = 0; i < column->n_values; ++i) \
	{ \
		char* endptr = NULL; \
		type new_value; \
		switch(type_enum) \
		{ \
			case DOUBLE: \
				new_value = strtod(old_values[i], &endptr); \
				break; \
			case FLOAT: \
				new_value = strtof(old_values[i], &endptr); \
				break; \
			case UINT8: \
			case UINT16: \
			case UINT32: \
				new_value = (type)strtoul(old_values[i], &endptr, 10); \
				break; \
			case UINT64: \
				new_value = (type)strtoull(old_values[i], &endptr, 10); \
				break; \
			case INT8: \
			case INT16: \
			case INT32: \
				new_value = (type)strtol(old_values[i], &endptr, 10); \
				break; \
			case INT64: \
				new_value = (type)strtoll(old_values[i], &endptr, 10); \
				break; \
			case STRING: \
				break; \
		} \
		dt_column_set_value(column, i, &new_value); \
		free(old_values[i]); \
	} \
	free(old_values); \
	column->deallocator = NULL; \
	}

static void
__cast_column_string_to_numeric(
	struct DataColumn* const column,
	enum data_type_e new_type)
{
	switch (new_type)
	{
		case DOUBLE:
			convert_string_to_numeric(column, new_type, double);
			break;
		case FLOAT:
			convert_string_to_numeric(column, new_type, float);
			break;
		case UINT8:
			convert_string_to_numeric(column, new_type, uint8_t);
			break;
		case UINT16:
			convert_string_to_numeric(column, new_type, uint16_t);
			break;
		case UINT32:
			convert_string_to_numeric(column, new_type, uint32_t);
			break;
		case UINT64:
			convert_string_to_numeric(column, new_type, uint64_t);
			break;
		case INT8:
			convert_string_to_numeric(column, new_type, int8_t);
			break;
		case INT16:
			convert_string_to_numeric(column, new_type, int16_t);
			break;
		case INT32:
			convert_string_to_numeric(column, new_type, int32_t);
			break;
		case INT64:
			convert_string_to_numeric(column, new_type, int64_t);
			break;
		// do nothing but ignore compiler warnings
		case STRING:
			break;
	}
}

/*
 * a macro that takes creates an array of all the OLD numeric values
 * in a column, clears the buffer, converts the numeric into a string
 * and stores the address back into the buffer.
 *
 * note that the buffer is already resized prior to calling this macro
 * (e.g., if moving from a uint8_t to char*, the buffer would need to be
 * larger)
 */
#define convert_numeric_to_string(column, type) \
	{ \
	type* old_values = calloc(column->n_values, sizeof(type)); \
	for (size_t i = 0; i < column->n_values; ++i) \
		old_values[i] = *(type*)((char*)column->value + i*sizeof(type)); \
	memset(column->value, 0, column->type_size * column->value_capacity); \
	for (size_t i = 0; i < column->n_values; ++i) \
	{ \
		char* numeric_string = calloc(25, sizeof(char)); \
		integer_to_string(old_values[i], numeric_string); \
		__reverse_string(numeric_string); \
		dt_column_set_value(column, i, &numeric_string); \
	} \
	free(old_values); \
	column->deallocator = &dt_string_dealloc; \
	}


static void
__cast_column_numeric_to_string(
	struct DataColumn* const column,
	enum data_type_e old_type)
{
	switch (old_type)
	{
		case INT8:
			convert_numeric_to_string(column, int8_t);
			break;
		case INT16:
			convert_numeric_to_string(column, int16_t);
			break;
		case INT32:
			convert_numeric_to_string(column, int32_t);
			break;
		case INT64:
			convert_numeric_to_string(column, int64_t);
			break;
		case UINT8:
			convert_numeric_to_string(column, uint8_t);
			break;
		case UINT16:
			convert_numeric_to_string(column, uint16_t);
			break;
		case UINT32:
			convert_numeric_to_string(column, uint32_t);
			break;
		case UINT64:
			convert_numeric_to_string(column, uint64_t);
			break;
		// TODO: handle these separately
		case FLOAT:
			break;
		case DOUBLE:
			break;

		// do nothing, but here to avoid compiler warning
		case STRING:
			break;
	}
}

/*
 * a macro that takes creates an array of all the OLD numeric values
 * in a column, clears the buffer, converts the numeric into the other type 
 * and stores the address back into the buffer.
 *
 * note that the buffer is already resized prior to calling this macro
 * (e.g., if moving from a uint8_t to uint64_t, the buffer would need to be
 * larger)
 */
#define convert_numeric_to_numeric(column, from_type, to_type) \
	{ \
		from_type* old_values = calloc(column->n_values, sizeof(from_type)); \
		for (size_t i = 0; i < column->n_values; ++i) \
			old_values[i] = *(from_type*)((char*)column->value + i*sizeof(from_type)); \
		memset(column->value, 0, column->value_capacity * column->type_size); \
		for (size_t i = 0; i < column->n_values; ++i) \
		{ \
			to_type new_value = (to_type)old_values[i]; \
			dt_column_set_value(column, i, &new_value); \
		} \
		free(old_values); \
	} \

static void
__cast_column_int_to_int(
	struct DataColumn* const column,
	enum data_type_e from_type,
	enum data_type_e to_type)
{
	switch (from_type)
	{
		case FLOAT:
		{
			switch (to_type)
			{
				case UINT8:
					convert_numeric_to_numeric(column, float, uint8_t);
					break;
				case UINT16:
					convert_numeric_to_numeric(column, float, uint16_t);
					break;
				case UINT32:
					convert_numeric_to_numeric(column, float, uint32_t);
					break;
				case UINT64:
					convert_numeric_to_numeric(column, float, uint64_t);
					break;
				case INT8:
					convert_numeric_to_numeric(column, float, int8_t);
					break;
				case INT16:
					convert_numeric_to_numeric(column, float, int16_t);
					break;
				case INT32:
					convert_numeric_to_numeric(column, float, int32_t);
					break;
				case INT64:
					convert_numeric_to_numeric(column, float, int64_t);
					break;
				case FLOAT:
					convert_numeric_to_numeric(column, float, float);
					break;
				case DOUBLE:
					convert_numeric_to_numeric(column, float, double);
					break;
				// does nothing but ignores compiler warning
				case STRING:
					break;
			}
			break;
		}

		case DOUBLE:
		{
			switch (to_type)
			{
				case UINT8:
					convert_numeric_to_numeric(column, double, uint8_t);
					break;
				case UINT16:
					convert_numeric_to_numeric(column, double, uint16_t);
					break;
				case UINT32:
					convert_numeric_to_numeric(column, double, uint32_t);
					break;
				case UINT64:
					convert_numeric_to_numeric(column, double, uint64_t);
					break;
				case INT8:
					convert_numeric_to_numeric(column, double, int8_t);
					break;
				case INT16:
					convert_numeric_to_numeric(column, double, int16_t);
					break;
				case INT32:
					convert_numeric_to_numeric(column, double, int32_t);
					break;
				case INT64:
					convert_numeric_to_numeric(column, double, int64_t);
					break;
				case FLOAT:
					convert_numeric_to_numeric(column, double, float);
					break;
				case DOUBLE:
					convert_numeric_to_numeric(column, double, double);
					break;
				// does nothing but ignores compiler warning
				case STRING:
					break;
			}
			break;
		}

		case UINT8:
		{
			switch (to_type)
			{
				case UINT8:
					convert_numeric_to_numeric(column, uint8_t, uint8_t);
					break;
				case UINT16:
					convert_numeric_to_numeric(column, uint8_t, uint16_t);
					break;
				case UINT32:
					convert_numeric_to_numeric(column, uint8_t, uint32_t);
					break;
				case UINT64:
					convert_numeric_to_numeric(column, uint8_t, uint64_t);
					break;
				case INT8:
					convert_numeric_to_numeric(column, uint8_t, int8_t);
					break;
				case INT16:
					convert_numeric_to_numeric(column, uint8_t, int16_t);
					break;
				case INT32:
					convert_numeric_to_numeric(column, uint8_t, int32_t);
					break;
				case INT64:
					convert_numeric_to_numeric(column, uint8_t, int64_t);
					break;
				case FLOAT:
					convert_numeric_to_numeric(column, uint8_t, float);
					break;
				case DOUBLE:
					convert_numeric_to_numeric(column, uint8_t, double);
					break;
				// does nothing but ignores compiler warning
				case STRING:
					break;
			}
			break;
		}

		case UINT16:
		{
			switch (to_type)
			{
				case UINT8:
					convert_numeric_to_numeric(column, uint16_t, uint8_t);
					break;
				case UINT16:
					convert_numeric_to_numeric(column, uint16_t, uint16_t);
					break;
				case UINT32:
					convert_numeric_to_numeric(column, uint16_t, uint32_t);
					break;
				case UINT64:
					convert_numeric_to_numeric(column, uint16_t, uint64_t);
					break;
				case INT8:
					convert_numeric_to_numeric(column, uint16_t, int8_t);
					break;
				case INT16:
					convert_numeric_to_numeric(column, uint16_t, int16_t);
					break;
				case INT32:
					convert_numeric_to_numeric(column, uint16_t, int32_t);
					break;
				case INT64:
					convert_numeric_to_numeric(column, uint16_t, int64_t);
					break;
				case FLOAT:
					convert_numeric_to_numeric(column, uint16_t, float);
					break;
				case DOUBLE:
					convert_numeric_to_numeric(column, uint16_t, double);
					break;
				// does nothing but ignores compiler warning
				case STRING:
					break;
			}
			break;
		}

		case UINT32:
		{
			switch (to_type)
			{
				case UINT8:
					convert_numeric_to_numeric(column, uint32_t, uint8_t);
					break;
				case UINT16:
					convert_numeric_to_numeric(column, uint32_t, uint16_t);
					break;
				case UINT32:
					convert_numeric_to_numeric(column, uint32_t, uint32_t);
					break;
				case UINT64:
					convert_numeric_to_numeric(column, uint32_t, uint64_t);
					break;
				case INT8:
					convert_numeric_to_numeric(column, uint32_t, int8_t);
					break;
				case INT16:
					convert_numeric_to_numeric(column, uint32_t, int16_t);
					break;
				case INT32:
					convert_numeric_to_numeric(column, uint32_t, int32_t);
					break;
				case INT64:
					convert_numeric_to_numeric(column, uint32_t, int64_t);
					break;
				case FLOAT:
					convert_numeric_to_numeric(column, uint32_t, float);
					break;
				case DOUBLE:
					convert_numeric_to_numeric(column, uint32_t, double);
					break;
				// does nothing but ignores compiler warning
				case STRING:
					break;
			}
			break;
		}

		case UINT64:
		{
			switch (to_type)
			{
				case UINT8:
					convert_numeric_to_numeric(column, uint64_t, uint8_t);
					break;
				case UINT16:
					convert_numeric_to_numeric(column, uint64_t, uint16_t);
					break;
				case UINT32:
					convert_numeric_to_numeric(column, uint64_t, uint32_t);
					break;
				case UINT64:
					convert_numeric_to_numeric(column, uint64_t, uint64_t);
					break;
				case INT8:
					convert_numeric_to_numeric(column, uint64_t, int8_t);
					break;
				case INT16:
					convert_numeric_to_numeric(column, uint64_t, int16_t);
					break;
				case INT32:
					convert_numeric_to_numeric(column, uint64_t, int32_t);
					break;
				case INT64:
					convert_numeric_to_numeric(column, uint64_t, int64_t);
					break;
				case FLOAT:
					convert_numeric_to_numeric(column, uint64_t, float);
					break;
				case DOUBLE:
					convert_numeric_to_numeric(column, uint64_t, double);
					break;
				// does nothing but ignores compiler warning
				case STRING:
					break;
			}
			break;
		}

		case INT8:
		{
			switch (to_type)
			{
				case UINT8:
					convert_numeric_to_numeric(column, int8_t, uint8_t);
					break;
				case UINT16:
					convert_numeric_to_numeric(column, int8_t, uint16_t);
					break;
				case UINT32:
					convert_numeric_to_numeric(column, int8_t, uint32_t);
					break;
				case UINT64:
					convert_numeric_to_numeric(column, int8_t, uint64_t);
					break;
				case INT8:
					convert_numeric_to_numeric(column, int8_t, int8_t);
					break;
				case INT16:
					convert_numeric_to_numeric(column, int8_t, int16_t);
					break;
				case INT32:
					convert_numeric_to_numeric(column, int8_t, int32_t);
					break;
				case INT64:
					convert_numeric_to_numeric(column, int8_t, int64_t);
					break;
				case FLOAT:
					convert_numeric_to_numeric(column, int8_t, float);
					break;
				case DOUBLE:
					convert_numeric_to_numeric(column, int8_t, double);
					break;
				// does nothing but ignores compiler warning
				case STRING:
					break;
			}
			break;
		}

		case INT16:
		{
			switch (to_type)
			{
				case UINT8:
					convert_numeric_to_numeric(column, int16_t, uint8_t);
					break;
				case UINT16:
					convert_numeric_to_numeric(column, int16_t, uint16_t);
					break;
				case UINT32:
					convert_numeric_to_numeric(column, int16_t, uint32_t);
					break;
				case UINT64:
					convert_numeric_to_numeric(column, int16_t, uint64_t);
					break;
				case INT8:
					convert_numeric_to_numeric(column, int16_t, int8_t);
					break;
				case INT16:
					convert_numeric_to_numeric(column, int16_t, int16_t);
					break;
				case INT32:
					convert_numeric_to_numeric(column, int16_t, int32_t);
					break;
				case INT64:
					convert_numeric_to_numeric(column, int16_t, int64_t);
					break;
				case FLOAT:
					convert_numeric_to_numeric(column, int16_t, float);
					break;
				case DOUBLE:
					convert_numeric_to_numeric(column, int16_t, double);
					break;
				// does nothing but ignores compiler warning
				case STRING:
					break;
			}
			break;
		}

		case INT32:
		{
			switch (to_type)
			{
				case UINT8:
					convert_numeric_to_numeric(column, int32_t, uint8_t);
					break;
				case UINT16:
					convert_numeric_to_numeric(column, int32_t, uint16_t);
					break;
				case UINT32:
					convert_numeric_to_numeric(column, int32_t, uint32_t);
					break;
				case UINT64:
					convert_numeric_to_numeric(column, int32_t, uint64_t);
					break;
				case INT8:
					convert_numeric_to_numeric(column, int32_t, int8_t);
					break;
				case INT16:
					convert_numeric_to_numeric(column, int32_t, int16_t);
					break;
				case INT32:
					convert_numeric_to_numeric(column, int32_t, int32_t);
					break;
				case INT64:
					convert_numeric_to_numeric(column, int32_t, int64_t);
					break;
				case FLOAT:
					convert_numeric_to_numeric(column, int32_t, float);
					break;
				case DOUBLE:
					convert_numeric_to_numeric(column, int32_t, double);
					break;
				// does nothing but ignores compiler warning
				case STRING:
					break;
			}
			break;
		}

		case INT64:
		{
			switch (to_type)
			{
				case UINT8:
					convert_numeric_to_numeric(column, int64_t, uint8_t);
					break;
				case UINT16:
					convert_numeric_to_numeric(column, int64_t, uint16_t);
					break;
				case UINT32:
					convert_numeric_to_numeric(column, int64_t, uint32_t);
					break;
				case UINT64:
					convert_numeric_to_numeric(column, int64_t, uint64_t);
					break;
				case INT8:
					convert_numeric_to_numeric(column, int64_t, int8_t);
					break;
				case INT16:
					convert_numeric_to_numeric(column, int64_t, int16_t);
					break;
				case INT32:
					convert_numeric_to_numeric(column, int64_t, int32_t);
					break;
				case INT64:
					convert_numeric_to_numeric(column, int64_t, int64_t);
					break;
				case FLOAT:
					convert_numeric_to_numeric(column, int64_t, float);
					break;
				case DOUBLE:
					convert_numeric_to_numeric(column, int64_t, double);
					break;
				// does nothing but ignores compiler warning
				case STRING:
					break;
			}
			break;
		}

		// do nothing but ignores compiler warnings
		case STRING:
			break;
	}	
}
