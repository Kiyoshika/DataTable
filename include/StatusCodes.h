#ifndef STATUS_CODES_H
#define STATUS_CODES_H

enum status_code_e
{
	DT_SUCCESS = 1,
	DT_ALLOC_ERROR = -1,
	DT_INDEX_ERROR = -2,
	DT_TYPE_MISMATCH = -3,
	DT_SIZE_MISMATCH = -4,
	DT_FAILURE = -5
};

#endif
