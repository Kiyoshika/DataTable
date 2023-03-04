#include <stdio.h>
#include "DataColumn.h"

int main()
{
	struct DataColumn* column = NULL;
	if (!dt_column_create(&column, 5, FLOAT))
	{
		fprintf(stderr, "There was a problem allocating memory.\n");
		return -1;
	}

	printf("Created column!\n");
	
	dt_column_free(&column);
	return 0;
}
