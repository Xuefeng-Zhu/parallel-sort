/** 
 * @file msort.c 
 * Author: Xuefeng Zhu

*/
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define INT_MAX	2147483647;
typedef struct _qsort_arg
{
	void* base;
	size_t num;
}qsort_arg;

typedef struct _merge_arg
{
	qsort_arg *qarg1;
	qsort_arg *qarg2;
	int *tArray;
}merge_arg;

int compareFunc (const void *a, const void *b)
{
	return *(int*)a - *(int*)b;	
}

void *qsort_thread(void *arg)
{
	qsort_arg *temp = (qsort_arg*)arg;
	void* base = temp->base;
	size_t num = temp->num;
	
	qsort(base, num, sizeof(int), compareFunc);

	fprintf(stderr, "Sorted %d elements.\n", (int)num);
}

void *merge_thread(void *arg)
{
	merge_arg *temp = (merge_arg*)arg;
	int *array1 = temp->qarg1->base;
	int num1 = temp->qarg1->num;
	int *array2= temp->qarg2->base;
	int num2 = temp->qarg2->num;
	int *tArray = temp->tArray;
	int i = 0, j = 0, index = 0;
	int dupes = 0;

	while(i < num1 && j < num2)
	{
		if (array1[i] <= array2[j])
		{
			if (array1[i] == array2[j])
			{
				dupes++;	
			}
			tArray[index] = array1[i];
			i++;
		}
		else
		{
			tArray[index] = array2[j];
			j++;
		}
		index++;
	}

	if (i == num1)
	{
		int k;
		for (k = j; k < num2; k++)
		{
			tArray[index] = array2[k];
			index++;
		}
	}
	else
	{
		int k;
		for (k = i; k < num1; k++)
		{
			tArray[index] = array1[k];
			index++;
		}
	}

	int k;
	for (k = 0; k < index; k++)
	{
		array1[k] = tArray[k];	
	}

	fprintf(stderr, "Merged %d and %d elements with %d duplicates.\n", num1,num2, dupes);
}

int main(int argc, char **argv)
{
	if (argc < 2)
	{
		fprintf(stderr, "usage: %s [# threads]", argv[0]);
		return 1;
	}

	int *array = malloc(sizeof(int) * 16);
	int arraySize = 16;
	int input_ct = 0;
	int number;
	
	while(fscanf(stdin, "%d", &number) != EOF)
	{
		array[input_ct] = number;
		input_ct++;
		if (input_ct >= arraySize)
		{
			arraySize *= 2;
			array = realloc(array, arraySize * sizeof(int));
		}
	}
	
	int segment_count = atoi(argv[1]);
	int values_per_segment;

	if (input_ct % segment_count == 0)
	{
		values_per_segment = input_ct / segment_count;	
	}
	else
	{
		values_per_segment = (input_ct / segment_count) + 1;
	}

	pthread_t threads[segment_count];
	
	qsort_arg *qsort_args = malloc(sizeof(qsort_arg) * segment_count);

	int i;

	for (i = 0; i < segment_count; i++)
	{
		qsort_args[i].base = array + values_per_segment * i;
		qsort_args[i].num = values_per_segment;
		if (i == segment_count -1)
		{
			qsort_args[i].num = input_ct - values_per_segment * i;	
		}

		pthread_create(&threads[i], NULL, qsort_thread, (void*)&qsort_args[i]);

	}

	for (i = 0; i < segment_count; i++)
	{
		pthread_join(threads[i], NULL);	
	}


	merge_arg *merge_args = malloc(sizeof(merge_arg) * segment_count / 2);
	int *tArray = malloc(input_ct * sizeof(int));
	while (segment_count > 1)
	{
		for (i = 0; i < segment_count / 2; i++)
		{
			merge_args[i].qarg1 = &qsort_args[2*i];
			merge_args[i].qarg2 = &qsort_args[2*i + 1];
			merge_args[i].tArray = tArray + ((int*)qsort_args[2*i].base - array);

			pthread_create(&threads[i], NULL, merge_thread, (void*)&merge_args[i]);
		}

		for (i = 0; i < segment_count / 2; i++)
		{
			pthread_join(threads[i], NULL);	
			qsort_args[i].base = qsort_args[2*i].base;
			qsort_args[i].num = qsort_args[2*i].num + qsort_args[2*i + 1].num;
		}

		if (segment_count % 2 != 0)
		{
			qsort_args[segment_count/2].base = qsort_args[segment_count - 1].base;	
			qsort_args[segment_count/2].num = qsort_args[segment_count - 1].num;	
		}

		segment_count = segment_count / 2 + segment_count % 2;
	}
	for (i = 0; i < input_ct; i++)
	{
		printf("%d\n", array[i]);
	}

	free(array);
	free(qsort_args);
	free(merge_args);
	free(tArray);
	return 0;

}
