//
//  isax_file_loaders.c
//  isax
//
//  Created by Kostas Zoumpatianos on 4/7/12.
//  Copyright 2012 University of Trento. All rights reserved.
//
#include "../../config.h"
#include "../../globals.h"
#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <inttypes.h>
#include <limits.h>
#include <bsd/stdlib.h>
#include "ads/isax_node.h"
#include "ads/isax_index.h"
#include "ads/isax_query_engine.h"
#include "ads/isax_node_record.h"
#include "ads/isax_file_loaders.h"
#include "ads/pqueue.h"

/*
int findLargestNum(int * array, int size){
  
  int i;
  int largestNum = -1;
  
  for(i = 0; i < size; i++){
    if(array[i] > largestNum)
      largestNum = array[i];
  }
  
  return largestNum;
}

 Radix Sort
void my_radixSort(int * array, int size){
  
  printf("\n\nRunning Radix Sort on Unsorted List!\n\n");

  // Base 10 is used
  int i;
  int semiSorted[size];
  int significantDigit = 1;
  int largestNum = findLargestNum(array, size);
  
  // Loop until we reach the largest significant digit
  while (largestNum / significantDigit > 0){
    
   
    
    
    int bucket[10] = { 0 };
    
    // Counts the number of "keys" or digits that will go into each bucket
    for (i = 0; i < size; i++)
      bucket[(array[i] / significantDigit) % 10]++;
    
    /**
     * Add the count of the previous buckets,
     * Acquires the indexes after the end of each bucket location in the array
		 * Works similar to the count sort algorithm
     **/
/*    for (i = 1; i < 10; i++)
      bucket[i] += bucket[i - 1];
    
    // Use the bucket to fill a "semiSorted" array
    for (i = size - 1; i >= 0; i--)
      semiSorted[--bucket[(array[i] / significantDigit) % 10]] = array[i];
    
    
    for (i = 0; i < size; i++)
      array[i] = semiSorted[i];
    
    // Move to next significant digit
    significantDigit *= 10;
    

  }
}
*/

void isax_query_binary_file(const char *ifilename, int q_num, isax_index *index,
                            float minimum_distance, int min_checked_leaves,
                            query_result (*search_function)(ts_type*, ts_type*, isax_index*, float, int)) {
	fprintf(stderr, ">>> Performing queries in file: %s\n", ifilename);

	FILE * ifile;
	ifile = fopen (ifilename,"rb");
    if (ifile == NULL) {
        fprintf(stderr, "File %s not found!\n", ifilename);
        exit(-1);
    }

    fseek(ifile, 0L, SEEK_END);
    file_position_type sz = (file_position_type) ftell(ifile);
    file_position_type total_records = sz/index->settings->ts_byte_size;
    fseek(ifile, 0L, SEEK_SET);
    if (total_records < q_num) {
        fprintf(stderr, "File %s has only %llu records!\n", ifilename, total_records);
        exit(-1);
    }

	int q_loaded = 0;
    ts_type * ts = malloc(sizeof(ts_type) * index->settings->timeseries_size);
    ts_type * paa = malloc(sizeof(ts_type) * index->settings->paa_segments);
    //sax_type * sax = malloc(sizeof(sax_type) * index->settings->paa_segments);

    float avg_approximate=0;
    float avg_time=0;
    int avg_nodes_checked=0;

    while (q_loaded < q_num)
    {
        COUNT_INPUT_TIME_START
        fread(ts, sizeof(ts_type),index->settings->timeseries_size,ifile);
        COUNT_INPUT_TIME_END
        //printf("Querying for: %d\n", index->settings->ts_byte_size * q_loaded);
        // Parse ts and make PAA representation
        paa_from_ts(ts, paa, index->settings->paa_segments,
                    index->settings->ts_values_per_paa_segment);
        COUNT_TOTAL_TIME_START
        query_result result = search_function(ts, paa, index, minimum_distance, min_checked_leaves);
        COUNT_TOTAL_TIME_END
	
	//printf("\n=----->total time:%lf", total_time);
	//printf("\n=----->checked_nodes:%d", checked_nodes);

        PRINT_STATS(result.distance)
        //fflush(stdout);
#if VERBOSE_LEVEL >= 1
        printf("[%p]: Distance: %lf\n", result.node, result.distance);
#endif
	//ts_print(ts, index->settings->sax_bit_cardinality);
        //sax_from_paa(paa, sax, index->settings->paa_segments, index->settings->sax_alphabet_cardinality, index->settings->sax_bit_cardinality);
	//printf("\n");int wi;
        //for (wi=0; wi<index->settings->paa_segments; wi++)
        //               printf("%d.%d", sax[wi], index->settings->sax_bit_cardinality);

        //if (index->settings->timeseries_size * sizeof(ts_type) * q_loaded == 1024) {
        //    sax_print(sax, index->settings->paa_segments, index->settings->sax_bit_cardinality);
        //}

        q_loaded++;

	//avg_nodes_checked=avg_nodes_checked+checked_nodes;
	//avg_approximate=avg_approximate+APPROXIMATE;
	//avg_time=avg_time+total_time;

    }

    //printf("\n Total_Approximate: %f  Total_Time:%lf  Total_Checked_Nodes:%d ", avg_approximate, avg_time, avg_nodes_checked);

    free(paa);
    free(ts);
    fclose(ifile);
    fprintf(stderr, ">>> Finished querying.\n");

}

 
/** Return returns 1 if a>b, -1 if b>a, 0 if a == b */
int myz_compare(const void * a1, const void *b1)
{

	my_node *a = a1;
	my_node *b = b1;
	
	//my_uint128_t *a = (my_uint128_t*) &((sax_vector*)a_or)->sax;

	//my_node *b = &((my_node *) b1);
	
	if(a==NULL || ((*a).value)==NULL)
		printf( "\nmyz_compare==>>>>Error sorting NULL values for a value\n\n");	
	if(b==NULL || ((*b).value)==NULL)
		printf( "\nmyz_compare==>>>>Error sorting NULL values for b value\n\n");	

	int x=strcmp( (*a).value, (*b).value);

	if(x>0)//b less than a
		return 1;
	else if(x<0)//a less than b
		return -1;
	else //equals
		return 0;
}

void isax_index_binary_file_hk(const char *ifilename, int ts_num, isax_index *index, int approach, int sort_approach)
{
	fprintf(stderr, ">>> Indexing: %s\n", ifilename);
    FILE * ifile;
    COUNT_INPUT_TIME_START
	ifile = fopen (ifilename,"r");
    COUNT_INPUT_TIME_END
    if (ifile == NULL) {
        fprintf(stderr, "File %s not found!\n", ifilename);
        exit(-1);
    }
    fseek(ifile, 0L, SEEK_END);
    file_position_type sz = (file_position_type) ftell(ifile);
    file_position_type total_records = sz/index->settings->ts_byte_size;
    fseek(ifile, 0L, SEEK_SET);
    if (total_records < ts_num) {
        fprintf(stderr, "File %s has only %llu records!\n", ifilename, total_records);
        exit(-1);
    }


    //printf("\n=======aaaaaaaaaaaaaaaaaaaaaaaaaaaaa%d",index->settings->paa_segments);
    //printf("\n=======aaaaaaaaaaaaaaaaaaaaaaaaaaaaa%d",index->settings->ts_values_per_paa_segment);
    //printf("\n=======aaaaaaaaaaaaaaaaaaaaaaaaaaaaa%d",index->settings->sax_alphabet_cardinality);
    //printf("\n=======aaaaaaaaaaaaaaaaaaaaaaaaaaaaa%d",index->settings->sax_bit_cardinality);

    int ts_loaded = 0;
    index->settings->raw_filename = malloc(256);
    strcpy(index->settings->raw_filename, ifilename);

    ts_type * ts = malloc(sizeof(ts_type) * index->settings->timeseries_size);
    sax_type * sax = malloc(sizeof(sax_type) * index->settings->paa_segments);

#ifdef BENCHMARK
    int percentage = (int) (ts_num / (file_position_type) 100);
#endif


    my_node * currqueue;
    int currcount=0;
    int runs=0;

    while (ts_loaded<ts_num)
    {
	    currcount=0;

	    int curqueusize=index->settings->max_total_full_buffer_size;
	    if(ts_num<index->settings->max_total_full_buffer_size)
		curqueusize=ts_num;
    	    my_node * currqueue  = malloc(sizeof(my_node) * curqueusize);
	    
	    while (ts_loaded<ts_num && currcount< index->settings->max_total_full_buffer_size)
	    {
    		currqueue[currcount].fileposition= malloc(sizeof(file_position_type));

        	COUNT_CHECKED_NODE();
        	COUNT_INPUT_TIME_START
		*(currqueue[currcount].fileposition)= ftell(ifile);
        	fread(ts, sizeof(ts_type), index->settings->timeseries_size, ifile);
        	COUNT_INPUT_TIME_END

        	if(sax_from_ts(ts, sax, index->settings->ts_values_per_paa_segment,
        	               index->settings->paa_segments, index->settings->sax_alphabet_cardinality,
        	               index->settings->sax_bit_cardinality) == SUCCESS)
		{
                	currqueue[currcount].sax = malloc(sizeof(sax_type)*index->settings->paa_segments);
			memcpy(currqueue[currcount].sax, sax, sizeof(sax_type)*index->settings->paa_segments);
        		
			//sax_print(sax,index->settings->paa_segments ,index->settings->sax_bit_cardinality);  //print
			//ts_print(ts, index->settings->sax_bit_cardinality);
			currqueue[currcount].value = malloc(sizeof(char) * ((index->settings->paa_segments*index->settings->sax_bit_cardinality)+1));
		        ordering_value(currqueue[currcount].value, sax,index->settings->paa_segments ,index->settings->sax_bit_cardinality);

			//if(sort_approach==3)
			//{	
			//	currqueue[currcount].ts = malloc(sizeof(ts_type) * index->settings->timeseries_size);
			//	memcpy(currqueue[currcount].ts, ts, sizeof(ts_type) * index->settings->timeseries_size);
			//}

			//printf("\n");int wi;
        		//for (wi=0; wi<index->settings->paa_segments; wi++)
                	//        printf("%d.%d", sax[wi], index->settings->sax_bit_cardinality);
	
			currcount++;
			ts_loaded++;
			
		}
        	else //if sax != success
        	{
        	    fprintf(stderr, "error: cannot insert record in index, since sax representation\
        	            failed to be created");
        	} //end sax!=success

    	     }	

    	runs++;
	COUNT_SORTING_TIME_START
	//qsort(currqueue, currcount, sizeof(my_node),  myz_compare);
	radixsort(currqueue, currcount, sizeof(my_node), myz_compare );	
	//sradixsort(currqueue, currcount, NULL, currqueue+currcount);	
	COUNT_SORTING_TIME_END
	


     	//radixsort(const unsigned char **base, int nmemb, const unsigned char *table, unsigned endbyte);
	//sradixsort(currqueue, currcount, NULL, currqueue+currcount);




    	if(total_records <= index->settings->max_total_full_buffer_size){
        	int z;	
		root_mask_type past_mask;
		
		//writing new ordered file
                char* pfilename = malloc(255);
		FILE *pfile;
		//if(sort_approach==3)
		//{	
                //	snprintf(pfilename, 255, "%s.ordered",index->settings->raw_filename);
    		//	strcpy(index->settings->raw_filename, pfilename);
                //	COUNT_OUTPUT_TIME_START
                //	pfile = fopen(pfilename, "w");
                //	COUNT_OUTPUT_TIME_END
		//}
		//writing new ordered file


		for(z=0;z<currcount;z++)
		{	
			
			//writing new ordered file
			//if(sort_approach==3)
			//{	
			//	*(currqueue[z].fileposition)= ftell(pfile);
                	//	COUNT_OUTPUT_TIME_START
        		//	fwrite(currqueue[z].ts, sizeof(ts_type), index->settings->timeseries_size, pfile);
                	//	COUNT_OUTPUT_TIME_END
			//}
			//writing new ordered file

			my_node max=currqueue[z];		
			if(approach==0)//if you are building the index with the new algorithm
			{
				root_mask_type first_bit_mask = 0x00;
    				CREATE_MASK(first_bit_mask, index, max.sax);

				if(z==0)
					past_mask=first_bit_mask;
    				

			        if(past_mask!=first_bit_mask) {
			        	FLUSHES++;
					//printf("\nmask:%d",first_bit_mask);
					//if(sort_approach==1)
        				//	flush_fbl_hk2(index->fbl, index);
        				//else
					flush_fbl_hk(index->fbl, index,1);
					past_mask=first_bit_mask;

					//TODO remove break
					//break;
    				}
				
				insert_to_fbl(index->fbl, max.sax, max.fileposition,first_bit_mask, index);
				index->total_records++;
			}
        		else //if you are building the index with the past algorithm
			{
        		        isax_fbl_index_insert(index, max.sax, max.fileposition);
			}

		}


                //COUNT_OUTPUT_TIME_START
                //fclose(pfile);
                //COUNT_OUTPUT_TIME_END
                //free(pfilename);

    	}
    	else if(currcount>0)
    	{
                char* pfilename = malloc(255);
                snprintf(pfilename, 255, "%s.0.%d",index->settings->raw_filename,runs);
                COUNT_OUTPUT_TIME_START
		//printf("\nzzzz--%s", pfilename);      
                FILE *pfile = fopen(pfilename, "w");
                int j;
                for (j = 0; j < currcount; j++) {
                          
			//printf("\nzzzz--%s %d",currqueue[j].value, *(currqueue[j].fileposition));      
        		COUNT_CHECKED_NODE();
                        fwrite(currqueue[j].fileposition,
                                sizeof(file_position_type), 1, pfile);
        		COUNT_CHECKED_NODE();
                        fwrite(currqueue[j].sax,
                                sizeof(sax_type), index->settings->paa_segments, pfile);

                }
                fclose(pfile);
                COUNT_OUTPUT_TIME_END
                free(pfilename);
    	}
        int j; 
	for (j = 0; j < currcount; j++) {
		free(currqueue[j].fileposition);
		free(currqueue[j].sax);
		free(currqueue[j].value);
	}

    	free(currqueue);
      }//end while
	

    int passes=(float)ceil(log((float)runs)/log((float)index->settings->max_total_full_buffer_size));

    free(ts);
    free(sax);
    COUNT_INPUT_TIME_START
    fclose(ifile);
    COUNT_INPUT_TIME_END
    
    if(ts_num > index->settings->max_total_full_buffer_size)
    	mergeandbuildindex(1,runs,  index, 0,0,passes, approach);
    

    fprintf(stderr, ">>> Finished indexing\n");

}

void mergeandbuildindex(int startrun, int endrun, isax_index * index, int currpass, int currrun, int totalpasses, int only_merge)
{
   int runs=endrun-startrun+1;
   int buffer_size=10000000;//index->settings->max_total_full_buffer_size;
   char * filename=index->settings->raw_filename;	 
   file_position_type * posinfile = malloc(sizeof(file_position_type));
   sax_type * sax = malloc(sizeof(sax_type) * index->settings->paa_segments);
   int i,j;
   if(runs==1)
   {
	//printf("\npass:%d run:%d no merge required", currpass, currrun);
	//printf("\n    ==> aconsuming%s.%d.%d",filename,currpass,startrun);

       	char* oldfilename = malloc(255);
       	snprintf(oldfilename, 255, "%s.%d.%d",filename,currpass,startrun);
       	
	//printf("\n    ==> agenerating %s.%d.%d",filename,currpass+1,currrun);

       	char* newfilename = malloc(255);
       	snprintf(newfilename, 255, "%s.%d.%d",filename,currpass+1,currrun);
       	

	rename(oldfilename, newfilename);

	free(oldfilename);
	free(newfilename);
   }
   else if(runs<=buffer_size)
   {

	//printf("\none merge startrun:%d endrun:%d in  pass %d",startrun,endrun,currpass);
     	
	my_node * currqueue  = malloc(sizeof(my_node) * runs);
	int currcount=0;
    	int mo=0;
	FILE ** files=malloc(sizeof(FILE)*runs);
        char * filenames[runs];

	int fileno=0;
	int limits[runs];

	int totalrecords=0;

	//opening files
	for(i=startrun;i<=endrun;i++)
	{
		//printf("\n    ==> bconsuming%s.%d.%d",filename,currpass,i);fflush(stdout);

            	filenames[fileno] = malloc(255);
            	snprintf(filenames[fileno], 255, "%s.%d.%d",filename,currpass,i);
            	

    		COUNT_INPUT_TIME_START
		files[fileno] = fopen(filenames[fileno], "r");
    		COUNT_INPUT_TIME_END
	
		//filenames[fileno]=strdup(pfilename);		

		if (files[fileno]== NULL) {
        	        fprintf(stderr, "File %s not found!\n",filenames[fileno]);
        		exit(-1);
        	}

        	fseek(files[fileno], 0L, SEEK_END);

		file_position_type sz = (file_position_type) ftell(files[fileno]);
		file_position_type total_records_in_file = (sz/(sizeof(file_position_type)+(sizeof(sax_type)*index->settings->paa_segments)));

        	fseek(files[fileno], 0L, SEEK_SET);

		limits[fileno]=total_records_in_file-1;
		totalrecords+=total_records_in_file;
		//free(pfilename);
    		
		currqueue[fileno].sax=malloc(sizeof(sax_type)*index->settings->paa_segments);
    		currqueue[fileno].fileposition=malloc(sizeof(file_position_type));
		currqueue[fileno].value = malloc(sizeof(char) * ((index->settings->paa_segments*index->settings->sax_bit_cardinality)+1));
		
		fileno++;
	}



	//read one element from each file
	for(i=0;i<fileno;i++)
        {

                FILE * ifile2=files[i];
        	COUNT_CHECKED_NODE();
        	COUNT_CHECKED_NODE();
    		COUNT_INPUT_TIME_START
		fread(posinfile, sizeof(file_position_type),1, ifile2);
                fread(sax, sizeof(sax_type),index->settings->paa_segments, ifile2);
    		COUNT_INPUT_TIME_END
		
        	/*
                my_node node;
		node.pos=i;
    		node.sax=malloc(sizeof(sax_type)*index->settings->paa_segments);
    		node.fileposition=malloc(sizeof(file_position_type));

		memcpy(node.sax, sax, sizeof(sax_type)*index->settings->paa_segments);
		memcpy(node.fileposition, posinfile, sizeof(file_position_type));

		node.value = malloc(sizeof(char) * ((index->settings->paa_segments*index->settings->sax_bit_cardinality)+1));
		ordering_value(node.value, sax,index->settings->paa_segments ,index->settings->sax_bit_cardinality);
		*/
		
		if(currcount==0)
		{
			//currqueue[0]=node;
			memcpy(currqueue[currcount].sax, sax, sizeof(sax_type)*index->settings->paa_segments);
			memcpy(currqueue[currcount].fileposition, posinfile, sizeof(file_position_type));
			ordering_value(currqueue[currcount].value, sax,index->settings->paa_segments ,index->settings->sax_bit_cardinality);
			currqueue[currcount].pos=i;
		}
		else
		{
			int mo;
			char *value = malloc(sizeof(char) * ((index->settings->paa_segments*index->settings->sax_bit_cardinality)+1));
			ordering_value(value, sax,index->settings->paa_segments ,index->settings->sax_bit_cardinality);
			
			for(mo=currcount-1;mo>=0;mo--)
                	{
                        	if(strcmp(value,currqueue[mo].value)>0)
				{
                                	//currqueue[mo+1]=currqueue[mo];
					memcpy(currqueue[mo+1].sax, currqueue[mo].sax, sizeof(sax_type)*index->settings->paa_segments);
					memcpy(currqueue[mo+1].fileposition, currqueue[mo].fileposition, sizeof(file_position_type));
					memcpy(currqueue[mo+1].value, currqueue[mo].value, sizeof(char) * ((index->settings->paa_segments*index->settings->sax_bit_cardinality)+1));
					currqueue[mo+1].pos=currqueue[mo].pos;
				}
                        	else
                                	break;
                	}
			memcpy(currqueue[mo+1].sax, sax, sizeof(sax_type)*index->settings->paa_segments);
			memcpy(currqueue[mo+1].fileposition, posinfile, sizeof(file_position_type));
			memcpy(currqueue[mo+1].value, value, sizeof(char) * ((index->settings->paa_segments*index->settings->sax_bit_cardinality)+1));
			currqueue[mo+1].pos=i;
			free(value);
			//currqueue[mo+1]=node;
		}	

	        // pqeue_insert(currqueue, node, currcount, index);
                //pqeue_insert(currqueue, i, sax, posinfile, currcount, index);
                currcount++;
		
        }




	char* newfilename = malloc(255);
	snprintf(newfilename, 255, "%s.%d.%d",index->settings->raw_filename,currpass+1, currrun);
	
	char *zivalue= malloc(sizeof(char)*(((index->settings->paa_segments)*(index->settings->sax_bit_cardinality))+1));
		


    	my_node * mybuffer;
    	int sizecounter=0;

	root_mask_type past_mask;
	int flag=0;
        int maxpos=0;
	for(j=0;j<totalrecords;j++)
	{
		maxpos=currqueue[currcount-1].pos;
		//printf("\na1111111111111zssssssssssssssssssssssssssssssssssssi %d", currcount-1);fflush(stdout);
		
		//my_node *max=malloc(sizeof(my_node));
		//max.sax=malloc(sizeof(sax_type)*index->settings->paa_segments);
    		//max.fileposition=malloc(sizeof(file_position_type));
		//max.value = malloc(sizeof(char) * ((index->settings->paa_segments*index->settings->sax_bit_cardinality)+1));
		//printf("\na1111111111111zssssssssssssssssssssssssssssssssssss");fflush(stdout);
		//get max
		//memcpy(max.sax, currqueue[currcount-1].sax, sizeof(sax_type)*index->settings->paa_segments);
		//memcpy(max.fileposition, currqueue[currcount-1].fileposition, sizeof(file_position_type));
		//memcpy(max.value, currqueue[currcount-1].value, sizeof(char) * ((index->settings->paa_segments*index->settings->sax_bit_cardinality)+1));
		//(*max).pos=currqueue[currcount-1].pos;
		//currqueue[currcount-1];
		
		
		
		//in the last pass build index
		/*if(1)//(totalpasses==currpass+1)// && j<fileno-1+4)//in the last pass build the index
		{
   			//file_position_type * mfileposition = malloc(sizeof(file_position_type));
   			//sax_type * msax = malloc(sizeof(sax_type) * index->settings->paa_segments);
			//memcpy(msax,max.sax, sizeof(sax_type)*index->settings->paa_segments);
			//memcpy(mfileposition, max.fileposition, sizeof(file_position_type));
			/////////////////building index/////////////////////////
			
			if(only_merge==0)//if you are building the index with the new algorithm
			{
				root_mask_type first_bit_mask = 0x00;
                                CREATE_MASK(first_bit_mask, index, currqueue[currcount-1].sax);

                                if(flag==0)
				{
                                        past_mask=first_bit_mask;
					flag=1;
				}


                                if(past_mask!=first_bit_mask) {
                                        FLUSHES++;
                                        flush_fbl_hk(index->fbl, index);
                                        past_mask=first_bit_mask;
                                }

				
				//insert_to_fbl(index->fbl, currqueue[currcount-1].sax, currqueue[currcount-1].fileposition, first_bit_mask, index);
                                index->total_records++;

			}
			else //if you are building the index with the past algorithm
	        		isax_fbl_index_insert(index, currqueue[currcount-1].sax ,currqueue[currcount-1].fileposition) ;
			
			//printf("\n%s",max.value);
		}*/	

		root_mask_type first_bit_mask = 0x00;
                CREATE_MASK(first_bit_mask, index, currqueue[currcount-1].sax);

                if(flag==0)
		{
 			mybuffer = malloc(sizeof(my_node) * buffer_size);
                	past_mask=first_bit_mask;
			flag=1;
			sizecounter=0;
		}

                if(past_mask!=first_bit_mask ) 
		{
			//printf("\na1111111111111zssssssssssssssssssssssssssssssssssss%d %d", buffer_size, sizecounter);fflush(stdout);
			int xi=0;
			for(xi=0;xi<sizecounter;xi++)
				insert_to_fbl(index->fbl, mybuffer[xi].sax, mybuffer[xi].fileposition, past_mask, index);
	        		//isax_fbl_index_insert(index, mybuffer[xi].sax, mybuffer[xi].fileposition) ;
			//printf("\nb1111111111111zssssssssssssssssssssssssssssssssssss %d",sizecounter);fflush(stdout);
                        FLUSHES++;
                        flush_fbl_hk(index->fbl, index, 1);


			//printf("\nc1111111111111zssssssssssssssssssssssssssssssssssss");fflush(stdout);
                        past_mask=first_bit_mask;
 			mybuffer = malloc(sizeof(my_node) * buffer_size);
			sizecounter=0;
			//printf("\nd1111111111111zssssssssssssssssssssssssssssssssssss");fflush(stdout);
		}

		mybuffer[sizecounter].sax=malloc(sizeof(sax_type)*index->settings->paa_segments);
    		mybuffer[sizecounter].fileposition=malloc(sizeof(file_position_type));

		memcpy(mybuffer[sizecounter].sax, currqueue[currcount-1].sax, sizeof(sax_type)*index->settings->paa_segments);
		memcpy(mybuffer[sizecounter].fileposition, currqueue[currcount-1].fileposition, sizeof(file_position_type));

		sizecounter++;
	        currcount--;	
		

		//flush buffer
		/*if(sizecounter==buffer_size)
		{

			
			COUNT_OUTPUT_TIME_START
			FILE *newfile = fopen(newfilename, "ab");
			
			int j;
                	for (j = 0; j < sizecounter; j++) {
				
                	//printf("\n---- %d", *(mybuffer[j].fileposition));      
       		 	//sax_print(mybuffer[j].sax, index->settings->paa_segments, index->settings->sax_bit_cardinality);
                          	
				//printf("\n-%d-%s %d",j, mybuffer[j].value,*(mybuffer[j]).fileposition);      

                        	fwrite(mybuffer[j].fileposition,
                        	        sizeof(file_position_type), 1,newfile);
                        	fwrite(mybuffer[j].sax,
                        	        sizeof(sax_type), index->settings->paa_segments, newfile);

				free(mybuffer[j].fileposition);
				free(mybuffer[j].sax);
				free(mybuffer[j].value);
                	}
			fclose(newfile);	
			COUNT_OUTPUT_TIME_END
			sizecounter=0;
                	//printf("\n----===========");      
		}*/

		
		//open file that record belonged and insert it as next element in queue
		if(limits[maxpos]>0)
		{
			//	printf("\ndzssssssssssssssssssssssssssssssssssss");fflush(stdout);
       	 		COUNT_CHECKED_NODE();
        		COUNT_CHECKED_NODE();
    			COUNT_INPUT_TIME_START
			FILE * ifile2=files[maxpos];
			size_t r1=fread(posinfile, sizeof(file_position_type),1, ifile2);
               		size_t r2=fread(sax, sizeof(sax_type),index->settings->paa_segments, ifile2);	
			COUNT_INPUT_TIME_END
			
                	//printf("\n---- %d", *(posinfile));      
            		/*my_node jnode;
    			jnode.sax=malloc(sizeof(sax_type)*index->settings->paa_segments);
    			jnode.fileposition=malloc(sizeof(file_position_type));

			memcpy(jnode.sax, sax, sizeof(sax_type)*index->settings->paa_segments);
			memcpy(jnode.fileposition, posinfile, sizeof(file_position_type));

			jnode.value = malloc(sizeof(char) * ((index->settings->paa_segments*index->settings->sax_bit_cardinality)+1));
			ordering_value(jnode.value, sax,index->settings->paa_segments ,index->settings->sax_bit_cardinality);

                	jnode.pos=max.pos;*/
			//printf("\na4444444  -- %d -- %d", r1, r2);fflush(stdout);
       		 	//sax_print(sax, index->settings->paa_segments, index->settings->sax_bit_cardinality);
			//printf("\nb4444444");fflush(stdout);
	
			if(currcount==0)
			{
				//currqueue[currcount].sax=malloc(sizeof(sax_type)*index->settings->paa_segments);
    				//currqueue[currcount].fileposition=malloc(sizeof(file_position_type));
				//currqueue[currcount].value = malloc(sizeof(char) * ((index->settings->paa_segments*index->settings->sax_bit_cardinality)+1));
				
				memcpy(currqueue[currcount].sax, sax, sizeof(sax_type)*index->settings->paa_segments);
				memcpy(currqueue[currcount].fileposition, posinfile, sizeof(file_position_type));
				ordering_value(currqueue[currcount].value, sax,index->settings->paa_segments ,index->settings->sax_bit_cardinality);
				currqueue[currcount].pos=maxpos;
			}
			else
			{
				ordering_value(zivalue, sax,index->settings->paa_segments ,index->settings->sax_bit_cardinality);
				
				int mo;
				for(mo=currcount-1;mo>=0;mo--)
                		{
                        		if(strcmp(zivalue,currqueue[mo].value)>0)
					{
						//currqueue[mo+1].sax=malloc(sizeof(sax_type)*index->settings->paa_segments);
    						//currqueue[mo+1].fileposition=malloc(sizeof(file_position_type));
						//currqueue[mo+1].value = malloc(sizeof(char) * ((index->settings->paa_segments*index->settings->sax_bit_cardinality)+1));


						memcpy(currqueue[mo+1].sax, currqueue[mo].sax, sizeof(sax_type)*index->settings->paa_segments);
						memcpy(currqueue[mo+1].fileposition, currqueue[mo].fileposition, sizeof(file_position_type));
						memcpy(currqueue[mo+1].value, currqueue[mo].value, sizeof(char) * ((index->settings->paa_segments*index->settings->sax_bit_cardinality)+1));
						currqueue[mo+1].pos=currqueue[mo].pos;
					}
                        		else
                        	        	break;
                		}
						//currqueue[mo+1].sax=malloc(sizeof(sax_type)*index->settings->paa_segments);
    						//currqueue[mo+1].fileposition=malloc(sizeof(file_position_type));
						//currqueue[mo+1].value = malloc(sizeof(char) * ((index->settings->paa_segments*index->settings->sax_bit_cardinality)+1));
				memcpy(currqueue[mo+1].sax, sax, sizeof(sax_type)*index->settings->paa_segments);
				memcpy(currqueue[mo+1].fileposition, posinfile, sizeof(file_position_type));
				memcpy(currqueue[mo+1].value, zivalue, sizeof(char) * ((index->settings->paa_segments*index->settings->sax_bit_cardinality)+1));
				currqueue[mo+1].pos=maxpos;
			}	
        	        currcount++;
			limits[maxpos]--;
		}



		//free(max.sax);//=malloc(sizeof(sax_type)*index->settings->paa_segments);
    		//free(max.fileposition);//=malloc(sizeof(file_position_type));
		//free(max.value);// = malloc(sizeof(char) * ((index->settings->paa_segments*index->settings->sax_bit_cardinality)+1));
		//free(max);
		

	}


	//if(totalpasses==currpass+1)//if you are building the index with the new algorithm
	//{		
        //	FLUSHES++;
        //        flush_fbl_hk(index->fbl, index);
	//}
	//printf("\n Point 2");fflush(stdout);
	//flush remaining data
	/*if(sizecounter>0)
        {
                COUNT_OUTPUT_TIME_START
                FILE *newfile = fopen(newfilename, "ab");
                int j;
                for (j = 0; j < sizecounter; j++) {
                	fwrite(mybuffer[j].fileposition,
                        	sizeof(file_position_type), 1,newfile);
                        fwrite(mybuffer[j].sax,
                                sizeof(sax_type), index->settings->paa_segments, newfile);
                }
                fclose(newfile);
                COUNT_OUTPUT_TIME_END
		sizecounter=0;
        }*/


	for(i=0;i<fileno;i++)
	{
    		free(currqueue[i].sax);
    		free(currqueue[i].fileposition);
		free(currqueue[i].value);
		fclose(files[i]);
		unlink(filenames[i]);
		free(filenames[i]);
	}
	//printf("\n Point 3");fflush(stdout);
	free(files);
	free(currqueue);
	free(mybuffer);
	free(newfilename);
	free(zivalue);
	
	//printf("\n Point 4");fflush(stdout);
	//printf("\n    ==> bgenerating %s.%d.%d",filename,currpass+1,currrun);
   }
   else
   {
	//printf("\n mergind startrun:%d endrun:%d in  pass %d",startrun,endrun,currpass);

	int newruns=0;
	int paststep=0;
     	for(i=startrun;i<endrun;i=i+buffer_size)
	{
		newruns++;
		paststep=i+buffer_size-1;
		if(endrun<i+buffer_size-1)
	  		mergeandbuildindex(i, endrun,index, currpass,newruns, totalpasses, only_merge);
		else
	  		mergeandbuildindex(i, i+buffer_size-1,index, currpass,newruns, totalpasses, only_merge);
	}
        
	if(endrun>paststep)
	{
		//printf("\n zzzi:%d runs:%d ",endrun,paststep);
		newruns++;
  		mergeandbuildindex(paststep+1, endrun, index, currpass, newruns, totalpasses, only_merge);
	}

	currpass++;
	mergeandbuildindex(1, newruns, index,  currpass, 1, totalpasses, only_merge);

		
   }
   free(posinfile);
   free(sax);    
}



void isax_index_binary_file(const char *ifilename, int ts_num,
                            isax_index *index)
{
	fprintf(stderr, ">>> Indexing: %s\n", ifilename);
    FILE * ifile;
    COUNT_INPUT_TIME_START
	ifile = fopen (ifilename,"rb");
    COUNT_INPUT_TIME_END
    if (ifile == NULL) {
        fprintf(stderr, "File %s not found!\n", ifilename);
        exit(-1);
    }
    fseek(ifile, 0L, SEEK_END);
    file_position_type sz = (file_position_type) ftell(ifile);
    file_position_type total_records = sz/index->settings->ts_byte_size;
    fseek(ifile, 0L, SEEK_SET);
    if (total_records < ts_num) {
        fprintf(stderr, "File %s has only %llu records!\n", ifilename, total_records);
        exit(-1);
    }


    int ts_loaded = 0;

    ts_type * ts = malloc(sizeof(ts_type) * index->settings->timeseries_size);
    sax_type * sax = malloc(sizeof(sax_type) * index->settings->paa_segments);
    file_position_type * pos = malloc(sizeof(file_position_type));

    index->settings->raw_filename = malloc(256);
    strcpy(index->settings->raw_filename, ifilename);

#ifdef BENCHMARK
    int percentage = (int) (ts_num / (file_position_type) 100);
#endif

    while (ts_loaded<ts_num)
    {
#ifndef DEBUG
#if VERBOSE_LEVEL == 2
        printf("\r\x1b[32mLoading: \x1b[36m%d\x1b[0m",(ts_loaded + 1));
#endif
#endif
        *pos = ftell(ifile);
        COUNT_CHECKED_NODE();
        COUNT_INPUT_TIME_START
        fread(ts, sizeof(ts_type), index->settings->timeseries_size, ifile);
        COUNT_INPUT_TIME_END

        if(sax_from_ts(ts, sax, index->settings->ts_values_per_paa_segment,
                       index->settings->paa_segments, index->settings->sax_alphabet_cardinality,
                       index->settings->sax_bit_cardinality) == SUCCESS)
        {
#ifdef CLUSTERED
		    root_mask_type first_bit_mask = 0x00;
		    CREATE_MASK(first_bit_mask, index, sax);
            char* pfilename = malloc(255);
            snprintf(pfilename, 255, "%s.%llu",index->settings->raw_filename,first_bit_mask);
            FILE *pfile = fopen(pfilename, "a+");
            *pos = ftell(pfile);
            COUNT_CHECKED_NODE();
            fwrite(ts, sizeof(ts_type), index->settings->timeseries_size, pfile);
            fclose(pfile);
            free(pfilename);
#endif
            isax_fbl_index_insert(index, sax, pos);
            ts_loaded++;

           /* if(percentage == 0) {
			    float distance = 0;
		    	printf("%d ", ts_loaded);
			    PRINT_STATS(distance);
                fflush(stdout);
		    }
		    else if(ts_loaded % percentage == 0)
		    {
		    	float distance = 0;
		    	printf("%d ", ts_loaded);
                PRINT_STATS(distance)
                
                fflush(stdout);
		    }*/
        }
        else
        {
            fprintf(stderr, "error: cannot insert record in index, since sax representation\
                    failed to be created");
        }
	}
    free(ts);
    free(sax);
    free(pos);
    COUNT_INPUT_TIME_START
	fclose(ifile);
    COUNT_INPUT_TIME_END
    fprintf(stderr, ">>> Finished indexing\n");
}



/*
 *
** Using documented GCC type unsigned __int128 instead of undocumented
** obsolescent typedef name __uint128_t.  Works with GCC 4.7.1 but not
** GCC 4.1.2 (but __uint128_t works with GCC 4.1.2) on Mac OS X 10.7.4.
*/
typedef unsigned __int128 uint128_t;

/*      UINT64_MAX 18446744073709551615ULL */
#define P10_UINT64 10000000000000000000ULL   /* 19 zeroes */
#define E10_UINT64 19

#define STRINGIZER(x)   # x
#define TO_STRING(x)    STRINGIZER(x)

static int print_u128_u(uint128_t u128)
{
    int rc;
    if (u128 > UINT64_MAX)
    {
        uint128_t leading  = u128 / P10_UINT64;
        uint64_t  trailing = u128 % P10_UINT64;
        rc = print_u128_u(leading);
        rc += printf("%." TO_STRING(E10_UINT64) PRIu64, trailing);
    }
    else
    {
        uint64_t u64 = u128;
        rc = printf("%" PRIu64, u64);
    }
    return rc;
}

typedef struct
{
    uint64_t hi;
    uint64_t lo;
} my_uint128_t;

typedef struct {
	my_uint128_t sax;
	file_position_type pos;
} sax_vector;

/** Return returns 1 if a>b, -1 if b>a, 0 if a == b */
int
uint128_compare (const void * a_or, const void *b_or)
{
	my_uint128_t *a = (my_uint128_t*) &((sax_vector*)a_or)->sax;
	my_uint128_t *b = (my_uint128_t*) &((sax_vector*)b_or)->sax;

    if (a->hi > b->hi) return -1;
    if (a->hi < b->hi) return 1;
    if (a->lo > b->lo) return -1;
    if (a->lo < b->lo) return 1;
    return 0;
}

/*
void isax_sorted_index_binary_file(const char *ifilename, int ts_num, isax_index *index)
{
	fprintf(stderr, ">>> Indexing: %s\n", ifilename);
    FILE * ifile;
    COUNT_INPUT_TIME_START
	ifile = fopen (ifilename,"rb");
    COUNT_INPUT_TIME_END
    if (ifile == NULL) {
        fprintf(stderr, "File %s not found!\n", ifilename);
        exit(-1);
    }
    fseek(ifile, 0L, SEEK_END);
    file_position_type sz = (file_position_type) ftell(ifile);
    file_position_type total_records = sz/index->settings->ts_byte_size;
    fseek(ifile, 0L, SEEK_SET);
    if (total_records < ts_num) {
        fprintf(stderr, "File %s has only %llu records!\n", ifilename, total_records);
        exit(-1);
    }


    int ts_loaded = 0;

    ts_type * ts = malloc(sizeof(ts_type) * index->settings->timeseries_size);
    sax_vector * sax_vectors = malloc(sizeof(sax_vector) * ts_num);

    index->settings->raw_filename = malloc(256);
    strcpy(index->settings->raw_filename, ifilename);

#ifdef BENCHMARK
    int percentage = (int) (ts_num / (file_position_type) 100);
#endif

    while (ts_loaded<ts_num)
    {
#ifndef DEBUG
#if VERBOSE_LEVEL == 2
        printf("\r\x1b[32mLoading: \x1b[36m%d\x1b[0m",(ts_loaded + 1));
#endif
#endif
        file_position_type pos = ftell(ifile);
        COUNT_INPUT_TIME_START
        fread(ts, sizeof(ts_type), index->settings->timeseries_size, ifile);
        COUNT_INPUT_TIME_END

        if(sax_from_ts(ts, &(sax_vectors[ts_loaded].sax), index->settings->ts_values_per_paa_segment,
                       index->settings->paa_segments, index->settings->sax_alphabet_cardinality,
                       index->settings->sax_bit_cardinality) == SUCCESS)
        {
#ifdef CLUSTERED
		    root_mask_type first_bit_mask = 0x00;
		    CREATE_MASK(first_bit_mask, index, sax);
            char* pfilename = malloc(255);
            snprintf(pfilename, 255, "%s.%llu",index->settings->raw_filename,first_bit_mask);
            FILE *pfile = fopen(pfilename, "a+");
            *pos = ftell(pfile);
            fwrite(ts, sizeof(ts_type), index->settings->timeseries_size, pfile);
            fclose(pfile);
            free(pfilename);
#endif

            sax_vectors[ts_loaded].pos = pos;

            ts_loaded++;

            if(percentage == 0) {
			    float distance = 0;
			    PRINT_STATS(distance);
		    }
		    else if(ts_loaded % percentage == 0)
		    {
		    	float distance = 0;
		    	PRINT_STATS(distance)
		    }
        }
        else
        {
            fprintf(stderr, "error: cannot insert record in index, since sax representation\
                    failed to be created");
        }
	}

    qsort(sax_vectors, sizeof(sax_vector), ts_num, uint128_compare);

    int i;
    for(i=0; i<ts_num; i++) {
    	isax_fbl_index_insert(index, (sax_type *) &(sax_vectors[i].sax), &(sax_vectors[i].pos));
    }

    free(ts);
    free(sax_vectors);
    COUNT_INPUT_TIME_START
	fclose(ifile);
    COUNT_INPUT_TIME_END
    fprintf(stderr, ">>> Finished indexing\n");

}

void isax_merge_sorted_index_binary_file(const char *ifilename, int ts_num, isax_index *index)
{
	fprintf(stderr, ">>> Indexing: %s\n", ifilename);
    FILE * ifile;
    COUNT_INPUT_TIME_START
	ifile = fopen (ifilename,"rb");
    COUNT_INPUT_TIME_END
    if (ifile == NULL) {
        fprintf(stderr, "File %s not found!\n", ifilename);
        exit(-1);
    }
    fseek(ifile, 0L, SEEK_END);
    file_position_type sz = (file_position_type) ftell(ifile);
    file_position_type total_records = sz/index->settings->ts_byte_size;
    fseek(ifile, 0L, SEEK_SET);
    if (total_records < ts_num) {
        fprintf(stderr, "File %s has only %llu records!\n", ifilename, total_records);
        exit(-1);
    }


    int ts_loaded = 0;

    ts_type * ts = malloc(sizeof(ts_type) * index->settings->timeseries_size);
    sax_vector * sax_vectors = malloc(sizeof(sax_vector) * ts_num);

    index->settings->raw_filename = malloc(256);
    strcpy(index->settings->raw_filename, ifilename);

#ifdef BENCHMARK
    int percentage = (int) (ts_num / (file_position_type) 100);
#endif

    while (ts_loaded<ts_num)
    {
#ifndef DEBUG
#if VERBOSE_LEVEL == 2
        printf("\r\x1b[32mLoading: \x1b[36m%d\x1b[0m",(ts_loaded + 1));
#endif
#endif
        file_position_type pos = ftell(ifile);
        COUNT_INPUT_TIME_START
        fread(ts, sizeof(ts_type), index->settings->timeseries_size, ifile);
        COUNT_INPUT_TIME_END

        if(sax_from_ts(ts, &(sax_vectors[ts_loaded].sax), index->settings->ts_values_per_paa_segment,
                       index->settings->paa_segments, index->settings->sax_alphabet_cardinality,
                       index->settings->sax_bit_cardinality) == SUCCESS)
        {
#ifdef CLUSTERED
		    root_mask_type first_bit_mask = 0x00;
		    CREATE_MASK(first_bit_mask, index, sax);
            char* pfilename = malloc(255);
            snprintf(pfilename, 255, "%s.%llu",index->settings->raw_filename,first_bit_mask);
            FILE *pfile = fopen(pfilename, "a+");
            *pos = ftell(pfile);
            fwrite(ts, sizeof(ts_type), index->settings->timeseries_size, pfile);
            fclose(pfile);
            free(pfilename);
#endif

            sax_vectors[ts_loaded].pos = pos;

            ts_loaded++;

            if(percentage == 0) {
			    float distance = 0;
			    PRINT_STATS(distance);
		    }
		    else if(ts_loaded % percentage == 0)
		    {
		    	float distance = 0;
		    	PRINT_STATS(distance)
		    }
        }
        else
        {
            fprintf(stderr, "error: cannot insert record in index, since sax representation\
                    failed to be created");
        }
	}

    //qsort(sax_vectors, sizeof(sax_vector), ts_num, uint128_compare);

    int j;
	unsigned long i;
	int segment;
	sax_type *removals = malloc(sizeof(sax_type) * index->settings->paa_segments);
	for(i=0; i<index->settings->paa_segments; i++) {
		removals[i] = 0;
	}


	int prev_best_segment = -1;
	float current_utilization = 0;
	sax_vector *new_dataset = malloc(sizeof(sax_vector) * ts_num);

	do {
		// If I remove 1 bit from a segment what is the average page utilization?
		int best_segment = prev_best_segment+1;
		if(best_segment >= index->settings->paa_segments) {
			best_segment = 0;
		}
		prev_best_segment = best_segment;
		int best_segment_pages = ts_num;
		float best_segment_average_utilization = 0;
		float best_segment_min_page_utilization = 0;
		char updated = 0;

		//printf("Starting with: %d\n", best_segment);
		for(segment=0; segment<index->settings->paa_segments; segment++) {

			for(i=0; i<ts_num; i++) {
				memcpy(&new_dataset[i], &sax_vectors[i], sizeof(sax_vector));
				sax_type *sax_cpy = (sax_type *) &(new_dataset[i].sax);
				int k;
				for(k=0; k<index->settings->paa_segments; k++) {
					if(k == segment) {
						sax_cpy[k] = (sax_cpy[k] >> (removals[k]+1));
					}
					else {
						sax_cpy[k] = (sax_cpy[k] >> removals[k]);
					}
				}
			}

			qsort (new_dataset, ts_num, sizeof(sax_vector), uint128_compare);

			sax_vector prev = new_dataset[0];
			int counter = 1;
			int pid = 1;
			float page_utilization = 0;
			float min_page_utilization = 10000000000;
			for(i=1; i<ts_num; i++) {
				uint128_t *prev_sax = &(prev.sax);
				uint128_t *this_sax = &(new_dataset[i].sax);
				if(*prev_sax != *this_sax) {
					float d = (float)(counter * index->settings->ts_byte_size) / 4096;
					page_utilization += d;
					counter = 1;
					pid++;
					if(page_utilization < min_page_utilization) {
						min_page_utilization = page_utilization;
					}
				}
				else {
					counter++;
				}
				prev = new_dataset[i];
			}

			float d = (float)(counter * index->settings->ts_byte_size) / (float) 4096;
			page_utilization += d;

			if(pid < best_segment_pages || (pid == best_segment_pages && segment == prev_best_segment))
			{
				updated = 1;
				best_segment = segment;
				best_segment_pages = pid;
				best_segment_average_utilization = page_utilization / (float) pid;
				best_segment_min_page_utilization = min_page_utilization;
			}


			if(!updated) {
				best_segment_pages = pid;
				best_segment_average_utilization = page_utilization / (float) pid;
				best_segment_min_page_utilization = min_page_utilization;
			}

			//printf("%d has %d\n", segment, pid);
		}

		//printf("Chosing: %d (%d pages %lf utilized on average, min: %lf)\n", best_segment, best_segment_pages, best_segment_average_utilization, best_segment_min_page_utilization);
		current_utilization = best_segment_average_utilization;
		removals[best_segment]++;
	} while(current_utilization < 1.2);



    for(i=0; i<ts_num; i++) {
    	int j = new_dataset[i].pos / index->settings->ts_byte_size;
    	isax_fbl_index_insert(index, (sax_type *) &(sax_vectors[j].sax), &(sax_vectors[j].pos));
    }


    free(new_dataset);
    free(removals);

    free(ts);
    free(sax_vectors);
    COUNT_INPUT_TIME_START
	fclose(ifile);
    COUNT_INPUT_TIME_END
    fprintf(stderr, ">>> Finished indexing\n");

}*/
