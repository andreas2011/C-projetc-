#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <pthread.h>
#include <sys/time.h>
#include "sbuffer.h"


/*
 * All data that can be stored in the sbuffer should be encapsulated in a
 * structure, this structure can then also hold extra info needed for your implementation
 */
struct sbuffer_data {
    sensor_data_t data;
};

typedef struct sbuffer_node {
  struct sbuffer_node * next;
  sbuffer_data_t element;
} sbuffer_node_t;

struct sbuffer {
  sbuffer_node_t * head;
  sbuffer_node_t * tail;
  pthread_cond_t cond;
  pthread_mutex_t mutex;
};	


int sbuffer_init(sbuffer_t ** buffer)
{
  *buffer = malloc(sizeof(sbuffer_t));
  if (*buffer == NULL) return SBUFFER_FAILURE;
  (*buffer)->head = NULL;
  (*buffer)->tail = NULL;
  pthread_mutex_init(&(*buffer)->mutex, NULL);
  pthread_cond_init(&(*buffer)->cond, NULL);
  return SBUFFER_SUCCESS; 
}


int sbuffer_free(sbuffer_t ** buffer)
{
  
  if ((buffer==NULL) || (*buffer==NULL)) 
  {
    return SBUFFER_FAILURE;
  } 
  while ( (*buffer)->head )
  {
	sbuffer_node_t * dummy;
    dummy = (*buffer)->head;
    (*buffer)->head = (*buffer)->head->next;
    free(dummy);
  }
  pthread_mutex_destroy(&(*buffer)->mutex);
  pthread_cond_destroy(&(*buffer)->cond);
  free(*buffer);
  *buffer = NULL;
  return SBUFFER_SUCCESS;		
}

void calculate_outtime(struct timespec *outtime)
{
	struct timeval now;
	gettimeofday(&now, NULL);
	outtime->tv_sec = now.tv_sec + TIMEOUT * 2;
	outtime->tv_nsec = now.tv_usec * 1000;
}

int sbuffer_remove(sbuffer_t * buffer,sensor_data_t * data)
{
  sbuffer_node_t * dummy;
  if (buffer == NULL) return SBUFFER_FAILURE;
  pthread_mutex_lock(&buffer->mutex);
  while (buffer->head == NULL){
	  struct timespec outtime;
	  calculate_outtime(&outtime);
	  if (pthread_cond_timedwait(&buffer->cond, &buffer->mutex, &outtime) == ETIMEDOUT){
		  pthread_mutex_unlock(&buffer->mutex);
		  return SBUFFER_NO_DATA;
	  }
  }
  *data = buffer->head->element.data;
  dummy = buffer->head;
  if (buffer->head == buffer->tail) // buffer has only one node
  {
    buffer->head = buffer->tail = NULL; 
  }
  else  // buffer has many nodes empty
  {
    buffer->head = buffer->head->next;
  }
  free(dummy);
  pthread_mutex_unlock(&buffer->mutex);
  return SBUFFER_SUCCESS;
}


int sbuffer_insert(sbuffer_t * buffer, sensor_data_t * data)
{
  sbuffer_node_t * dummy;
  if (buffer == NULL) return SBUFFER_FAILURE;
  pthread_mutex_lock(&buffer->mutex);
  dummy = malloc(sizeof(sbuffer_node_t));
  if (dummy == NULL){
	  pthread_mutex_unlock(&buffer->mutex);
	  return SBUFFER_FAILURE;
  }
  dummy->element.data = *data;
  dummy->next = NULL;
  if (buffer->tail == NULL) // buffer empty (buffer->head should also be NULL
  {
    buffer->head = buffer->tail = dummy;
  } 
  else // buffer not empty
  {
    buffer->tail->next = dummy;
    buffer->tail = buffer->tail->next; 
  }
  pthread_cond_signal(&buffer->cond);
  pthread_mutex_unlock(&buffer->mutex);
  return SBUFFER_SUCCESS;
}





