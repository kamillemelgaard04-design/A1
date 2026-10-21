#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

#include "job_queue.h"

int job_queue_init(struct job_queue *job_queue, int capacity) {
  
  // If job queue is NULL or capacity is less than or equal to 0, return error.
  if (job_queue == NULL || capacity <= 0) {
    return -1;
  }

  // Allocate memory for the buffer to hold the jobs. The buffer is an array of void pointers, 
  // allowing it to store any type of data.
  job_queue->buffer = malloc(sizeof(void *) * capacity);
  
  // If the buffer allocation fails, return error.
  if (job_queue->buffer == NULL) {
    return -1;
  }

  // Initialize the job queue attributes.
  job_queue->capacity = capacity;
  job_queue->size = 0;
  job_queue->first = 0;
  job_queue->next = 0;
  job_queue->dying = 0;
  job_queue->waiting = 0;

  // Initialize the mutex and condition variables. The mutex is used to protect access to the
  // job queue, while the condition variables are used to signal when the job queue is not full, 
  // not empty, or empty.
  pthread_mutex_init(&job_queue->mutex, NULL);
  pthread_cond_init(&job_queue->not_empty, NULL);
  pthread_cond_init(&job_queue->not_full, NULL);
  pthread_cond_init(&job_queue->empty, NULL);

  return 0;
}

int job_queue_destroy(struct job_queue *job_queue) {
  if (job_queue == NULL) {
    return -1;
  }

  // Lock the mutex to ensure that only one thread can access the job queue at a time.
  pthread_mutex_lock(&job_queue->mutex);
  
  // Wait until all queued jobs have been popped, so no work is lost.
  while (job_queue->size > 0) {
    pthread_cond_wait(&job_queue->empty, &job_queue->mutex);
  }

  // Mark the queue as closed. From now on pop returns -1 and push fails.
  job_queue->dying = 1;

  // Wake all threads blocked in pop (and push) so they can see the flag.
  pthread_cond_broadcast(&job_queue->not_empty);
  pthread_cond_broadcast(&job_queue->not_full);

  // The queue is now empty, so nobody will touch the buffer anymore. 
  // Free the buffer to avoid any leaks.
  free(job_queue->buffer);

  // Set the buffer pointer to NULL to avoid dangling pointer issues.
  job_queue->buffer = NULL;

  // The mutex and condition variables are deliberately NOT destroyed, since
  // workers that were busy with a job may call pop() again later and
  // must still be able to lock the mutex and see that the queue is closed.
  pthread_mutex_unlock(&job_queue->mutex);
  return 0;
}

int job_queue_push(struct job_queue *job_queue, void *data) {
  if (job_queue == NULL) {
    return -1;
  }

  // Lock the mutex to ensure that only one thread can access the job queue at a time.
  pthread_mutex_lock(&job_queue->mutex);

  // While the job queue is full and the job queue isn't dying, wait on the not_full condition variable.
  while (job_queue->size == job_queue->capacity && !job_queue->dying) {
    
    // Free the mutex and wait for the not_full condition variable to be signaled. 
    pthread_cond_wait(&job_queue->not_full, &job_queue->mutex);
  }

  // If the job queue is dying, unlock the mutex and return error.
  if (job_queue->dying) {
    pthread_mutex_unlock(&job_queue->mutex);
    return -1;
  }

  // Add the new job to the buffer at the next index, and update the next index and size of the job queue.
  job_queue->buffer[job_queue->next] = data;
  job_queue->next = (job_queue->next + 1) % job_queue->capacity;
  job_queue->size++;

  // Signal the not_empty condition variable to wake up any threads waiting to pop a job from the queue.
  pthread_cond_signal(&job_queue->not_empty);
  pthread_mutex_unlock(&job_queue->mutex);
  return 0;
}

int job_queue_pop(struct job_queue *job_queue, void **data) {
  if (job_queue == NULL || data == NULL) {
    return -1;
  }

  pthread_mutex_lock(&job_queue->mutex);

  // While the job queue is empty and the job queue isn't dying, 
  // wait on the not_empty condition variable.
  while (job_queue->size == 0 && !job_queue->dying) {
    pthread_cond_wait(&job_queue->not_empty, &job_queue->mutex);
  }

  // If the job queue is dying, unlock the mutex and return error.
  if (job_queue->size == 0) {
    pthread_mutex_unlock(&job_queue->mutex);
    return -1;
  }

  // Take the oldest job from the buffer and update the first index and size of the job queue.
  *data = job_queue->buffer[job_queue->first];
  job_queue->first = (job_queue->first + 1) % job_queue->capacity;
  job_queue->size--;

  // Signal the not_full condition variable to wake up any threads waiting to push a job onto the queue.
  pthread_cond_signal(&job_queue->not_full);

  // If the queue is now empty, signal the empty condition variable to wake up any threads waiting for the queue to be empty.
  if (job_queue->size == 0) {
    pthread_cond_signal(&job_queue->empty);
  }

  // Unlock the mutex and return success.
  pthread_mutex_unlock(&job_queue->mutex);
  return 0;
}
