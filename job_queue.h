#ifndef JOB_QUEUE_H
#define JOB_QUEUE_H

#include <pthread.h>

struct job_queue {
  // Using a void pointer for the buffer allows the job queue to store any type of data.
  void **buffer;

  // The maximum size of the job queue.
  int capacity;

  // The current number of elements in the job queue.
  int size;

  // Index of the first element in the job queue.
  int first;

  // Index of where the next element in the job queue ((first + size) % capacity).
  int next;

  // Flag to indicate if the job queue is dying.
  int dying;

  // Flag to indicate if there are threads waiting.
  int waiting;

  // Mutex to protect access to the job queue.
  pthread_mutex_t mutex;

  // Condition variable to signal when the job queue is not empty. Pop is waiting here when the queue is empty.
  pthread_cond_t not_empty;

  // Condition variable to signal when the job queue is not full. Push is waiting here when the queue is full.
  pthread_cond_t not_full;

  // Condition variable to signal when the job queue is empty. Destroy is waiting here when the queue is not empty.
  pthread_cond_t empty;
};

// Initialise a job queue with the given capacity.  The queue starts out
// empty.  Returns non-zero on error.
int job_queue_init(struct job_queue *job_queue, int capacity);

// Destroy the job queue.  Blocks until the queue is empty before it
// is destroyed.
int job_queue_destroy(struct job_queue *job_queue);

// Push an element onto the end of the job queue.  Blocks if the
// job_queue is full (its size is equal to its capacity).  Returns
// non-zero on error.  It is an error to push a job onto a queue that
// has been destroyed.
int job_queue_push(struct job_queue *job_queue, void *data);

// Pop an element from the front of the job queue.  Blocks if the
// job_queue contains zero elements.  Returns non-zero on error.  If
// job_queue_destroy() has been called (possibly after the call to
// job_queue_pop() blocked), this function will return -1.
int job_queue_pop(struct job_queue *job_queue, void **data);

#endif
