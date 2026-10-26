// Setting _DEFAULT_SOURCE is necessary to activate visibility of
// certain header file contents on GNU/Linux systems.
#define _DEFAULT_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <stdint.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fts.h>

#include "job_queue.h"

pthread_mutex_t stdout_mutex = PTHREAD_MUTEX_INITIALIZER;

// err.h contains various nonstandard BSD extensions, but they are
// very handy.
#include <err.h>

#include "histogram.h"

//Number of bytes that are processed before it merges into the global histogram and redraws.
#define UPDATE_INTERVAL 100000

//Chunk size for reading files
#define READ_CHUNK 65536

static int global_histogram[8] = { 0 };

static void flush_local(int local_histogram[8]) {
  pthread_mutex_lock(&stdout_mutex); 
  merge_histogram(local_histogram, global_histogram);
  print_histogram(global_histogram);
  fflush(stdout);
  pthread_mutex_unlock(&stdout_mutex);
}

static int fhistogram(char const *path) {
  FILE *f = fopen(path, "r");

  if (f == NULL) {
    pthread_mutex_lock(&stdout_mutex);
    fflush(stdout);
    warn("failed top open %s", path);
    pthread_mutex_unlock(&stdout_mutex);
    return -1;
  }

  int local_histogram[8] = { 0 };
  unsigned char buf[READ_CHUNK];
  size_t since_update = 0;
  size_t n; 

  while ((n = fread(buf, 1, sizeof(buf), f)) > 0) {
    for (size_t j = 0; j < n; j++) {
      update_histogram(local_histogram, buf[j]);
    }
    since_update += n;
    if (since_update >= UPDATE_INTERVAL) {
      flush_local(local_histogram);
      since_update = 0;
    }
  }

  fclose(f);

  flush_local(local_histogram);
  return 0; 
}

static void* worker(void *arg) {
  struct job_queue *jq = arg;

  while (1) {
    char *path; 
    if (job_queue_pop(jq, (void**)&path) == 0) {
      fhistogram(path);
      free(path);
    } else {
      break; 
    }
  }

  return NULL;
}

int main(int argc, char * const *argv) {
  if (argc < 2) {
    err(1, "usage: [-n INT] paths...");
    exit(1);
  }

  int num_threads = 1;
  char * const *paths = &argv[1];

  if (argc > 3 && strcmp(argv[1], "-n") == 0) {
    // Since atoi() simply returns zero on syntax errors, we cannot
    // distinguish between the user entering a zero, or some
    // non-numeric garbage.  In fact, we cannot even tell whether the
    // given option is suffixed by garbage, i.e. '123foo' returns
    // '123'.  A more robust solution would use strtol(), but its
    // interface is more complicated, so here we are.
    num_threads = atoi(argv[2]);

    if (num_threads < 1) {
      err(1, "invalid thread count: %s", argv[2]);
    }

    paths = &argv[3];
  } else {
    paths = &argv[1];
  }

struct job_queue jq;
if (job_queue_init(&jq, 64) != 0) {
  err(1, "job_queue_init() failed");
}

pthread_t *threads = calloc(num_threads, sizeof(pthread_t));
for (int i = 0; i < num_threads; i++) {
  if (pthread_create(&threads[i], NULL, &worker, &jq) != 0) {
    err(1, "pthread_create() failed");
  }
}

  // FTS_LOGICAL = follow symbolic links
  // FTS_NOCHDIR = do not change the working directory of the process
  //
  // (These are not particularly important distinctions for our simple
  // uses.)
  int fts_options = FTS_LOGICAL | FTS_NOCHDIR;

  FTS *ftsp;
  if ((ftsp = fts_open(paths, fts_options, NULL)) == NULL) {
    err(1, "fts_open() failed");
    return -1;
  }

  FTSENT *p;
  while ((p = fts_read(ftsp)) != NULL) {
    switch (p->fts_info) {
    case FTS_D:
      break;
    case FTS_F:
      job_queue_push(&jq, strdup(p->fts_path));
      break;
    default:
      break;
    }
  }

  fts_close(ftsp);

job_queue_destroy(&jq); //stopping job quwuw

for (int i = 0; i < num_threads; i++) {
  if (pthread_join(threads[i], NULL) != 0) {
    err(1, "pthread_join() failed");
  }
}
  free(threads);

  move_lines(9);

  return 0;
}
