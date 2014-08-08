// Timing measurement utilities.

#ifndef JERASURE_INCLUDED__TIMING_H
#define JERASURE_INCLUDED__TIMING_H

// Define USE_CLOCK to use clock(). Otherwise use gettimeofday().
#define USE_CLOCK

#ifdef USE_CLOCK
#include <time.h>
#else
#include <sys/time.h>
#endif

struct timing {
#ifdef USE_CLOCK
  clock_t clock;
#else
  struct timeval tv;
#endif
};

// Get the current time as a double in seconds.
double
timing_now(
  void);

// Set *t to the current time.
void
timing_set(
  struct timing * t);

// Get *t as a double in seconds.
double
timing_get(
  struct timing * t);

// Return *t2 - *t1 as a double in seconds.
double
timing_delta(
  struct timing * t1,
  struct timing * t2);
#endif
