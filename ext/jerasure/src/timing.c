// Timing measurement utilities implementation.

#include "timing.h"
#include <stddef.h>

void
timing_set(
  struct timing * t)
{
#ifdef USE_CLOCK
  t->clock = clock();
#else
  gettimeofday(&t->tv, NULL);
#endif
}

double
timing_get(
  struct timing * t)
{
#ifdef USE_CLOCK
  // The clock_t type is an "arithmetic type", which could be
  // integral, double, long double, or others.
  //
  // Add 0.0 to make it a double or long double, then divide (in
  // double or long double), then convert to double for our purposes.
  return (double) ((t->clock + 0.0) / CLOCKS_PER_SEC);
#else
  return (double) t->tv.tv_sec + ((double) t->tv.tv_usec) / 1000000.0;
#endif
}

double
timing_now()
{
#ifdef USE_CLOCK
  return (double) ((clock() + 0.0) / CLOCKS_PER_SEC);
#else
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return (double) tv.tv_sec + ((double) tv.tv_usec) / 1000000.0;
#endif
}

double
timing_delta(
  struct timing * t1,
  struct timing * t2)
{
#ifdef USE_CLOCK
  // The clock_t type is an "arithmetic type", which could be
  // integral, double, long double, or others.
  //
  // Subtract first, resulting in another clock_t, then add 0.0 to
  // make it a double or long double, then divide (in double or long
  // double), then convert to double for our purposes.
  return (double) (((t2->clock - t1->clock) + 0.0) / CLOCKS_PER_SEC);
#else
  double const d2 = (double) t2->tv.tv_sec + ((double) t2->tv.tv_usec) / 1000000.0;
  double const d1 = (double) t1->tv.tv_sec + ((double) t1->tv.tv_usec) / 1000000.0;
  return d2 - d1;
#endif
}
