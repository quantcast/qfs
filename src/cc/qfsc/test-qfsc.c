#include <stdio.h>
#include <stdarg.h>
#include <stdint.h>
#include <string.h>
#include <fcntl.h>

#include "qfs.h"


// Inspired by minunit
#define check(test, format, ...) \
    do { \
        if (!(test)) { \
        snprintf(mbuf, sizeof(mbuf), "expect " #test ": " format, ##__VA_ARGS__); \
        return mbuf; } printf(mbuf, sizeof(mbuf), #test ": " format, ##__VA_ARGS__); \
    } while (0)
#define run(test) \
    do { \
        char *message = test(); tests_run++; \
        if(message) { \
            printf("fail\t" #test "\n** %s\n", message); return message; \
        } else printf("ok\t" #test "\n"); \
    } while(0)
#define check_qfs_call(result) \
    do { \
        int r; \
        char msg[128]; \
        check(0 <= (r = (result)), "%s\n", qfs_strerror(r, msg, sizeof(msg))); \
    } while(0);

// Buffer to format all error messages
static char mbuf[4096];

static int tests_run;
static char* metaserver_host = "localhost";
static int metaserver_port = 20000;

// Global file system and file descriptor under test.
struct QFS* qfs;
int fd;

char* testdata = "qwerasdf1234567890";

static char* test_qfs_connect() {
  qfs = qfs_connect(metaserver_host, metaserver_port);
  check(qfs, "qfs should be non null");
  return 0;
}

static char* test_get_metaserver_location() {
  char buf[5];
  const int blen = (int)(sizeof(buf)/sizeof(buf[0]) - 1);
  int n = qfs_get_metaserver_location(qfs, buf, sizeof(buf));
  int len, hlen;
  char loc[4096];
  char bbuf[4096];

  buf[blen] = 0;
  len = (int)strlen(buf);
  hlen = (int)strlen(metaserver_host);

  check(n > sizeof(buf), "n should larger than sizeof(buf)");
  check(strncmp(metaserver_host, buf, hlen < len ? hlen : len) == 0,
    "partial string should be written");

  snprintf(loc, sizeof(loc), "%s:%d", metaserver_host, metaserver_port);

  n = qfs_get_metaserver_location(qfs, bbuf, sizeof(bbuf));

  check(n == strlen(bbuf), "full location should have been written out");
  check(strcmp(loc, bbuf) == 0,
    "location should be correct: %s, got %s", bbuf, loc);

  return 0;
}

static char* test_qfs_user_functions() {
  uint32_t uid = qfs_getuid(qfs);

  check(uid > 0, "uid should be greater than 0"); // or something.

  return 0;
}

static char* test_qfs_cleanup() {
  // Really just cleans things up, but runs through several qfs ops doing so.

  if(qfs_exists(qfs, "/unit-test")) {
    check_qfs_call(qfs_rmdirs(qfs, "/unit-test"));
  }

  return 0;
}

static char* test_qfs_release() {
  qfs_release(qfs);
  return 0;
}

static char* test_qfs_mkdir() {
  check_qfs_call(qfs_mkdir(qfs, "/unit-test", 0755));
  return 0;
}

static char* test_qfs_mkdirs() {
  check_qfs_call(qfs_mkdirs(qfs, "/unit-test/a/b", 0755));
  return 0;
}

static char* test_qfs_cd() {
  check_qfs_call(qfs_cd(qfs, "/unit-test"));
  return 0;
}

static char* test_qfs_getwd() {
  char cwd[QFS_MAX_FILENAME_LEN];
  check(qfs_getwd(qfs, cwd, sizeof(cwd)) == strlen("/unit-test"),
    "return should match len of current directory");
  check(strcmp(cwd, "/unit-test") == 0,
    "unexpected working directory: %s", cwd);
  return 0;
}

static char* test_readdir() {
  struct qfs_iter* iter = NULL;
  struct qfs_attr attr;
  int res;

  const char* expected[] = {
    "a",
    "..",
    ".",
  };
  int expected_length = sizeof(expected)/sizeof(expected[0]);

  int count = 0;
  char msg[128];
  while((res = qfs_readdir(qfs, "/unit-test", &iter, &attr)) > 0) {
    check(res <= expected_length,
      "value of result should be less that expected length");
    check(strcmp(attr.filename, expected[res-1]) == 0,
      "unexpected directory entry: %s != %s", attr.filename, expected[res-1]);
    check(attr.directory, "all files should be directories");
    count++;
  }
  qfs_iter_free(&iter);

  check(count == expected_length, "read all entries: %d != %d", count, expected_length);
  check(res <= 0, "%s", qfs_strerror(res, msg, sizeof(msg)));
  check(iter == NULL, "iterator should have been freed");

  return 0;
}

static char* test_readdirnames() {
  struct qfs_iter* iter = NULL;
  const char* dentry;
  int res;
  const char* expected[] = {
    "a",
    "..",
    ".",
  };
  char msg[128];
  int expected_length = sizeof(expected)/sizeof(expected[0]);

  int count = 0;
  while((res = qfs_readdirnames(qfs, "/unit-test", &iter, &dentry)) > 0) {
    check(res <= expected_length,
      "value of result should be less that expected length");
    check(strcmp(dentry, expected[res - 1]) == 0,
      "unexpected directory entry: %s != %s", dentry, expected[res - 1]);
    count++;
  }

  check(res >= 0, "%s", qfs_strerror(res, msg, sizeof(msg)));
  check(count == expected_length, "iterate through all entries");

  return 0;
}

static char* test_qfs_stat() {
  struct qfs_attr attr;
  check_qfs_call(qfs_stat(qfs, "/unit-test", &attr));

  check(strcmp(attr.filename, "unit-test") == 0,
    "attr should have correct path name: %s != %s", attr.filename, "unit-test");
  check(attr.directory, "file should be a directory");
  check(attr.mode == 0755, "mode should be 0755");

  return 0;
}

static char* test_qfs_create() {
  check_qfs_call(fd = qfs_create(qfs, "/unit-test/file"));

  // Now test qfs_stat_fd
  struct qfs_attr attr;
  check_qfs_call(qfs_stat_fd(qfs, fd, &attr));

  check(strcmp(attr.filename, "file") == 0, "filename should be correct");
  return 0;
}

static char* test_qfs_write() {
  check_qfs_call(qfs_write(qfs, fd, testdata, strlen(testdata) + 1));
  return 0;
}

static char* test_qfs_close() {
  check_qfs_call(qfs_close(qfs, fd));
  return 0;
}

static char* test_qfs_open() {
  check_qfs_call(fd = qfs_open(qfs, "/unit-test/file"));
  return 0;
}

static char* test_qfs_read() {
  char buf[4096];
  check_qfs_call(qfs_read(qfs, fd, buf, sizeof(buf)));

  check(strcmp(buf, testdata) == 0,
    "all expected data should be read: %s != %s", buf, testdata);

  // Now close the file
  check_qfs_call(qfs_close(qfs, fd));

  return 0;
}

static char* test_qfs_open_file() {
  check_qfs_call(fd = qfs_open_file(qfs, "/unit-test/file", O_TRUNC|O_RDWR, 0, ""));
  return 0;
}

static char* test_large_write() {
  ssize_t len = qfs_get_chunksize(qfs, "/unit-test/file");
  char* large = malloc(len);
  char* ptr;
  char  v = 0;
  for(ptr = large; ptr < large + len; ptr++) {
    *ptr = v++;
  }
  // Write the same set of data twice; but xord
  check_qfs_call(qfs_write(qfs, fd, large, len));
  check_qfs_call(qfs_sync(qfs, fd));

  for(ptr = large; ptr < large + len; ptr++) {
    *ptr ^= (char)0xA;
  }
  check_qfs_call(qfs_write(qfs, fd, large, len));
  free(large);
  large = 0;
  check_qfs_call(qfs_sync(qfs, fd));

  // Close and reopen to make stat work.
  check_qfs_call(qfs_close(qfs, fd));
  check_qfs_call(fd = qfs_open_file(qfs, "/unit-test/file", O_RDWR, 0, ""));

  struct qfs_attr attr;
  check_qfs_call(qfs_stat(qfs, "/unit-test/file", &attr));

  check(strcmp(attr.filename, "file") == 0,
    "filename should be correct: %s != %s", attr.filename, "file");
  check(attr.size == len*2,
    "file size should be correct: %li != %li",
    (long)attr.size, (long)len);

  return 0;
}

static char* test_qfs_pwrite() {
  // Now, go way passed the data we wrote before and write some more!
  ssize_t len = qfs_get_chunksize(qfs, "/unit-test/file");

  // Seek to the start
  check_qfs_call(qfs_seek(qfs, fd, 0, SEEK_SET));

  // Now write to the end, in a third chunk, but small.
  check_qfs_call(qfs_pwrite(qfs, fd, testdata, strlen(testdata), len*2));
  check_qfs_call(qfs_sync(qfs, fd)); // sync the data out

  return 0;
}

static char* test_qfs_pread() {
  ssize_t chunksize = qfs_get_chunksize(qfs, "/unit-test/file");
  off_t res;
  // Seek to the start
  check_qfs_call(res = qfs_tell(qfs, fd));
  check(res == 0, "file position should be at start");

  char buf[4096];
  memset(buf, 0, sizeof(buf));
  check_qfs_call(qfs_pread(qfs, fd, buf, sizeof(buf), chunksize*2));
  check(strcmp(buf, testdata) == 0,
    "expected data should be read: %s != %s", buf, testdata);

  return 0;
}

static char* test_qfs_get_data_locations() {
  check_qfs_call(qfs_close(qfs, fd)); // shut it down
  struct qfs_iter* iter = NULL;
  const char* location = NULL;
  off_t chunk = 0;
  int res;

  struct {
    off_t chunk;
    const char* hostname;
  } expected[] = {
    // In reverse order.
    {1, "127.0.0.1"},
    {1, "127.0.0.1"},
    {0, "127.0.0.1"},
    {0, "127.0.0.1"},
  };
  int expected_length = sizeof(expected)/sizeof(expected[0]);

  int count = 0;
  while((res = qfs_get_data_locations(qfs, "/unit-test/file", 0, 2*qfs_get_chunksize(qfs, "/unit-test/file"), &iter, &chunk, &location)) > 0) {
    check(res <= expected_length,
      "result should always be less than the expected number of chunks");
    check(chunk == expected[res].chunk,
      "unexpected chunk: %jd != %jd", (intmax_t)chunk, (intmax_t)expected[res].chunk);
    check(0 < strlen(location), "unexpected location");
    /* check(strncmp(location, expected[res].hostname, strlen(expected[res].hostname)) == 0,
      "unexpected location");*/
    count++;
  }

  check(count == expected_length,
    "unexpected number of chunk locations: %d != %d", count, expected_length);
  check_qfs_call(res >= 0);

  return 0;
}

static char * all_tests() {
  run(test_qfs_connect);
  run(test_get_metaserver_location);
  run(test_qfs_user_functions);
  run(test_qfs_cleanup);
  run(test_qfs_mkdir);
  run(test_qfs_mkdirs);
  run(test_qfs_cd);
  run(test_qfs_getwd);
  run(test_readdir);
  run(test_readdirnames);
  run(test_qfs_stat);
  run(test_qfs_create);
  run(test_qfs_write);
  run(test_qfs_close);
  run(test_qfs_open);
  run(test_qfs_read);
  run(test_qfs_open_file);
  run(test_large_write);
  run(test_qfs_pwrite);
  /* Always close and open file before issuing read, sync isn't sufficient to
     relinquish write lease(s) and update logical EOF on the meta server
     and in the kfs client's file table
  */
  run(test_qfs_close);
  run(test_qfs_open);
  run(test_qfs_pread);
  run(test_qfs_get_data_locations);
  run(test_qfs_cleanup);
  run(test_qfs_release);
  return 0;
}

int main(int argc, char **argv) {
  if(argc > 1) {
    // Override the default values for the metaserver host:port

    char* addr = argv[1];
    char* sep  = strrchr(addr, ':');

    if(sep == NULL) {
      fprintf(stderr, "invalid argument for metaserver addr: %s\n", addr);
      return EXIT_FAILURE;
    }

    if(strlen(sep) < 2) {
      fprintf(stderr, "Port must not be zero length\n");
      return EXIT_FAILURE;
    }

    metaserver_host = malloc(sep - addr + 1);
    memcpy(metaserver_host, addr, sep - addr);
    metaserver_host[sep-addr] = '\0';
    metaserver_port = atoi(sep + 1);
    fprintf(stderr, "running test against %s\n", addr);
  }

  char* result = all_tests();

  if (result == 0) {
    printf("pass\n");
  } else {
    printf("fail\n");
  }
  printf("%d tests\n", tests_run);
  return result != 0;
}
