/* Test-only: fail the intent or reply sync for one durable close. */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int injected;
int fsync(int fd) {
    int (*original)(int) = dlsym(RTLD_NEXT, "fsync");
    const char *mode = getenv("LAYERFS_TEST_CLOSE_FAULT");
    char link[64], actual[PATH_MAX];
    if (!mode || snprintf(link, sizeof(link), "/proc/self/fd/%d", fd) < 0) return original(fd);
    ssize_t length = readlink(link, actual, sizeof(actual) - 1);
    if (length < 0 || (size_t)length >= sizeof(actual) - 1) return original(fd);
    actual[length] = '\0';
    if (strstr(actual, "/close-intent-") &&
        ((strcmp(mode, "intent") == 0 && strstr(actual, ".record.") == NULL) ||
         (strcmp(mode, "answer") == 0 && strstr(actual, ".record.") != NULL)) &&
        __sync_bool_compare_and_swap(&injected, 0, 1)) {
        errno = EIO;
        return -1;
    }
    return original(fd);
}
