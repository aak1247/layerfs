/* Test-only: fail the first replacement of a Sandbox creation intent. */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int injected;
int fsync(int fd) {
    int (*original)(int) = dlsym(RTLD_NEXT, "fsync");
    const char *home = getenv("LAYERFS_HOME");
    char link[64], actual[PATH_MAX], marker[PATH_MAX];
    if (!home || snprintf(link, sizeof(link), "/proc/self/fd/%d", fd) < 0) return original(fd);
    ssize_t length = readlink(link, actual, sizeof(actual) - 1);
    if (length < 0 || (size_t)length >= sizeof(actual) - 1) return original(fd);
    actual[length] = '\0';
    if (strstr(actual, "/create.intent.") && __sync_bool_compare_and_swap(&injected, 0, 1)) {
        if (snprintf(marker, sizeof(marker), "%s/create-sync-failed", home) > 0) {
            int output = open(marker, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
            if (output >= 0) { (void)write(output, "done\n", 5); close(output); }
        }
        errno = EIO;
        return -1;
    }
    return original(fd);
}
