/* Test-only: fail directory syncs after a fixture creates its trigger. */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int fail_directory(int fd) {
    const char *trigger = getenv("LAYERFS_TEST_DIR_SYNC_TRIGGER");
    const char *target = getenv("LAYERFS_TEST_DIR_SYNC_TARGET");
    const char *receipt = getenv("LAYERFS_TEST_DIR_SYNC_RECEIPT");
    if (!trigger || !target || !receipt || access(trigger, F_OK) != 0) return 0;
    char descriptor[64], actual[PATH_MAX];
    if (snprintf(descriptor, sizeof(descriptor), "/proc/self/fd/%d", fd) < 0) return 0;
    ssize_t length = readlink(descriptor, actual, sizeof(actual) - 1);
    if (length < 0 || (size_t)length >= sizeof(actual) - 1) return 0;
    actual[length] = '\0';
    if (strcmp(actual, target)) return 0;
    int out = open(receipt, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0600);
    if (out >= 0) {
        (void)write(out, "EIO\n", 4);
        close(out);
    }
    errno = EIO;
    return 1;
}

int fsync(int fd) {
    if (fail_directory(fd)) return -1;
    int (*original)(int) = dlsym(RTLD_NEXT, "fsync");
    return original(fd);
}

int fdatasync(int fd) {
    if (fail_directory(fd)) return -1;
    int (*original)(int) = dlsym(RTLD_NEXT, "fdatasync");
    return original(fd);
}
