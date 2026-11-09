/* Test-only: fail the first replacement of a durable mount intent. */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static int injected;
int fsync(int fd) {
    int (*original)(int) = dlsym(RTLD_NEXT, "fsync");
    const char *home = getenv("LAYERFS_HOME");
    char link[64], actual[PATH_MAX], marker[PATH_MAX], directory[PATH_MAX], backup[PATH_MAX];
    if (!home || snprintf(link, sizeof(link), "/proc/self/fd/%d", fd) < 0) return original(fd);
    ssize_t length = readlink(link, actual, sizeof(actual) - 1);
    if (length < 0 || (size_t)length >= sizeof(actual) - 1) return original(fd);
    actual[length] = '\0';
    if (strstr(actual, "/mount-intent-") && strstr(actual, ".record.") &&
        __sync_bool_compare_and_swap(&injected, 0, 1)) {
        const char *mode = getenv("LAYERFS_TEST_MOUNT_FAULT");
        if (mode && strcmp(mode, "swap") == 0) {
            int result = original(fd);
            int (*original_rename)(const char *, const char *) = dlsym(RTLD_NEXT, "rename");
            if (snprintf(directory, sizeof(directory), "%s/durable-project/sandboxes/1", home) > 0 &&
                snprintf(backup, sizeof(backup), "%s/original-sandbox", home) > 0 &&
                original_rename(directory, backup) == 0 && mkdir(directory, 0700) == 0 &&
                snprintf(marker, sizeof(marker), "%s/mount-parent-swapped", home) > 0) {
                int output = open(marker, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
                if (output >= 0) { (void)write(output, "done\n", 5); close(output); }
            }
            return result;
        }
        if (snprintf(marker, sizeof(marker), "%s/mount-sync-failed", home) > 0) {
            int output = open(marker, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
            if (output >= 0) { (void)write(output, "done\n", 5); close(output); }
        }
        errno = EIO;
        return -1;
    }
    return original(fd);
}
