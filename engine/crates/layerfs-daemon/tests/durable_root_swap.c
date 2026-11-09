/* Test-only: exchange the Overlay directory after the catalog's fsync. */
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

int fsync(int fd) {
    int (*original)(int) = dlsym(RTLD_NEXT, "fsync");
    int result = original(fd);
    int original_errno = errno;
    const char *target = getenv("LAYERFS_TEST_SWAP_TARGET");
    const char *source = getenv("LAYERFS_TEST_SWAP_SOURCE");
    const char *backup = getenv("LAYERFS_TEST_SWAP_BACKUP");
    const char *trigger = getenv("LAYERFS_TEST_SWAP_TRIGGER");
    const char *receipt = getenv("LAYERFS_TEST_SWAP_RECEIPT");
    static int swapped;
    if (!target || !source || !backup || !trigger || !receipt || access(trigger, F_OK) != 0 || __atomic_load_n(&swapped, __ATOMIC_SEQ_CST)) goto done;
    char descriptor[64], actual[PATH_MAX];
    if (snprintf(descriptor, sizeof(descriptor), "/proc/self/fd/%d", fd) < 0) goto done;
    ssize_t length = readlink(descriptor, actual, sizeof(actual) - 1);
    if (length < 0 || (size_t)length >= sizeof(actual) - 1) goto done;
    actual[length] = '\0';
    if (strcmp(actual, target) || !__sync_bool_compare_and_swap(&swapped, 0, 1)) goto done;
    int changed = rename(source, backup) == 0 && mkdir(source, 0700) == 0;
    if (changed && getenv("LAYERFS_TEST_SWAP_DENY")) changed = chmod(backup, 0755) == 0;
    const char *outcome = changed ? "swapped\n" : "failed\n";
    int out = open(receipt, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    if (out >= 0) { (void)write(out, outcome, strlen(outcome)); close(out); }
done:
    errno = original_errno;
    return result;
}
