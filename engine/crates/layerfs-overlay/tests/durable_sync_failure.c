/* Test-only: fail one sync of this fixture's SQLite rollback journal after
 * the child creates its explicit trigger. All other syscalls are delegated. */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static atomic_int fired;

static int fail_journal(int fd) {
    const char *trigger = getenv("LAYERFS_TEST_SYNC_TRIGGER");
    const char *receipt = getenv("LAYERFS_TEST_SYNC_RECEIPT");
    if (!trigger || !receipt || access(trigger, F_OK) != 0) return 0;
    char descriptor[64], target[PATH_MAX];
    if (snprintf(descriptor, sizeof(descriptor), "/proc/self/fd/%d", fd) < 0) return 0;
    ssize_t length = readlink(descriptor, target, sizeof(target) - 1);
    if (length < 0 || (size_t)length >= sizeof(target) - 1) return 0;
    target[length] = '\0';
    const char *suffix = "/overlay.sqlite-journal";
    size_t target_len = strlen(target), suffix_len = strlen(suffix);
    if (target_len < suffix_len || strcmp(target + target_len - suffix_len, suffix)) return 0;
    int expected = 0;
    if (!atomic_compare_exchange_strong(&fired, &expected, 1)) return 0;
    int out = open(receipt, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    if (out >= 0) {
        (void)write(out, "EIO\n", 4);
        close(out);
    }
    errno = EIO;
    return 1;
}

int fsync(int fd) {
    if (fail_journal(fd)) return -1;
    int (*original)(int) = dlsym(RTLD_NEXT, "fsync");
    return original(fd);
}

int fdatasync(int fd) {
    if (fail_journal(fd)) return -1;
    int (*original)(int) = dlsym(RTLD_NEXT, "fdatasync");
    return original(fd);
}
