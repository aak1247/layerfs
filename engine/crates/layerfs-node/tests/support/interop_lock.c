/* Test-only observations of the actual Node discovery flock boundary. */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <unistd.h>

static void mark(const char *home, const char *name) {
    char path[4096];
    snprintf(path, sizeof path, "%s/%s", home, name);
    int file = open(path, O_WRONLY | O_CREAT | O_CLOEXEC, 0600);
    if (file >= 0) close(file);
}

int flock(int fd, int operation) {
    int (*original)(int, int) = dlsym(RTLD_NEXT, "flock");
    int result = original(fd, operation);
    int saved_errno = errno;
    const char *home = getenv("LAYERFS_FORK_HOME");
    if (!home || operation != (LOCK_EX | LOCK_NB)) { errno = saved_errno; return result; }
    char link[64], target[4096], expected[4096], release[4096];
    snprintf(link, sizeof link, "/proc/self/fd/%d", fd);
    ssize_t size = readlink(link, target, sizeof target - 1);
    if (size < 0) { errno = saved_errno; return result; }
    target[size] = 0;
    snprintf(expected, sizeof expected, "%s/proj/ownership.lock", home);
    if (strcmp(target, expected) != 0) { errno = saved_errno; return result; }
    if (result != 0 && saved_errno == EWOULDBLOCK) {
        mark(home, "interop-blocked");
    } else if (result == 0) {
        mark(home, "interop-acquired");
        if (getenv("LAYERFS_INTEROP_HOLD")) {
            snprintf(release, sizeof release, "%s/interop-release", home);
            for (int i = 0; i < 500 && access(release, F_OK) != 0; ++i) usleep(10000);
        }
    }
    errno = saved_errno;
    return result;
}
