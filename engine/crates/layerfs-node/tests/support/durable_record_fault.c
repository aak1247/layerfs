/* Test-only: inject a sync failure or directory swap at project.record publication. */
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

static int published, changed;
static const char *home(void) { return getenv("LAYERFS_HOME"); }
static void receipt(const char *name) {
    char path[PATH_MAX];
    if (!home() || snprintf(path, sizeof(path), "%s/%s", home(), name) < 0) return;
    int fd = open(path, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    if (fd >= 0) { (void)write(fd, "done\n", 5); close(fd); }
}
int rename(const char *oldpath, const char *newpath) {
    int (*original)(const char *, const char *) = dlsym(RTLD_NEXT, "rename");
    int result = original(oldpath, newpath);
    const char *mode = getenv("LAYERFS_TEST_RECORD_MODE");
    size_t n = strlen(newpath);
    const char *suffix = "/project.record";
    size_t s = strlen(suffix);
    if (result == 0 && mode && strcmp(mode, "sync") == 0 && n >= s && strcmp(newpath + n - s, suffix) == 0)
        __atomic_store_n(&published, 1, __ATOMIC_SEQ_CST);
    return result;
}
int fsync(int fd) {
    int (*original)(int) = dlsym(RTLD_NEXT, "fsync");
    int (*original_rename)(const char *, const char *) = dlsym(RTLD_NEXT, "rename");
    const char *mode = getenv("LAYERFS_TEST_RECORD_MODE");
    char descriptor[64], actual[PATH_MAX], directory[PATH_MAX], prefix[PATH_MAX], backup[PATH_MAX];
    if (!mode || !home() || snprintf(descriptor, sizeof(descriptor), "/proc/self/fd/%d", fd) < 0) return original(fd);
    ssize_t length = readlink(descriptor, actual, sizeof(actual) - 1);
    if (length < 0 || (size_t)length >= sizeof(actual) - 1) return original(fd);
    actual[length] = '\0';
    if (snprintf(directory, sizeof(directory), "%s/durable-project", home()) < 0 ||
        snprintf(prefix, sizeof(prefix), "%s/project.", directory) < 0 ||
        snprintf(backup, sizeof(backup), "%s/original-project", home()) < 0) return original(fd);
    if (strcmp(mode, "sync") == 0 && __atomic_load_n(&published, __ATOMIC_SEQ_CST) && strcmp(actual, directory) == 0) {
        receipt("record-sync-failed");
        errno = EIO;
        return -1;
    }
    int result = original(fd);
    int saved_errno = errno;
    if (strcmp(mode, "swap") == 0 && strncmp(actual, prefix, strlen(prefix)) == 0 && __sync_bool_compare_and_swap(&changed, 0, 1)) {
        if (original_rename(directory, backup) == 0 && mkdir(directory, 0700) == 0) receipt("record-parent-swapped");
    }
    errno = saved_errno;
    return result;
}
