"""Lifecycle checks for the VM proof's owned child processes."""

import gzip
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import durable_vm_power_loss as proof


class OwnedChildTests(unittest.TestCase):
    def test_guest_archive_has_root_owned_nonwritable_ancestors(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ("probe", "busybox"):
                (root / name).write_bytes(b"test")
            with patch.object(proof, "shared_libraries", return_value=[]):
                archive = proof.prepare_initramfs(root, root / "probe", root / "busybox")
            listed = subprocess.run(
                ["cpio", "--list", "--verbose", "--numeric-uid-gid", "--quiet"],
                input=gzip.decompress(archive.read_bytes()),
                capture_output=True,
                check=True,
            ).stdout.decode()
            mount_directory = next(line for line in listed.splitlines() if line.split()[-1] in {"mnt", "./mnt"})
            fields = mount_directory.split()
            self.assertEqual(fields[0], "drwxr-xr-x")
            self.assertEqual(fields[2:4], ["0", "0"])

    def test_marker_terminates_only_the_started_guest(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "serial.log"
            result = proof.boot_until(
                [sys.executable, "-u", "-c", "import time; print('READY'); time.sleep(10)"],
                rb"READY\n",
                log,
            )
            self.assertEqual(result["signal"], "SIGKILL")
            self.assertEqual(log.read_text(), "READY\n")

    def test_archive_timeout_terminates_its_child(self):
        original_popen = subprocess.Popen
        children = []

        def slow_cpio(*_args, **_kwargs):
            child = original_popen(
                [sys.executable, "-c", "import time; time.sleep(10)"],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            children.append(child)
            communicate = child.communicate
            child.communicate = lambda data=None, timeout=None: communicate(data, timeout=0.1 if data else timeout)
            return child

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ("probe", "busybox"):
                (root / name).write_bytes(b"test")
            with (
                patch.object(proof, "shared_libraries", return_value=[]),
                patch.object(proof.subprocess, "Popen", side_effect=slow_cpio),
                self.assertRaises(subprocess.TimeoutExpired),
            ):
                proof.prepare_initramfs(root, root / "probe", root / "busybox")
            self.assertEqual(len(children), 1)
            self.assertEqual(children[0].poll(), -9)


if __name__ == "__main__":
    unittest.main()
