#!/usr/bin/env python3
"""Kill an owned Linux VM after a durable database write, then reopen its disk.

This tests guest memory loss and recovery of a Store or Workspace overlay. It
does not simulate loss of the physical host or a drive that lies about flush
completion. All artifacts remain in the new output directory.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import re
import selectors
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path


def digest(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def shared_libraries(binary: Path) -> list[Path]:
    result = subprocess.run(["ldd", str(binary)], check=True, capture_output=True, text=True, timeout=10)
    if "not found" in result.stdout:
        raise RuntimeError(f"unresolved guest binary library: {result.stdout}")
    paths = []
    for line in result.stdout.splitlines():
        match = re.search(r"(?:=>\s+|^\s*)(/\S+)", line)
        if match:
            paths.append(Path(match.group(1)))
    if not paths:
        raise RuntimeError("ldd did not identify the guest dynamic loader")
    return paths


def prepare_initramfs(output: Path, probe: Path, busybox: Path) -> Path:
    root = output / "initramfs"
    for directory in ("bin", "dev", "lib", "lib64", "mnt", "proc", "sys"):
        (root / directory).mkdir(parents=True, exist_ok=True)
    shutil.copy2(busybox, root / "bin/busybox")
    for name in ("sh", "mount", "mkdir", "sleep", "cat", "poweroff"):
        (root / "bin" / name).symlink_to("busybox")
    shutil.copy2(probe, root / "probe")
    for library in shared_libraries(probe):
        destination = root / library.relative_to("/")
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(library, destination, follow_symlinks=True)
    init = root / "init"
    init.write_text(
        """#!/bin/sh
export PATH=/bin
mount -t proc proc /proc || exit 1
mount -t sysfs sysfs /sys || exit 1
mount -t devtmpfs devtmpfs /dev || exit 1
for i in 1 2 3 4 5 6 7 8 9 10; do
  [ -b /dev/sda ] && break
  sleep 1
done
mount -t ext4 /dev/sda /mnt || exit 1
case " $(cat /proc/cmdline) " in
  *" layerfs.mode=write "*) mode=write ;;
  *" layerfs.mode=read "*) mode=read ;;
  *) exit 1 ;;
esac
LAYERFS_DURABLE_VM_ROOT=/mnt/store LAYERFS_DURABLE_VM_MODE=$mode \\
  /probe --ignored --exact durable_save_survives_vm_power_loss --nocapture
status=$?
echo LAYERFS_VM_PROBE_EXIT=$status
poweroff -f
"""
    )
    init.chmod(0o755)
    entries = [b".\0"]
    for path in sorted(root.rglob("*")):
        entries.append(os.fsencode("./" + str(path.relative_to(root))) + b"\0")
    archive = output / "initramfs.cpio.gz"
    with gzip.open(archive, "wb", compresslevel=6) as compressed:
        cpio = subprocess.Popen(
            ["cpio", "--null", "--create", "--format=newc", "--quiet"],
            cwd=root,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            data, error = cpio.communicate(b"".join(entries), timeout=30)
        finally:
            if cpio.poll() is None:
                cpio.kill()
                cpio.communicate(timeout=5)
        if cpio.returncode:
            raise RuntimeError(f"cpio failed: {error.decode(errors='replace')}")
        compressed.write(data)
    return archive


def boot_until(command: list[str], marker: bytes, log: Path) -> dict:
    started = time.monotonic()
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    captured = bytearray()
    try:
        with selectors.DefaultSelector() as selector, log.open("wb") as serial:
            assert process.stdout is not None
            selector.register(process.stdout, selectors.EVENT_READ)
            while time.monotonic() - started < 30:
                for key, _ in selector.select(timeout=0.5):
                    chunk = os.read(key.fd, 65536)
                    if not chunk:
                        raise RuntimeError(f"guest exited before {marker!r}: {process.poll()}")
                    serial.write(chunk)
                    serial.flush()
                    captured.extend(chunk)
                    match = re.search(marker, captured)
                    if match:
                        process.kill()
                        code = process.wait(timeout=5)
                        if code != -signal.SIGKILL:
                            raise RuntimeError(f"guest was not killed: {code}")
                        return {
                            "marker": marker.decode(),
                            "signal": "SIGKILL",
                            "elapsed_seconds": round(time.monotonic() - started, 3),
                            "matched": match.group().decode().strip(),
                        }
                if process.poll() is not None:
                    raise RuntimeError(f"guest exited before {marker!r}: {process.returncode}")
            raise TimeoutError(f"guest did not emit {marker!r} in 30 seconds")
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        if process.stdout is not None:
            process.stdout.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="new owned artifact directory")
    parser.add_argument("--kernel", type=Path, required=True)
    parser.add_argument("--probe", type=Path, required=True, help="prebuilt Rust test binary")
    parser.add_argument("--probe-kind", choices=("store", "overlay"), default="store")
    parser.add_argument("--busybox", type=Path, default=Path("/bin/busybox"))
    args = parser.parse_args()
    output = args.output.absolute()
    kernel, probe, busybox = (path.resolve(strict=True) for path in (args.kernel, args.probe, args.busybox))
    if "," in str(output):
        parser.error("output path must not contain a comma (QEMU drive syntax)")
    output.mkdir(mode=0o700)
    receipt = {
        "status": "started",
        "kernel_sha256": digest(kernel),
        "probe_sha256": digest(probe),
        "busybox_sha256": digest(busybox),
        "scope": (
            "guest power loss after acknowledged Durable Store save; not Workspace recovery"
            if args.probe_kind == "store"
            else "guest power loss after acknowledged durable overlay transaction; not mount recovery"
        ),
        "disk": "256 MiB raw ext4, QEMU IDE cache=none",
    }
    try:
        archive = prepare_initramfs(output, probe, busybox)
        disk = output / "disk.raw"
        with disk.open("xb") as file:
            file.truncate(256 * 1024 * 1024)
        subprocess.run(["mkfs.ext4", "-F", "-q", str(disk)], check=True, timeout=20)
        with disk.open("rb") as file:
            os.fsync(file.fileno())
        common = [
            "qemu-system-x86_64",
            "-machine",
            "accel=kvm",
            "-cpu",
            "host",
            "-smp",
            "2",
            "-m",
            "512",
            "-display",
            "none",
            "-monitor",
            "none",
            "-serial",
            "stdio",
            "-no-reboot",
            "-net",
            "none",
            "-kernel",
            str(kernel),
            "-initrd",
            str(archive),
            "-drive",
            f"file={disk},if=ide,format=raw,cache=none",
        ]
        ack = (
            rb"LAYERFS_VM_ACK wal_bytes=[1-9][0-9]*\r?\n"
            if args.probe_kind == "store"
            else rb"LAYERFS_VM_ACK overlay_bytes=[1-9][0-9]*\r?\n"
        )
        receipt["write"] = boot_until(
            common + ["-append", "console=ttyS0 panic=-1 quiet layerfs.mode=write"],
            ack,
            output / "write-serial.log",
        )
        receipt["read"] = boot_until(
            common + ["-append", "console=ttyS0 panic=-1 quiet layerfs.mode=read"],
            rb"LAYERFS_VM_RECOVERED\r?\n",
            output / "read-serial.log",
        )
        receipt["status"] = "passed"
    except (OSError, RuntimeError, subprocess.SubprocessError, ValueError, shutil.Error) as error:
        receipt["status"] = "failed"
        receipt["error"] = repr(error)
    (output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))
    return 0 if receipt["status"] == "passed" else 1


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, lambda _signum, _frame: sys.exit("interrupted"))
    sys.exit(main())
