#!/usr/bin/env python3
"""
Parallel rsync copy helper.

This tool keeps rsync's source semantics for files and directories:
  source_dir/  -> copy directory contents into destination
  source_dir   -> copy source_dir itself into destination
  source_file  -> copy the file to destination

Both source and destination may be local paths or rsync-style remote specs such
as user@host:/path.  Transfers are executed as many independent rsync processes,
one per regular file, with directories created separately.
"""

from __future__ import annotations

import argparse
import os
import posixpath
import random
import re
import shlex
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence


REMOTE_RE = re.compile(r"^(?P<host>(?:[^/@:]+@)?[^/:]+):(?P<path>.*)$")


@dataclass(frozen=True)
class Entry:
    """One item reported by rsync --list-only."""

    path: str
    size: int
    kind: str  # "file", "dir", or "other"


@dataclass(frozen=True)
class SourcePlan:
    """How a source will be scanned and transferred."""

    original: str
    base: str
    is_single_file: bool
    entries: tuple[Entry, ...]


@dataclass(frozen=True)
class Settings:
    source: str
    dest: str
    args: tuple[str, ...]
    threads: int
    delay_ms: float
    verbose: bool
    dryrun: bool


def parse_arguments(cmd_args: Sequence[str]) -> Settings:
    parser = argparse.ArgumentParser(
        description="Copy files/directories with multiple parallel rsync streams.",
        usage="%(prog)s -s SOURCE -d DEST [options]",
    )
    parser.add_argument("-s", "--source", required=True, help="source path or user@host:path")
    parser.add_argument("-d", "--dest", required=True, help="destination path or user@host:path")
    parser.add_argument(
        "-a",
        "--args",
        default="-a",
        help="additional rsync arguments, shell-style quoted; default: '-a'",
    )
    parser.add_argument(
        "-t",
        "--threads",
        default=4,
        type=int,
        help="number of parallel rsync processes; default: 4",
    )
    parser.add_argument(
        "-w",
        "--delay",
        default=3.0,
        type=float,
        help="maximum random startup delay per transfer in milliseconds; default: 3.0",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="print every rsync command")
    parser.add_argument("-x", "--dryrun", action="store_true", help="print commands without transferring")
    args = parser.parse_args(cmd_args)

    if args.threads < 1:
        parser.error("--threads must be at least 1")
    if args.delay < 0:
        parser.error("--delay must not be negative")

    return Settings(
        source=args.source,
        dest=args.dest,
        args=tuple(shlex.split(args.args)),
        threads=args.threads,
        delay_ms=args.delay,
        verbose=args.verbose,
        dryrun=args.dryrun,
    )


def split_remote(spec: str) -> tuple[str | None, str]:
    """Return (host, path). host is None for local paths."""
    match = REMOTE_RE.match(spec)
    if match:
        return match.group("host"), match.group("path")
    return None, spec


def join_remote(host: str | None, path: str) -> str:
    return f"{host}:{path}" if host else path


def path_dirname(path: str) -> str:
    # Use POSIX rules for rsync remote paths, and local Path rules for local paths.
    if path in ("", "."):
        return "."
    return posixpath.dirname(path.rstrip("/")) or "."


def path_basename(path: str) -> str:
    return posixpath.basename(path.rstrip("/"))


def parent_spec(spec: str) -> str:
    host, path = split_remote(spec)
    return join_remote(host, path_dirname(path))


def source_arg_with_relative_marker(base: str, relpath: str) -> str:
    """Build an rsync source argument using /./ so --relative preserves relpath."""
    host, path = split_remote(base)
    clean_base = path.rstrip("/") or "/"
    clean_rel = relpath.lstrip("/")
    if clean_base == "/":
        marked = f"/./{clean_rel}"
    else:
        marked = f"{clean_base}/./{clean_rel}"
    return join_remote(host, marked)


def dest_as_directory(dest: str) -> str:
    return dest if dest.endswith("/") else dest + "/"


def run_command(cmd: Sequence[str], *, verbose: bool, dryrun: bool) -> int:
    printable = shlex.join(cmd)
    if verbose or dryrun:
        print(printable, flush=True)
    if dryrun:
        return 0
    return subprocess.run(cmd).returncode


def parse_rsync_list_line(line: str) -> Entry | None:
    # Example:
    # -rw-r--r--          1,234 2025/01/01 10:00:00 file name.txt
    # drwxr-xr-x          4,096 2025/01/01 10:00:00 dir
    parts = line.rstrip("\n").split(maxsplit=4)
    if len(parts) < 5:
        return None
    mode, size_text, _date, _time, name = parts
    if name in (".", "./"):
        return None
    kind = "file" if mode.startswith("-") else "dir" if mode.startswith("d") else "other"
    try:
        size = int(size_text.replace(",", ""))
    except ValueError:
        size = 0
    return Entry(path=name.rstrip("/") if kind == "dir" else name, size=size, kind=kind)


def list_source(source: str, rsync_args: Sequence[str]) -> tuple[Entry, ...]:
    cmd = ["rsync", "-r", "--list-only", "--protect-args", *rsync_args, source]
    proc = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if proc.returncode != 0:
        raise RuntimeError(
            "Could not list source with rsync.\n"
            f"Command: {shlex.join(cmd)}\n"
            f"rsync stderr:\n{proc.stderr.strip()}"
        )
    entries = [e for line in proc.stdout.splitlines() if (e := parse_rsync_list_line(line))]
    return tuple(entries)


def plan_source(source: str, rsync_args: Sequence[str], verbose: bool = False) -> SourcePlan:
    entries = list_source(source, rsync_args)
    files = [e for e in entries if e.kind == "file"]
    dirs = [e for e in entries if e.kind == "dir"]

    # A single regular file is listed as exactly one file and no directories.
    is_single_file = len(files) == 1 and not dirs and files[0].path == path_basename(split_remote(source)[1])

    if source.endswith("/"):
        base = source.rstrip("/") or source
    else:
        # For a directory without a trailing slash, rsync lists paths with the
        # directory basename included.  Transfer from its parent so /./ preserves
        # that basename at the destination.  For a single file this is harmless,
        # though single files are copied with one plain rsync command.
        base = parent_spec(source)

    sorted_entries = tuple(sorted(entries, key=lambda e: (e.kind != "file", -e.size, e.path)))

    if verbose:
        print("Detected source entries:")
        for e in sorted_entries:
            print(f"  {e.kind:4} {e.size:12d} {e.path}")

    return SourcePlan(original=source, base=base, is_single_file=is_single_file, entries=sorted_entries)


def transfer_entry(settings: Settings, entry: Entry, base: str) -> int:
    if settings.delay_ms:
        time.sleep(random.random() * settings.delay_ms / 1000.0)

    dest_dir = dest_as_directory(settings.dest)
    if entry.kind == "dir":
        cmd = [
            "rsync",
            "-d",
            "--mkpath",
            "--relative",
            "--protect-args",
            *settings.args,
            source_arg_with_relative_marker(base, entry.path),
            dest_dir,
        ]
    elif entry.kind == "file":
        cmd = [
            "rsync",
            "--mkpath",
            "--relative",
            "--protect-args",
            *settings.args,
            source_arg_with_relative_marker(base, entry.path),
            dest_dir,
        ]
    else:
        return 0
    return run_command(cmd, verbose=settings.verbose, dryrun=settings.dryrun)


def transfer(settings: Settings) -> int:
    plan = plan_source(settings.source, settings.args, verbose=settings.verbose)

    if plan.is_single_file:
        cmd = ["rsync", "--protect-args", *settings.args, settings.source, settings.dest]
        return run_command(cmd, verbose=True if settings.dryrun else settings.verbose, dryrun=settings.dryrun)

    entries = [e for e in plan.entries if e.kind in {"file", "dir"}]
    files = [e for e in entries if e.kind == "file"]
    dirs = [e for e in entries if e.kind == "dir"]
    print(f"Found {len(files)} files and {len(dirs)} directories under {settings.source}")

    failures: list[tuple[Entry, int]] = []
    with ThreadPoolExecutor(max_workers=settings.threads) as executor:
        futures = {executor.submit(transfer_entry, settings, entry, plan.base): entry for entry in entries}
        for future in as_completed(futures):
            entry = futures[future]
            try:
                rc = future.result()
            except Exception as exc:  # noqa: BLE001 - top-level transfer should report all failures
                print(f"ERROR: {entry.path}: {exc}", file=sys.stderr)
                failures.append((entry, 1))
                continue
            if rc != 0:
                print(f"ERROR: rsync failed with exit code {rc}: {entry.path}", file=sys.stderr)
                failures.append((entry, rc))

    if failures:
        print(f"{len(failures)} transfer(s) failed.", file=sys.stderr)
        return 1
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    settings = parse_arguments(sys.argv[1:] if argv is None else argv)
    print(f"source = {settings.source}")
    print(f"dest   = {settings.dest}")
    return transfer(settings)


if __name__ == "__main__":
    raise SystemExit(main())
