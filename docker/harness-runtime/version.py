"""Resolve the Harness image tag once for all Bits pipeline jobs."""

import os
from pathlib import Path
import re
import subprocess

VERSION_FILE = Path(__file__).with_name("VERSION")


def resolve(version: str, branch: str, revision: str) -> tuple[str, bool]:
    if not re.fullmatch(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)", version):
        raise ValueError("VERSION must contain MAJOR.MINOR.PATCH, e.g. 1.0.0")
    if not branch or "${{" in branch:
        raise ValueError("HARNESS_BUILD_BRANCH must be supplied by Bits Head.Branch")
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("Expected a full Git commit SHA")
    release = branch == "main"
    return (version if release else revision), release


def main():
    version = VERSION_FILE.read_text().strip()
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    tag, release = resolve(
        version, os.environ.get("HARNESS_BUILD_BRANCH", ""), revision
    )
    print(f"Harness version: {version}; image tag: {tag}; release: {release}")
    print(f"::set-output name=tag::{tag}")
    print(f"::set-output name=release::{str(release).lower()}")


if __name__ == "__main__":
    main()
