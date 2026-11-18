# Copyright (c) 2025 Beijing Volcano Engine Technology Co., Ltd. and/or its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""Build the Harness Runtime image from immutable resolved Git snapshots."""

import argparse
from datetime import datetime, timezone
import importlib.util
import json
import re
import shlex
import subprocess
import tarfile
import tempfile
from pathlib import Path

RECIPE = Path("docker/harness-runtime")
PREPARE_PATH = Path(__file__).with_name("prepare_studio.py")
PREPARE_SPEC = importlib.util.spec_from_file_location("prepare_studio", PREPARE_PATH)
prepare_studio = importlib.util.module_from_spec(PREPARE_SPEC)
PREPARE_SPEC.loader.exec_module(prepare_studio)


def git(repo, *args):
    return subprocess.check_output(["git", "-C", str(repo), *args], text=True).strip()


def commit_sha(value):
    if not re.fullmatch(r"[0-9a-f]{40}", value):
        raise ValueError("Studio commit must be a full lowercase 40-character SHA")
    return value


def pinned_image(value):
    if not re.fullmatch(r"[^\s]+@sha256:[0-9a-f]{64}", value):
        raise ValueError("--python-image must include an immutable @sha256 digest")
    return value


def export_commit(repo, revision, destination):
    destination.mkdir()
    archive = destination.with_suffix(".tar")
    subprocess.run(
        [
            "git",
            "-C",
            str(repo),
            "archive",
            "--format=tar",
            f"--output={archive}",
            revision,
        ],
        check=True,
    )
    with tarfile.open(archive) as stream:
        stream.extractall(destination, filter="data")
    archive.unlink()


def build_command(
    python_image,
    ma_sha,
    studio_sha,
    tag,
    tarball=None,
    *,
    studio_repository="",
    studio_ref="",
):
    command = [
        "docker",
        "buildx",
        "build",
        "--platform",
        "linux/amd64",
        "--file",
        str(RECIPE / "Dockerfile"),
        "--build-arg",
        f"PYTHON_IMAGE={pinned_image(python_image)}",
        "--build-arg",
        f"VCS_REF={commit_sha(ma_sha)}",
        "--build-arg",
        f"STUDIO_REVISION={commit_sha(studio_sha)}",
        "--build-arg",
        f"STUDIO_SOURCE={studio_repository}",
        "--build-arg",
        f"STUDIO_REF={studio_ref}",
        "--label",
        f"org.opencontainers.image.revision={ma_sha}",
        "--label",
        f"com.agentkit.studio.revision={studio_sha}",
        "--label",
        f"org.opencontainers.image.base.name={python_image}",
        "--tag",
        tag,
    ]
    if tarball is None:
        command.append("--load")
    else:
        command.extend(["--output", f"type=docker,dest={Path(tarball).resolve()}"])
    command.append(".")
    return command


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python-image", required=True, type=pinned_image)
    parser.add_argument("--tag", required=True)
    parser.add_argument(
        "--studio-checkout",
        type=Path,
        help="Optional local Git object cache; its checkout is not changed",
    )
    parser.add_argument("--studio-repository")
    parser.add_argument("--studio-ref")
    parser.add_argument("--source-metadata-file", type=Path)
    parser.add_argument(
        "--output-tarball",
        type=Path,
        help="Export a Docker archive instead of loading the image locally",
    )
    parser.add_argument(
        "--print", action="store_true", dest="print_only", help="Print the command"
    )
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[2]
    if git(repo, "status", "--porcelain"):
        raise ValueError("Commit the Harness Runtime checkout before building")
    ma_sha = commit_sha(git(repo, "rev-parse", "HEAD"))
    with tempfile.TemporaryDirectory(prefix="agentkit-unified-") as directory:
        work = Path(directory)
        source_checkout = work / "runtime"
        export_commit(repo, ma_sha, source_checkout)
        repository, ref = prepare_studio.source_config(
            args.studio_repository, args.studio_ref
        )
        studio_sha = prepare_studio.prepare(
            source_checkout / RECIPE / ".studio-source",
            args.studio_checkout,
            repository,
            ref,
        )
        command = build_command(
            args.python_image,
            ma_sha,
            studio_sha,
            args.tag,
            args.output_tarball,
            studio_repository=repository,
            studio_ref=ref,
        )
        metadata = {
            "runtime_commit": ma_sha,
            "studio_repository": repository,
            "studio_ref": ref,
            "studio_commit": studio_sha,
            "python_image": args.python_image,
            "tag": args.tag,
            "resolved_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        if args.source_metadata_file is not None:
            args.source_metadata_file.parent.mkdir(parents=True, exist_ok=True)
            args.source_metadata_file.write_text(json.dumps(metadata, indent=2) + "\n")
        print(
            f"Runtime={ma_sha}\nStudio={studio_sha}\nStudioRepository={repository}\n"
            f"StudioRef={ref}\nPython={args.python_image}",
            flush=True,
        )
        if args.print_only:
            print(shlex.join(command))
        else:
            subprocess.run(command, cwd=source_checkout, check=True)


if __name__ == "__main__":
    main()
