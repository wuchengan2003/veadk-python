"""Fetch and export Studio Harness source into the Docker build context."""

import argparse
import json
import re
import subprocess
import tarfile
import tempfile
from pathlib import Path

SOURCE_FILE = Path(__file__).with_name("sources.json")


def commit_sha(value):
    if not re.fullmatch(r"[0-9a-f]{40}", value):
        raise ValueError("Studio commit must be a full lowercase 40-character SHA")
    return value


def git(repo, *args):
    return subprocess.check_output(["git", "-C", str(repo), *args], text=True).strip()


def safe_extract(archive, destination):
    destination = Path(destination).resolve()
    with tarfile.open(archive) as stream:
        for member in stream.getmembers():
            target = (destination / member.name).resolve()
            if target != destination and destination not in target.parents:
                raise ValueError(f"Unsafe Studio archive path: {member.name}")
            if member.isdev():
                raise ValueError(f"Unsupported Studio archive member: {member.name}")
            if member.issym() or member.islnk():
                link = (target.parent / member.linkname).resolve()
                if link != destination and destination not in link.parents:
                    raise ValueError(f"Unsafe Studio archive link: {member.name}")
        stream.extractall(destination)


def fetch_studio(repository, ref, destination, cache=None):
    if cache is None:
        subprocess.run(
            ["git", "init", "--bare", "--quiet", str(destination)], check=True
        )
    else:
        subprocess.run(
            [
                "git",
                "clone",
                "--bare",
                "--shared",
                "--quiet",
                "--",
                str(Path(cache).resolve()),
                str(destination),
            ],
            check=True,
        )
    subprocess.run(
        [
            "git",
            "-C",
            str(destination),
            "fetch",
            "--depth=1",
            "--no-tags",
            "--",
            repository,
            ref,
        ],
        check=True,
    )
    return commit_sha(git(destination, "rev-parse", "FETCH_HEAD^{commit}"))


def source_config(repository=None, ref=None):
    configured = json.loads(SOURCE_FILE.read_text())["studio"]
    repository = repository or configured["repository"]
    if not repository:
        raise ValueError(
            "Provide --studio-repository or set studio.repository in sources.json"
        )
    return repository, ref or configured["ref"]


def prepare(output, checkout=None, repository=None, ref=None):
    repository, ref = source_config(repository, ref)
    output = Path(output)
    if output.exists():
        if any(output.iterdir()):
            raise ValueError(f"Studio output directory is not empty: {output}")
        output.rmdir()
    output.mkdir(parents=True)
    with tempfile.TemporaryDirectory(prefix="agentkit-studio-source-") as directory:
        work = Path(directory)
        source_repo = work / "repository"
        revision = fetch_studio(repository, ref, source_repo, checkout)
        archive = work / "studio.tar"
        subprocess.run(
            [
                "git",
                "-C",
                str(source_repo),
                "archive",
                "--format=tar",
                f"--output={archive}",
                f"{revision}:servers/harness_app_server",
            ],
            check=True,
        )
        safe_extract(archive, output)
    if not (output / "requirements.lock").is_file():
        raise ValueError("Resolved Studio commit has no Harness service requirements")
    return revision


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument(
        "--studio-checkout",
        type=Path,
        help="Optional local Git object cache; its checkout is not changed",
    )
    parser.add_argument("--studio-repository")
    parser.add_argument("--studio-ref")
    args = parser.parse_args()
    repository, ref = source_config(args.studio_repository, args.studio_ref)
    revision = prepare(args.output, args.studio_checkout, repository, ref)
    print(f"Studio source: {repository}@{ref} -> {revision}")
    print(f"::set-output name=studio_revision::{revision}")
    print(f"::set-output name=studio_repository::{repository}")
    print(f"::set-output name=studio_ref::{ref}")


if __name__ == "__main__":
    main()
