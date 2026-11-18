"""Authenticate the CI build and read back its pushed registry manifest."""

import base64
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from urllib.error import HTTPError
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen


def settings():
    image = os.environ.get("VOLC_CR_IMAGE", "").strip()
    if not re.fullmatch(r"[a-z0-9.-]+\.cr\.volces\.com/[a-z0-9_./-]+", image):
        raise ValueError("VOLC_CR_IMAGE must be a complete CR repository without a tag")
    user = os.environ.get("VOLC_CR_USERNAME", "")
    password = os.environ.get("VOLC_CR_PASSWORD", "")
    if not user or not password:
        raise ValueError("Missing VOLC_CR_USERNAME or VOLC_CR_PASSWORD")
    tag = os.environ.get("VOLC_CR_TAG")
    if tag is None:
        tag = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}", tag):
        raise ValueError("Invalid VOLC_CR_TAG")
    return image, tag, base64.b64encode(f"{user}:{password}".encode()).decode()


def auth_file():
    return Path.home() / ".docker" / "config.json"


def prepare():
    image, sha, auth = settings()
    path = auth_file()
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    config = json.loads(path.read_text()) if path.exists() else {}
    config.setdefault("auths", {})[image.split("/")[0]] = {"auth": auth}
    path.touch(mode=0o600, exist_ok=True)
    path.chmod(0o600)
    path.write_text(json.dumps(config))
    print(f"Registry authentication prepared for image tag {sha}")


def manifest_digest():
    """Read an authenticated manifest; HTTP errors remain distinguishable."""
    image, sha, auth = settings()
    registry, repository = image.split("/", 1)
    url = f"https://{registry}/v2/{repository}/manifests/{sha}"
    headers = {
        "Accept": ", ".join(
            [
                "application/vnd.oci.image.index.v1+json",
                "application/vnd.oci.image.manifest.v1+json",
                "application/vnd.docker.distribution.manifest.list.v2+json",
                "application/vnd.docker.distribution.manifest.v2+json",
            ]
        ),
        "Authorization": f"Basic {auth}",
    }
    try:
        response = urlopen(Request(url, headers=headers), timeout=60)
    except HTTPError as error:
        if error.code != 401:
            raise
        challenge = error.headers.get("WWW-Authenticate", "")
        if not challenge.lower().startswith("bearer "):
            raise ValueError("Unexpected registry authentication challenge") from None
        params = dict(re.findall(r'(\w+)="([^"]*)"', challenge))
        realm = params.pop("realm")
        parsed = urlsplit(realm)
        if parsed.scheme != "https" or parsed.hostname != registry:
            raise ValueError("Unexpected registry token endpoint")
        token_url = realm + ("&" if parsed.query else "?") + urlencode(params)
        request = Request(token_url, headers={"Authorization": f"Basic {auth}"})
        with urlopen(request, timeout=60) as token_response:
            result = json.load(token_response)
        headers["Authorization"] = "Bearer " + (
            result.get("token") or result["access_token"]
        )
        response = urlopen(Request(url, headers=headers), timeout=60)
    with response:
        data = response.read()
        digest = "sha256:" + hashlib.sha256(data).hexdigest()
        if response.headers.get("Docker-Content-Digest", digest) != digest:
            raise ValueError("Registry manifest digest mismatch")
        json.loads(data)
    return digest


def require_absent():
    """Fail closed: only a registry manifest-not-found response permits release."""
    try:
        manifest_digest()
    except HTTPError as error:
        if error.code != 404:
            raise
        with error:
            body = json.load(error)
        errors = body.get("errors", [])
        if errors and all(
            e.get("code") in {"MANIFEST_UNKNOWN", "NAME_UNKNOWN"} for e in errors
        ):
            return
        raise ValueError("Registry did not confirm that the tag is absent") from None
    print(
        "Image tag already exists; bump docker/harness-runtime/VERSION", file=sys.stderr
    )
    raise ValueError("Image tag already exists; bump docker/harness-runtime/VERSION")


def verify():
    image, sha, _ = settings()
    digest = manifest_digest()
    print(f"Verified image: {image}:{sha}")
    print(f"Manifest digest: {digest}")
    print(f"::set-output name=digest::{digest}")


def cleanup():
    image = os.environ.get("VOLC_CR_IMAGE", "")
    path = auth_file()
    if path.exists() and image:
        config = json.loads(path.read_text())
        config.get("auths", {}).pop(image.split("/")[0], None)
        path.write_text(json.dumps(config))


if __name__ == "__main__":
    try:
        {
            "prepare": prepare,
            "verify": verify,
            "cleanup": cleanup,
            "require-absent": require_absent,
        }[sys.argv[1]]()
    except Exception as error:
        # Never include credentials, token responses or HTTP request headers.
        print(f"Registry step failed: {type(error).__name__}", file=sys.stderr)
        sys.exit(1)
