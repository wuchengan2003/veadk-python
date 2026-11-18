# Copyright (c) 2025 Beijing Volcano Engine Technology Co., Ltd. and/or its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Check deduplication boundaries and Python/symlink runtime semantics."""

import importlib.util
import os
import py_compile
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location(
    "pack_runtime", Path(__file__).with_name("pack_runtime.py")
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def packed(destination, path):
    return destination / path.relative_to("/")


def test_shares_identical_files_but_preserves_versions_and_modes(tmp_path):
    roots = [tmp_path / "studio", tmp_path / "headroom"]
    for root in roots:
        root.mkdir()
        (root / "shared.py").write_text("VALUE = 1\n")
        os.utime(root / "shared.py", ns=(1000000000, 1000000000))
    (roots[1] / "different.py").write_text("VALUE = 2\n")
    for name, mode, timestamp in [
        ("executable.py", 0o755, 1000000000),
        ("newer.py", 0o644, 2000000000),
    ]:
        path = roots[1] / name
        path.write_text("VALUE = 1\n")
        path.chmod(mode)
        os.utime(path, ns=(timestamp, timestamp))
    destination = tmp_path / "packed"
    result = module.pack(destination, roots)
    original = packed(destination, roots[0] / "shared.py")
    shared = packed(destination, roots[1] / "shared.py")
    assert original.samefile(shared)
    assert original.samefile(packed(destination, roots[1] / "newer.py"))
    assert original.stat().st_mtime_ns == 0
    for name in ("different.py", "executable.py"):
        assert not original.samefile(packed(destination, roots[1] / name))
    assert result["hardlinked_files"] == 2
    assert result["hardlinked_bytes"] == 2 * original.stat().st_size
    assert not (roots[0] / "shared.py").samefile(roots[1] / "shared.py")


def test_preserves_symlinks_sourceless_bytecode_and_nonroot_readability(tmp_path):
    root = tmp_path / "runtime"
    root.mkdir(mode=0o700)
    source = root / "module.py"
    source.write_text("VALUE = 42\n")
    source.chmod(0o600)
    cache = Path(py_compile.compile(str(source), doraise=True))
    legacy = root / "legacy.pyc"
    legacy.write_bytes(cache.read_bytes())
    (root / "python").symlink_to("/usr/local/bin/python")
    (root / "cache-ref").symlink_to("__pycache__", target_is_directory=True)
    destination = tmp_path / "packed"
    result = module.pack(destination, [root])
    target = packed(destination, root)
    assert not packed(destination, cache).exists()
    assert (target / "legacy.pyc").read_bytes() == legacy.read_bytes()
    assert (target / "python").is_symlink()
    assert os.readlink(target / "python") == "/usr/local/bin/python"
    assert os.readlink(target / "cache-ref") == "__pycache__"
    assert (target / "module.py").stat().st_mode & 0o444 == 0o444
    assert target.stat().st_mode & 0o555 == 0o555
    assert result["bytecode_bytes_removed"] == cache.stat().st_size
    assert cache.exists()
    loaded = importlib.util.spec_from_file_location(
        "packed_module", target / "module.py"
    )
    instance = importlib.util.module_from_spec(loaded)
    loaded.loader.exec_module(instance)
    assert instance.VALUE == 42


def test_rejects_existing_output_and_symlink_root(tmp_path):
    with pytest.raises(FileExistsError):
        module.pack(tmp_path, [])
    alias = tmp_path / "alias"
    alias.symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(ValueError, match="absolute runtime directory"):
        module.pack(tmp_path / "packed", [alias])
