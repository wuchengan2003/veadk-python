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

"""Pack isolated runtime trees, sharing only identical immutable files."""

import hashlib
import importlib.util
import json
import os
import shutil
import stat
import sys
from pathlib import Path


def source_cache(path):
    if path.suffix != ".pyc":
        return False
    try:
        source = Path(importlib.util.source_from_cache(str(path)))
    except ValueError:
        source = path.with_suffix(".py")
    return source.is_file()


def pack(destination, roots):
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=False)
    stats = {"bytecode_bytes_removed": 0, "hardlinked_bytes": 0, "hardlinked_files": 0}

    def ignore(directory, names):
        skipped = []
        for name in names:
            path = Path(directory) / name
            # 保留独立发布的无源码 .pyc；只移除可以从源码重新生成的缓存。
            if not path.is_symlink() and path.is_file() and source_cache(path):
                stats["bytecode_bytes_removed"] += path.stat().st_size
                skipped.append(name)
        return skipped

    for root in roots:
        root = Path(root)
        if not root.is_absolute() or root.is_symlink() or ".." in root.parts:
            raise ValueError(f"Expected an absolute runtime directory: {root}")
        shutil.copytree(
            root, destination / root.relative_to("/"), symlinks=True, ignore=ignore
        )

    seen = {}
    for directory, directories, files in os.walk(destination):
        directories.sort()
        folder = Path(directory)
        folder.chmod(stat.S_IMODE(folder.stat().st_mode) | 0o555)
        for name in sorted(files):
            path = Path(directory) / name
            info = path.lstat()
            if not stat.S_ISREG(info.st_mode) or not info.st_size:
                continue
            mode = stat.S_IMODE(info.st_mode) | 0o444
            if mode & 0o111:
                mode |= 0o111
            path.chmod(mode)
            # 安装时间不属于运行时契约；规范化时间戳才能跨独立安装共享文件。
            if path.suffix != ".pyc":
                os.utime(path, ns=(0, 0))
            info = path.stat()
            # 路径和环境保持独立；只有内容及元数据一致的只读运行时文件共享 inode。
            with path.open("rb") as stream:
                digest = hashlib.file_digest(stream, "sha256").digest()
            attributes = tuple(
                (key, os.getxattr(path, key)) for key in sorted(os.listxattr(path))
            )
            key = (
                digest,
                info.st_size,
                info.st_mode,
                info.st_uid,
                info.st_gid,
                info.st_mtime_ns,
                attributes,
            )
            original = seen.get(key)
            if original is None:
                seen[key] = path
            else:
                path.unlink()
                os.link(original, path)
                stats["hardlinked_bytes"] += info.st_size
                stats["hardlinked_files"] += 1
    return stats


if __name__ == "__main__":
    print(json.dumps(pack(sys.argv[1], sys.argv[2:]), sort_keys=True))
