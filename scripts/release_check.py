"""Check version identity, wheel RECORD, installed import and sdist rebuild."""

import argparse
import base64
import csv
import hashlib
import io
import os
import re
import subprocess
import tarfile
import tempfile
import zipfile
from pathlib import Path

import tomllib

ROOT = Path(__file__).resolve().parents[1]


def run(*args, cwd=None):
    env = os.environ.copy()
    env.setdefault("CARGO_TARGET_DIR", str(ROOT / "target"))
    subprocess.run(args, cwd=cwd, env=env, check=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tag")
    parser.add_argument("--version-only", action="store_true")
    args = parser.parse_args()
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())
    version = project["project"]["version"]
    assert re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version), "Release version must be numeric X.Y.Z"
    assert version == tomllib.loads((ROOT / "Cargo.toml").read_text())["package"]["version"]
    if args.tag:
        assert args.tag == f"v{version}", f"tag {args.tag!r} must match v{version}"
    if args.version_only:
        return
    with tempfile.TemporaryDirectory(prefix="spater-release-") as directory:
        work = Path(directory)
        run("uv", "build", "--no-sources", "--out-dir", str(work / "dist"), cwd=ROOT)
        (wheel,) = (work / "dist").glob("*.whl")
        with zipfile.ZipFile(wheel) as archive:
            assert any(name.endswith((".so", ".pyd")) for name in archive.namelist())
            (record,) = [name for name in archive.namelist() if name.endswith(".dist-info/RECORD")]
            for name, digest, size in csv.reader(io.StringIO(archive.read(record).decode())):
                assert not name.endswith("/"), "RECORD must not contain directory entries"
                if not digest:
                    assert name == record
                    continue
                data = archive.read(name)
                assert int(size) == len(data)
                assert (
                    digest == "sha256=" + base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
                )
        run(
            "uv",
            "run",
            "--isolated",
            "--no-project",
            "--with",
            str(wheel),
            "python",
            str(ROOT / "scripts" / "smoke_wheel.py"),
            cwd=work,
        )
        (sdist,) = (work / "dist").glob("*.tar.gz")
        with tarfile.open(sdist) as archive:
            archive.extractall(work / "source", filter="data")
        (source,) = (work / "source").iterdir()
        run("uv", "build", "--wheel", "--no-sources", "--out-dir", str(work / "rebuilt"), cwd=source)


if __name__ == "__main__":
    main()
