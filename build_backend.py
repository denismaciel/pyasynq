"""uv_build packaging plus Cargo's native extension (PEP 517/660).

uv_build itself only builds pure Python wheels. This adapter owns compilation,
platform tags and RECORD; uv_build owns Python files, metadata and sdists.
Build on the target architecture; Linux portability is handled by auditwheel.
"""

from __future__ import annotations

import base64
import csv
import hashlib
import io
import json
import os
import re
import shutil
import subprocess
import sys
import sysconfig
import tempfile
import zipfile
from pathlib import Path

from packaging.tags import mac_platforms, sys_tags

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib
import uv_build

ROOT = Path(__file__).parent


def _config():
    with (ROOT / "pyproject.toml").open("rb") as handle:
        project = tomllib.load(handle)
    with (ROOT / "Cargo.toml").open("rb") as handle:
        cargo = tomllib.load(handle)
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", project["project"]["version"]):
        raise ValueError("Release version must be numeric X.Y.Z")
    if project["project"]["version"] != cargo["package"]["version"]:
        raise ValueError("pyproject.toml and Cargo.toml versions must match")
    return project["tool"]["native-build"], project["tool"]["uv"]["build-backend"]


def _native():
    config, layout = _config()
    env = os.environ.copy()
    env["PYO3_PYTHON"] = sys.executable
    env.setdefault("CARGO_BUILD_JOBS", "2")
    rust_cfg = ["rustc", "--print", "cfg"]
    if env.get("CARGO_BUILD_TARGET"):
        rust_cfg += ["--target", env["CARGO_BUILD_TARGET"]]
    if 'target_env="musl"' in subprocess.check_output(rust_cfg, env=env, text=True).splitlines():
        # Rust's musl target defaults to a static CRT, which cannot produce our
        # cdylib. Apply this to source installs too, not only release builders.
        if "CARGO_ENCODED_RUSTFLAGS" in env:
            flags = env["CARGO_ENCODED_RUSTFLAGS"]
            env["CARGO_ENCODED_RUSTFLAGS"] = (flags + "\x1f" if flags else "") + "-Ctarget-feature=-crt-static"
        else:
            env["RUSTFLAGS"] = env.get("RUSTFLAGS", "") + " -C target-feature=-crt-static"
    command = ["cargo", "build", "--locked", "--release", "--lib", "--message-format=json-render-diagnostics"]
    if config.get("cargo-features"):
        command += ["--features", ",".join(config["cargo-features"])]
    result = subprocess.run(command, cwd=ROOT, env=env, text=True, stdout=subprocess.PIPE, check=True)
    artifacts = []
    for line in result.stdout.splitlines():
        message = json.loads(line)
        if message.get("reason") == "compiler-artifact" and message["target"]["name"] == config["cargo-library"]:
            artifacts += [Path(name) for name in message["filenames"] if name.endswith((".so", ".dylib", ".dll"))]
    if len(artifacts) != 1:
        raise RuntimeError(f"Expected one native library, found {artifacts}")
    free_threaded = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))
    platform = (
        sysconfig.get_platform().replace("-", "_").replace(".", "_")
        if sys.platform.startswith("linux")
        else next(sys_tags()).platform
    )
    if sys.platform == "darwin" and os.environ.get("MACOSX_DEPLOYMENT_TARGET"):
        deployment = tuple(int(part) for part in os.environ["MACOSX_DEPLOYMENT_TARGET"].split("."))
        platform = next(mac_platforms(version=deployment[:2]))
    if free_threaded:
        python_tag = f"cp{sys.version_info.major}{sys.version_info.minor}"
        tag = f"{python_tag}-{python_tag}t-{platform}"
        suffix = sysconfig.get_config_var("EXT_SUFFIX")
    else:
        tag = f"{config['abi3-minimum']}-abi3-{platform}"
        suffix = ".pyd" if sys.platform == "win32" else ".abi3.so"
    module = config["module-name"].replace(".", "/") + suffix
    return artifacts[0], module, tag, layout["module-root"]


def _rewrite(wheel_directory, wheel_name, tag, native=None):
    source = Path(wheel_directory) / wheel_name
    with zipfile.ZipFile(source) as archive:
        files = {name: archive.read(name) for name in archive.namelist() if not name.endswith("/")}
    record = next(name for name in files if name.endswith(".dist-info/RECORD"))
    metadata = record.removesuffix("RECORD") + "WHEEL"
    lines = files[metadata].decode().splitlines()
    files[metadata] = (
        "\n".join(line for line in lines if not line.startswith(("Tag:", "Root-Is-Purelib:")))
        + f"\nRoot-Is-Purelib: false\nTag: {tag}\n"
    ).encode()
    if native:
        artifact, module = native
        files[module] = artifact.read_bytes()
    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n")
    for name, data in sorted(files.items()):
        if name != record:
            digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
            writer.writerow([name, f"sha256={digest}", len(data)])
    writer.writerow([record, "", ""])
    files[record] = output.getvalue().encode()
    filename = wheel_name.rsplit("-", 3)[0] + "-" + tag + ".whl"
    destination = Path(wheel_directory) / filename
    with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, data in files.items():
            archive.writestr(name, data)
    if source != destination:
        source.unlink()
    return filename


def build_wheel(wheel_directory, config_settings=None, metadata_directory=None):
    artifact, module, tag, _ = _native()
    name = uv_build.build_wheel(wheel_directory, config_settings, metadata_directory)
    return _rewrite(wheel_directory, name, tag, (artifact, module))


def build_editable(wheel_directory, config_settings=None, metadata_directory=None):
    artifact, module, tag, source = _native()
    destination = ROOT / source / module
    destination.parent.mkdir(parents=True, exist_ok=True)
    # Atomic replacement keeps a running interpreter's mapped library intact.
    with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as handle:
        temporary = Path(handle.name)
    try:
        shutil.copyfile(artifact, temporary)
        temporary.replace(destination)
    finally:
        temporary.unlink(missing_ok=True)
    name = uv_build.build_editable(wheel_directory, config_settings, metadata_directory)
    return _rewrite(wheel_directory, name, tag)


def build_sdist(sdist_directory, config_settings=None):
    _config()
    return uv_build.build_sdist(sdist_directory, config_settings)
