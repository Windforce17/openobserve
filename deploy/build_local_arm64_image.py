#!/usr/bin/env python3
"""Cross-compile OpenObserve on an Apple Silicon Mac and package the image.

Rust compilation happens on the host through Kache and cargo-zigbuild. Docker
only receives the finished Linux/ARM64 ELF and builds the small runtime image.
No registry push or cluster mutation is performed by this script.
"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import uuid
from collections.abc import Callable
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
WEB = REPO / "web"
TARGET = "aarch64-unknown-linux-gnu"
PROFILE = "release-prod"
BINARY = REPO / "target" / TARGET / PROFILE / "openobserve"
DOCKERFILE = REPO / "deploy" / "build" / "Dockerfile.local.aarch64"
EXPECTED_KACHE_VERSION = "0.16.0"
EXPECTED_CARGO_ZIGBUILD_VERSION = "0.23.3"
EXPECTED_ZIG_VERSION = "0.16.0"


@dataclasses.dataclass(frozen=True)
class RepositoryState:
    commit: str
    status: str
    fingerprint: str


def run(command: list[str], *, cwd: Path = REPO, env: dict[str, str] | None = None) -> None:
    print(f"+ {' '.join(command)}", flush=True)
    subprocess.run(command, cwd=cwd, env=env, check=True)


def output(command: list[str], *, cwd: Path = REPO) -> str:
    return subprocess.run(
        command,
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def output_bytes(command: list[str], *, cwd: Path = REPO) -> bytes:
    return subprocess.run(
        command,
        cwd=cwd,
        check=True,
        capture_output=True,
    ).stdout


def require_tool(name: str, install_hint: str) -> None:
    if shutil.which(name) is None:
        raise RuntimeError(f"required tool '{name}' was not found; {install_hint}")


def require_version(command: list[str], expected: str, tool: str) -> str:
    version = output(command)
    if expected not in version:
        raise RuntimeError(
            f"{tool} version mismatch: expected {expected}, got {version!r}"
        )
    return version


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_tree(root: Path) -> str:
    if not root.is_dir():
        raise RuntimeError(f"required directory does not exist: {root}")

    digest = hashlib.sha256()
    paths = sorted(root.rglob("*"), key=lambda path: os.fsencode(path.relative_to(root)))
    for path in paths:
        relative = os.fsencode(path.relative_to(root))
        digest.update(relative)
        digest.update(b"\0")
        if path.is_symlink():
            digest.update(b"symlink\0")
            digest.update(os.fsencode(os.readlink(path)))
        elif path.is_file():
            digest.update(b"file\0")
            digest.update(sha256_file(path).encode())
        elif path.is_dir():
            digest.update(b"directory\0")
        else:
            raise RuntimeError(f"unsupported filesystem entry in {root}: {path}")
        digest.update(b"\0")
    return digest.hexdigest()


def repository_state() -> RepositoryState:
    commit = output(["git", "rev-parse", "HEAD"])
    status_bytes = output_bytes(
        ["git", "status", "--porcelain=v1", "-z", "--untracked-files=all"]
    )
    diff_bytes = output_bytes(["git", "diff", "--binary", "--no-ext-diff", "HEAD", "--"])
    untracked_bytes = output_bytes(
        ["git", "ls-files", "--others", "--exclude-standard", "-z"]
    )

    digest = hashlib.sha256()
    digest.update(commit.encode())
    digest.update(b"\0status\0")
    digest.update(status_bytes)
    digest.update(b"\0diff\0")
    digest.update(diff_bytes)
    digest.update(b"\0untracked\0")
    for raw_path in filter(None, untracked_bytes.split(b"\0")):
        path = REPO / os.fsdecode(raw_path)
        digest.update(raw_path)
        digest.update(b"\0")
        if path.is_symlink():
            digest.update(os.fsencode(os.readlink(path)))
        else:
            digest.update(sha256_file(path).encode())
        digest.update(b"\0")

    return RepositoryState(
        commit=commit,
        status=status_bytes.replace(b"\0", b"\n").decode(errors="replace").strip(),
        fingerprint=digest.hexdigest(),
    )


def validate_repository_unchanged(initial: RepositoryState) -> None:
    current = repository_state()
    if current != initial:
        raise RuntimeError(
            "repository changed while the build was running; refusing to package a "
            "mixed-source binary\n"
            f"initial commit={initial.commit} fingerprint={initial.fingerprint}\n"
            f"current commit={current.commit} fingerprint={current.fingerprint}\n"
            f"current status:\n{current.status or '(clean)'}"
        )


def validate_web_dist_unchanged(expected_sha256: str) -> None:
    current_sha256 = sha256_tree(WEB / "dist")
    if current_sha256 != expected_sha256:
        raise RuntimeError(
            "web/dist changed while Rust or image packaging was running; refusing "
            "to package a mixed-source binary\n"
            f"expected web/dist SHA-256={expected_sha256}\n"
            f"current web/dist SHA-256={current_sha256}"
        )


def validate_host() -> None:
    if sys.platform != "darwin" or platform.machine() != "arm64":
        raise RuntimeError(
            "this build path requires an Apple Silicon Mac "
            f"(found {sys.platform}/{platform.machine()})"
        )
    require_tool("kache", "install Kache 0.16.0 first")
    require_tool("zig", "install Zig first (for example: brew install zig)")
    require_tool(
        "cargo-zigbuild",
        "install it with: kache cargo install cargo-zigbuild --version 0.23.3 --locked",
    )
    require_tool("npm", "install Node.js 24 and npm")
    require_tool("docker", "start Docker/OrbStack first")
    require_tool("file", "install the macOS command-line tools")

    versions = [
        require_version(
            ["kache", "--version"], EXPECTED_KACHE_VERSION, "Kache"
        ),
        require_version(
            ["cargo-zigbuild", "--version"],
            EXPECTED_CARGO_ZIGBUILD_VERSION,
            "cargo-zigbuild",
        ),
        f"zig {require_version(['zig', 'version'], EXPECTED_ZIG_VERSION, 'Zig')}",
        output(["rustc", "--version"]),
        f"node {output(['node', '--version'])}",
        f"npm {output(['npm', '--version'])}",
    ]
    print("toolchain: " + ", ".join(versions), flush=True)


def validate_binary(commit: str) -> None:
    if not BINARY.is_file():
        raise RuntimeError(f"cross-build did not create expected binary: {BINARY}")

    description = output(["file", str(BINARY)])
    if "ELF 64-bit" not in description or "ARM aarch64" not in description:
        raise RuntimeError(f"expected an ARM64 Linux ELF, got: {description}")

    for needle, description_text in (("mimalloc", "mimalloc"), (commit, "Git commit")):
        result = subprocess.run(
            ["grep", "-a", "-q", needle, str(BINARY)],
            cwd=REPO,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"binary validation failed: embedded {description_text} '{needle}' was not found"
            )

    print(f"binary: {description}", flush=True)
    print(
        f"binary sha256={sha256_file(BINARY)} size={BINARY.stat().st_size / 1_000_000:.1f}MB",
        flush=True,
    )


def build_runtime_image(
    image: str,
    commit: str,
    web_dist_sha256: str,
    pre_tag_check: Callable[[], None],
) -> None:
    binary_sha256 = sha256_file(BINARY)
    validation_image = (
        f"openobserve-local-arm64-validation:{commit[:12]}-{uuid.uuid4().hex[:12]}"
    )
    with tempfile.TemporaryDirectory(prefix="openobserve-arm64-image-") as directory:
        context = Path(directory)
        shutil.copy2(BINARY, context / "openobserve")
        (context / "GIT_COMMIT").write_text(f"{commit}\n", encoding="utf-8")

        try:
            run(
                [
                    "docker",
                    "build",
                    "--platform",
                    "linux/arm64",
                    "--build-arg",
                    f"GIT_COMMIT={commit}",
                    "--build-arg",
                    f"BINARY_SHA256={binary_sha256}",
                    "--build-arg",
                    f"WEB_DIST_SHA256={web_dist_sha256}",
                    "-f",
                    str(DOCKERFILE),
                    "-t",
                    validation_image,
                    str(context),
                ]
            )

            inspection = json.loads(
                output(["docker", "image", "inspect", validation_image])
            )[0]
            architecture = inspection.get("Architecture")
            labels = inspection.get("Config", {}).get("Labels", {})
            if architecture != "arm64":
                raise RuntimeError(
                    f"image architecture is {architecture!r}, expected 'arm64'"
                )
            for label, expected in (
                ("git_commit", commit),
                ("org.opencontainers.image.revision", commit),
                ("org.opencontainers.image.openobserve.binary.sha256", binary_sha256),
                (
                    "org.opencontainers.image.openobserve.web-dist.sha256",
                    web_dist_sha256,
                ),
            ):
                if labels.get(label) != expected:
                    raise RuntimeError(
                        f"image label {label!r} is {labels.get(label)!r}, "
                        f"expected {expected!r}"
                    )

            verification_container = (
                f"openobserve-local-arm64-verification-{uuid.uuid4().hex[:12]}"
            )
            extracted_binary = context / "verified-openobserve"
            extracted_commit = context / "verified-GIT_COMMIT"
            try:
                run(
                    [
                        "docker",
                        "container",
                        "create",
                        "--name",
                        verification_container,
                        validation_image,
                    ]
                )
                run(
                    [
                        "docker",
                        "container",
                        "cp",
                        f"{verification_container}:/openobserve",
                        str(extracted_binary),
                    ]
                )
                run(
                    [
                        "docker",
                        "container",
                        "cp",
                        f"{verification_container}:/GIT_COMMIT",
                        str(extracted_commit),
                    ]
                )
            finally:
                subprocess.run(
                    ["docker", "container", "rm", verification_container],
                    cwd=REPO,
                    check=False,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )

            image_binary_sha256 = sha256_file(extracted_binary)
            if image_binary_sha256 != binary_sha256:
                raise RuntimeError(
                    f"image binary SHA-256 is {image_binary_sha256!r}, "
                    f"expected {binary_sha256!r}"
                )
            file_commit = extracted_commit.read_text(encoding="utf-8").strip()
            if file_commit != commit:
                raise RuntimeError(
                    f"image /GIT_COMMIT is {file_commit!r}, expected {commit!r}"
                )

            # Run exactly as the production pod does: Linux/ARM64, uid 10000,
            # gid 3000, and a writable /data volume. This catches loader,
            # dynamic-library, permission, and runtime-image incompatibility.
            run(
                [
                    "docker",
                    "run",
                    "--rm",
                    "--user",
                    "10000:3000",
                    "--tmpfs",
                    "/data:rw,uid=10000,gid=3000,mode=0770",
                    "--entrypoint",
                    "/openobserve",
                    validation_image,
                    "init-dir",
                    "-p",
                    "/data/",
                ]
            )

            final_image_id = inspection.get("Id")
            if not final_image_id:
                raise RuntimeError("validated image has no Docker image ID")

            # This is deliberately the last fallible check before changing
            # the requested target tag. A failed source/provenance gate leaves
            # any existing target tag untouched.
            pre_tag_check()
            run(["docker", "image", "tag", validation_image, image])
            print(
                f"OK: built local linux/arm64 image {image} image_id={final_image_id} "
                f"commit={commit} binary_sha256={binary_sha256}",
                flush=True,
            )
        finally:
            subprocess.run(
                ["docker", "image", "rm", validation_image],
                cwd=REPO,
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--image",
        default="openobserve:local-arm64",
        help="local image name/tag (the script never pushes it)",
    )
    parser.add_argument(
        "--allow-dirty",
        action="store_true",
        help="allow a dirty worktree for pre-commit validation only",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    validate_host()

    initial_state = repository_state()
    if initial_state.status and not args.allow_dirty:
        raise RuntimeError(
            "repository is dirty; commit/stash changes or use --allow-dirty only for a "
            f"non-production validation build:\n{initial_state.status}"
        )

    commit = initial_state.commit
    run(["npm", "ci", "--no-audit", "--no-fund"], cwd=WEB)
    web_env = os.environ.copy()
    web_env["NODE_OPTIONS"] = "--max-old-space-size=8192"
    run(["npm", "run", "build"], cwd=WEB, env=web_env)
    validate_repository_unchanged(initial_state)
    web_dist_sha256 = sha256_tree(WEB / "dist")
    print(f"web/dist sha256={web_dist_sha256}", flush=True)

    # The build script embeds the current Git SHA/date. Cleaning only the
    # metadata and web crates prevents Cargo from reusing an old commit or UI
    # while preserving the expensive dependency graph and Kache.
    run(["cargo", "clean", "-p", "config", "-p", "web", "--target", TARGET])
    build_env = os.environ.copy()
    build_env.update(
        {
            "CARGO_INCREMENTAL": "0",
            "KACHE_MAX_SIZE": "150GiB",
        }
    )
    run(
        [
            "kache",
            "cargo",
            "zigbuild",
            "--locked",
            "--profile",
            PROFILE,
            "--features",
            "mimalloc",
            "--target",
            TARGET,
        ],
        env=build_env,
    )

    validate_repository_unchanged(initial_state)
    validate_web_dist_unchanged(web_dist_sha256)
    validate_binary(commit)
    run(["kache", "stats"])

    def pre_tag_check() -> None:
        validate_repository_unchanged(initial_state)
        validate_web_dist_unchanged(web_dist_sha256)

    build_runtime_image(args.image, commit, web_dist_sha256, pre_tag_check)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("ERROR: build canceled by user", file=sys.stderr)
        raise SystemExit(130)
    except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(1)
