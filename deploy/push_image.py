#!/usr/bin/env python3
"""Build + push the obs image with provenance gates.

Usage: python3 deploy/push_image.py --tag v0.93.0-vix-YYYYMMDD.NN [--prev-tag ...]

Exists because .37/.38 shipped a byte-identical STALE binary: a broken
release build silently left an old target/release/openobserve in place and
the image pipeline copied it. Every gate here answers that incident:
  0. repo must be clean (the imaged binary corresponds to HEAD)
  1. binary mtime must postdate the HEAD commit (no stale artifact)
  2. binary must embed mimalloc (owner directive; default cargo feature)
  3. image carries git_commit label + /GIT_COMMIT + in-image mimalloc grep
  4. pushed manifest digest must differ from --prev-tag's (gate arch:
     amd64, or arm64 under --arm64-only)
Ship verification is on the caller: check the extraction/inspector log
lines and scan_size on prod — NEVER timing alone.
"""
import argparse, hashlib, json, os, shutil, subprocess, sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CTX = os.path.dirname(os.path.abspath(__file__))
REGISTRIES = [
    ("eks-prod", "us-east-1", "665386098866.dkr.ecr.us-east-1.amazonaws.com"),
    ("eks-dev", "ap-southeast-1", "395389371795.dkr.ecr.ap-southeast-1.amazonaws.com"),
]

def sh(cmd, **kw):
    print(f"+ {' '.join(cmd)}", flush=True)
    return subprocess.run(cmd, check=True, **kw)

def out(cmd):
    return subprocess.run(cmd, capture_output=True, text=True, check=True).stdout

def arch_digests(profile, region, tag, arch):
    try:
        text = out(["aws", "ecr", "batch-get-image", "--profile", profile, "--region", region,
                    "--repository-name", "devops/obs", "--image-ids", f"imageTag={tag}",
                    "--query", "images[0].imageManifest", "--output", "text"])
        m = json.loads(text)
    except Exception:
        return []
    return [s["digest"] for s in m.get("manifests", [])
            if s.get("platform", {}).get("architecture") == arch]

def tag_sort_key(tag):
    # v0.93.0-vix-YYYYMMDD.NN -> (YYYYMMDD, NN); malformed tags sort first
    try:
        date_nn = tag.rsplit("-", 1)[1]
        date, nn = date_nn.split(".", 1)
        return (int(date), int(nn))
    except Exception:
        return (0, 0)

def latest_existing_tag(profile, region, before_tag):
    """Newest v*-vix-DATE.NN tag in ECR strictly older than before_tag.

    Exists because the naive NN-1 default silently misses across a date
    change (v...-20260810.75's NN-1 is v...-20260810.74, which never
    existed — the real predecessor was v...-20260807.74) and an absent
    prev tag DISABLED the stale-image digest gate on exactly the builds
    that most need it (first build of a day)."""
    text = out(["aws", "ecr", "list-images", "--profile", profile, "--region", region,
                "--repository-name", "devops/obs", "--filter", "tagStatus=TAGGED",
                "--query", "imageIds[].imageTag", "--output", "json"])
    tags = [t for t in json.loads(text)
            if "-vix-" in t and tag_sort_key(t) != (0, 0) and tag_sort_key(t) < tag_sort_key(before_tag)]
    return max(tags, key=tag_sort_key) if tags else None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", required=True)
    ap.add_argument("--prev-tag", help="tag whose image the new one must DIFFER from (default: NN-1)")
    ap.add_argument("--arm64", action="store_true",
                    help="also image target/aarch64-unknown-linux-gnu/release/openobserve and "
                         "push {tag} as a MULTI-ARCH manifest list (amd64+arm64). The same "
                         "provenance gates run on both binaries; per-arch images are pushed "
                         "as {tag}-amd64/{tag}-arm64 and stitched with docker manifest.")
    ap.add_argument("--arm64-only", action="store_true",
                    help="image ONLY target/aarch64-unknown-linux-gnu/release/openobserve — no "
                         "amd64 binary required. Every obs workload in BOTH envs pins "
                         "kubernetes.io/arch: arm64 (dev + prod checked 2026-08-26), so the "
                         "amd64 half stopped serving anything. {tag} is still pushed as a "
                         "manifest LIST (single arm64 member, stitched exactly like --arm64) so "
                         "the tag layout matches every earlier ship, and the stale-image digest "
                         "gate runs on the arm64 digests instead of amd64.")
    args = ap.parse_args()
    if args.arm64 and args.arm64_only:
        sys.exit("ABORT: --arm64 and --arm64-only are mutually exclusive")
    tag = args.tag
    prev = args.prev_tag
    if not prev:
        prev = latest_existing_tag(*REGISTRIES[0][:2], tag)
        if prev is None:
            sys.exit("ABORT: no existing -vix- tag older than %s in ECR; pass --prev-tag "
                     "explicitly (or confirm this is truly the first ship)" % tag)
        print(f"auto prev tag (newest in ECR before {tag}): {prev}")

    dirty = out(["git", "-C", REPO, "status", "--porcelain"]).strip()
    if dirty:
        sys.exit(f"ABORT: repo dirty:\n{dirty}")
    commit = out(["git", "-C", REPO, "rev-parse", "HEAD"]).strip()
    # freshness is judged against the last commit touching CODE — doc/backlog
    # commits after a build must not force a pointless relink
    head_ct = int(out(["git", "-C", REPO, "log", "-1", "--format=%ct",
                       "--", "src", "Cargo.toml", "Cargo.lock", "web"]).strip())

    if args.arm64_only:
        binaries = {"arm64": os.path.join(
            REPO, "target/aarch64-unknown-linux-gnu/release/openobserve")}
    else:
        binaries = {"amd64": os.path.join(REPO, "target/release/openobserve")}
        if args.arm64:
            binaries["arm64"] = os.path.join(
                REPO, "target/aarch64-unknown-linux-gnu/release/openobserve")
    gate_arch = "arm64" if args.arm64_only else "amd64"
    for arch, binary in binaries.items():
        if os.path.getmtime(binary) <= head_ct:
            sys.exit(f"ABORT: {binary} predates HEAD commit — stale artifact (rebuild!)")
        with open(binary, "rb") as f:
            data = f.read()
        if b"mimalloc" not in data:
            sys.exit(f"ABORT: {arch} binary does not embed mimalloc")
        sha = hashlib.sha256(data).hexdigest()
        print(f"{arch} binary sha256={sha} size={len(data)/1e6:.0f}MB commit={commit}")

    prev_digests = set(arch_digests(*REGISTRIES[0][:2], prev, gate_arch))
    print(f"prev tag {prev} {gate_arch} digests: {sorted(prev_digests) or 'none (skip digest gate)'}")

    # Ship with COMPRESSED debug sections: the perf-profiling env vars make
    # the raw binary carry full DWARF (~4.4GB since .40; 364MB before) and
    # rolls pay the pull. zlib-compressed debug keeps addr2line/perf usable
    # at a fraction of the bytes. Falls back to a plain copy if objcopy is
    # unavailable. Cross binaries get the target-prefixed objcopy.
    # Cross objcopy resolution: binutils' target-prefixed tool on a Linux
    # ship host; llvm-objcopy (arch-agnostic ELF support) anywhere else —
    # rustup's llvm-tools component ships one next to the active toolchain,
    # which covers a macOS ship host with no binutils cross package.
    def resolve_objcopy(candidates):
        for c in candidates:
            if shutil.which(c):
                return c
        try:
            sysroot = out(["rustc", "--print", "sysroot"]).strip()
            for p in [os.path.join(root, "llvm-objcopy")
                      for root, _, files in os.walk(os.path.join(sysroot, "lib", "rustlib"))
                      if "llvm-objcopy" in files]:
                return p
        except Exception:
            pass
        return candidates[0]  # let the call fail -> raw-binary fallback

    OBJCOPY = {"amd64": resolve_objcopy(["objcopy", "llvm-objcopy"]),
               "arm64": resolve_objcopy(["aarch64-linux-gnu-objcopy", "llvm-objcopy"])}
    PLATFORM = {"amd64": "linux/amd64", "arm64": "linux/arm64"}
    with open(os.path.join(CTX, "GIT_COMMIT"), "w") as f:
        f.write(commit + "\n")
    ctx_bin = os.path.join(CTX, "openobserve")
    multi = args.arm64 or args.arm64_only
    arch_tags = {a: (f"{tag}-{a}" if multi else tag) for a in binaries}
    for arch, binary in binaries.items():
        try:
            sh([OBJCOPY[arch], "--compress-debug-sections=zlib", binary, ctx_bin])
            os.chmod(ctx_bin, 0o755)
            print(f"{arch} debug-compressed image binary: {os.path.getsize(ctx_bin)/1e6:.0f}MB")
        except Exception as e:
            print(f"{arch} objcopy compress failed ({e}); shipping raw binary")
            shutil.copy2(binary, ctx_bin)
        tags = [f"{reg}/devops/obs:{arch_tags[arch]}" for _, _, reg in REGISTRIES]
        sh(["docker", "build", "--network=host", "--platform", PLATFORM[arch],
            "--build-arg", f"GIT_COMMIT={commit}",
            *sum((["-t", t] for t in tags), []), CTX])

    for profile, region, reg in REGISTRIES:
        pw = out(["aws", "ecr", "get-login-password", "--profile", profile, "--region", region])
        subprocess.run(["docker", "login", "--username", "AWS", "--password-stdin", reg],
                       input=pw, text=True, check=True, capture_output=True)
        for arch in binaries:
            sh(["docker", "push", f"{reg}/devops/obs:{arch_tags[arch]}"])
        if multi:
            # one tag serving both archs: nodes pull their platform's image,
            # so the migration/rollback is manifest-only. imagetools (not
            # `docker manifest create`) because BuildKit pushes each arch
            # tag as its own manifest LIST (provenance wrapper), which the
            # legacy command refuses to nest.
            sh(["docker", "buildx", "imagetools", "create",
                "-t", f"{reg}/devops/obs:{tag}",
                *[f"{reg}/devops/obs:{arch_tags[a]}" for a in binaries]])

    new_digests = set(arch_digests(*REGISTRIES[0][:2], tag, gate_arch))
    print(f"pushed {gate_arch} digests: {sorted(new_digests)}")
    if prev_digests and new_digests & prev_digests:
        sys.exit(f"ABORT: pushed image is IDENTICAL to {prev} — stale binary shipped AGAIN")
    if args.arm64 or args.arm64_only:
        arm = [s for m in [json.loads(out(["aws", "ecr", "batch-get-image", "--profile",
                REGISTRIES[0][0], "--region", REGISTRIES[0][1],
                "--repository-name", "devops/obs", "--image-ids", f"imageTag={tag}",
                "--query", "images[0].imageManifest", "--output", "text"]))]
               for s in m.get("manifests", [])
               if s.get("platform", {}).get("architecture") == "arm64"]
        if not arm:
            sys.exit("ABORT: multi-arch push requested but the manifest list carries no arm64 entry")
        print(f"pushed arm64 digests: {[s['digest'] for s in arm]}")
    for f in ("openobserve", "GIT_COMMIT"):
        os.remove(os.path.join(CTX, f))
    print(f"OK: {tag} pushed to both registries (commit {commit[:12]}, differs from {prev})")

if __name__ == "__main__":
    main()
