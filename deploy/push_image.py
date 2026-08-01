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
  4. pushed amd64 manifest digest must differ from --prev-tag's
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

def amd64_digests(profile, region, tag):
    try:
        text = out(["aws", "ecr", "batch-get-image", "--profile", profile, "--region", region,
                    "--repository-name", "devops/obs", "--image-ids", f"imageTag={tag}",
                    "--query", "images[0].imageManifest", "--output", "text"])
        m = json.loads(text)
    except Exception:
        return []
    return [s["digest"] for s in m.get("manifests", [])
            if s.get("platform", {}).get("architecture") == "amd64"]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", required=True)
    ap.add_argument("--prev-tag", help="tag whose image the new one must DIFFER from (default: NN-1)")
    args = ap.parse_args()
    tag = args.tag
    prev = args.prev_tag
    if not prev:
        base, nn = tag.rsplit(".", 1)
        prev = f"{base}.{int(nn) - 1}"

    dirty = out(["git", "-C", REPO, "status", "--porcelain"]).strip()
    if dirty:
        sys.exit(f"ABORT: repo dirty:\n{dirty}")
    commit = out(["git", "-C", REPO, "rev-parse", "HEAD"]).strip()
    # freshness is judged against the last commit touching CODE — doc/backlog
    # commits after a build must not force a pointless relink
    head_ct = int(out(["git", "-C", REPO, "log", "-1", "--format=%ct",
                       "--", "src", "Cargo.toml", "Cargo.lock", "web"]).strip())

    binary = os.path.join(REPO, "target/release/openobserve")
    if os.path.getmtime(binary) <= head_ct:
        sys.exit("ABORT: target/release/openobserve predates HEAD commit — stale artifact (rebuild!)")
    with open(binary, "rb") as f:
        data = f.read()
    if b"mimalloc" not in data:
        sys.exit("ABORT: binary does not embed mimalloc")
    sha = hashlib.sha256(data).hexdigest()
    print(f"binary sha256={sha} size={len(data)/1e6:.0f}MB commit={commit}")

    prev_digests = set(amd64_digests(*REGISTRIES[0][:2], prev))
    print(f"prev tag {prev} amd64 digests: {sorted(prev_digests) or 'none (skip digest gate)'}")

    # Ship with COMPRESSED debug sections: the perf-profiling env vars make
    # the raw binary carry full DWARF (~4.4GB since .40; 364MB before) and
    # rolls pay the pull. zlib-compressed debug keeps addr2line/perf usable
    # at a fraction of the bytes. Falls back to a plain copy if objcopy is
    # unavailable.
    ctx_bin = os.path.join(CTX, "openobserve")
    try:
        sh(["objcopy", "--compress-debug-sections=zlib", binary, ctx_bin])
        os.chmod(ctx_bin, 0o755)
        print(f"debug-compressed image binary: {os.path.getsize(ctx_bin)/1e6:.0f}MB (raw {len(data)/1e6:.0f}MB)")
    except Exception as e:
        print(f"objcopy compress failed ({e}); shipping raw binary")
        shutil.copy2(binary, ctx_bin)
    with open(os.path.join(CTX, "GIT_COMMIT"), "w") as f:
        f.write(commit + "\n")
    tags = [f"{reg}/devops/obs:{tag}" for _, _, reg in REGISTRIES]
    sh(["docker", "build", "--network=host", "--build-arg", f"GIT_COMMIT={commit}",
        *sum((["-t", t] for t in tags), []), CTX])

    for profile, region, reg in REGISTRIES:
        pw = out(["aws", "ecr", "get-login-password", "--profile", profile, "--region", region])
        subprocess.run(["docker", "login", "--username", "AWS", "--password-stdin", reg],
                       input=pw, text=True, check=True, capture_output=True)
        sh(["docker", "push", f"{reg}/devops/obs:{tag}"])

    new_digests = set(amd64_digests(*REGISTRIES[0][:2], tag))
    print(f"pushed amd64 digests: {sorted(new_digests)}")
    if prev_digests and new_digests & prev_digests:
        sys.exit(f"ABORT: pushed image is IDENTICAL to {prev} — stale binary shipped AGAIN")
    for f in ("openobserve", "GIT_COMMIT"):
        os.remove(os.path.join(CTX, f))
    print(f"OK: {tag} pushed to both registries (commit {commit[:12]}, differs from {prev})")

if __name__ == "__main__":
    main()
