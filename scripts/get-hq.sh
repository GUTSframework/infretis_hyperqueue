#!/usr/bin/env bash
# Download the pinned HyperQueue release binary into ./bin/hq (for clusters
# without Nix). Keep the version and hash in sync with flake.nix.
set -euo pipefail

HQ_VERSION=0.26.2
HQ_SHA256=e15dae9113e1a307a97a66bfe90f74f78c6016239436b5d9f1e4efec480e84b5

root="$(cd "$(dirname "$0")/.." && pwd)"
tarball="hq-v${HQ_VERSION}-linux-x64.tar.gz"
url="https://github.com/It4innovations/hyperqueue/releases/download/v${HQ_VERSION}/${tarball}"

mkdir -p "$root/bin"
cd "$root/bin"
curl -fsSL -o "$tarball" "$url"
echo "${HQ_SHA256}  ${tarball}" | sha256sum -c -
tar xzf "$tarball"
rm "$tarball"
./hq --version
