#!/usr/bin/env bash
# Download the pinned HyperQueue release binary into ./bin/hq (for clusters
# without Nix). The release is the one in src/gutsrun/hq-release.toml.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
release="$root/src/gutsrun/hq-release.toml"
# The value of `key = "value"` in the release file.
read_release() { sed -n "s/^$1 = \"\(.*\)\"\$/\1/p" "$release"; }
HQ_VERSION=$(read_release version)
HQ_SHA256=$(read_release sha256)
if [[ -z $HQ_VERSION || -z $HQ_SHA256 ]]; then
    echo "no version or sha256 in $release" >&2
    exit 1
fi

tarball="hq-v${HQ_VERSION}-linux-x64.tar.gz"
url="https://github.com/It4innovations/hyperqueue/releases/download/v${HQ_VERSION}/${tarball}"

mkdir -p "$root/bin"
cd "$root/bin"
curl -fsSL -o "$tarball" "$url"
echo "${HQ_SHA256}  ${tarball}" | sha256sum -c -
tar xzf "$tarball"
rm "$tarball"
./hq --version
