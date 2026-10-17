#!/usr/bin/env bash
set -euo pipefail
export LC_ALL=C

root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
version=${1:-dev}
version=${version#v}
arch=${2:?usage: package.sh VERSION ARCH [LIBRARY] [OUTPUT_DIRECTORY]}
library=${3:-build/deepseek-search.so}
output=${4:-dist}

if [[ ! $version =~ ^[[:alnum:]][[:alnum:].+_-]*$ ]]; then
  echo "Invalid release version: $version" >&2
  exit 1
fi
case "$arch" in
  amd64) machine='Advanced Micro Devices X86-64' ;;
  arm64) machine='AArch64' ;;
  *) echo "Unsupported release architecture: $arch" >&2; exit 1 ;;
esac

cd "$root"
header=$(readelf --file-header "$library")
if ! grep -Eq 'Class: +ELF64' <<<"$header" ||
   ! grep -Eq 'Type: +DYN ' <<<"$header" ||
   ! grep -Fq "$machine" <<<"$header"; then
  echo "Expected a Linux ELF64 shared library for $arch: $library" >&2
  exit 1
fi

symbols=$(readelf --dyn-syms --wide "$library")
for symbol in cliproxy_plugin_init cliproxyPluginCall cliproxyPluginFree cliproxyPluginShutdown; do
  if ! awk -v name="$symbol" '$4 == "FUNC" && $5 == "GLOBAL" && $7 != "UND" && $8 == name { found=1 } END { exit !found }' <<<"$symbols"; then
    echo "Missing exported C ABI function: $symbol" >&2
    exit 1
  fi
done

# Debian Bookworm's cgo builds currently require glibc 2.34. Fail if a future
# toolchain silently raises that floor beyond the documented release baseline.
versions=$(readelf --version-info "$library")
glibc=$(grep -oE 'GLIBC_[0-9]+(\.[0-9]+)+' <<<"$versions" | sort -Vu | tail -n 1 || true)
if [[ -z $glibc ]] || [[ $(printf '%s\n' GLIBC_2.34 "$glibc" | sort -V | tail -n 1) != GLIBC_2.34 ]]; then
  echo "Unsupported glibc requirement: ${glibc:-unknown} (maximum GLIBC_2.34)" >&2
  exit 1
fi
echo "Verified Linux/$arch C ABI exports; highest required symbol version: $glibc"

mkdir -p "$output"
output=$(cd "$output" && pwd)
stage=$(mktemp -d)
trap 'rm -rf -- "$stage"' EXIT
install -m 0755 "$library" "$stage/deepseek-search.so"
install -m 0644 README.md config.example.yaml LICENSE "$stage/"
archive="deepseek-search_${version}_linux_${arch}.zip"
rm -f -- "$output/$archive"
(cd "$stage" && zip -X -q "$output/$archive" deepseek-search.so README.md config.example.yaml LICENSE)
(cd "$output" && sha256sum "$archive" > checksums.txt)
echo "Created $output/$archive"
