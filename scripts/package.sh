#!/usr/bin/env bash
set -euo pipefail
export LC_ALL=C

root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
version=${1:-dev}
version=${version#v}
arch=${2:?usage: package.sh VERSION ARCH [LIBRARY] [OUTPUT_DIRECTORY] [OS]}
os=${5:-${GOOS:-linux}}
output=${4:-dist}

if [[ ! $version =~ ^[[:alnum:]][[:alnum:].+_-]*$ ]]; then
  echo "Invalid release version: $version" >&2
  exit 1
fi
case "$arch" in
  amd64|arm64) ;;
  *) echo "Unsupported release architecture: $arch" >&2; exit 1 ;;
esac
case "$os" in
  linux) library_name=deepseek-search.so ;;
  darwin) library_name=deepseek-search.dylib ;;
  windows) library_name=deepseek-search.dll ;;
  *) echo "Unsupported release OS: $os" >&2; exit 1 ;;
esac
library=${3:-build/$library_name}
exports=(cliproxy_plugin_init cliproxyPluginCall cliproxyPluginFree cliproxyPluginShutdown)

cd "$root"
case "$os" in
  linux)
    case "$arch" in
      amd64) machine='Advanced Micro Devices X86-64' ;;
      arm64) machine='AArch64' ;;
    esac
    header=$(readelf --file-header "$library")
    if ! grep -Eq 'Class: +ELF64' <<<"$header" ||
       ! grep -Eq 'Type: +DYN ' <<<"$header" ||
       ! grep -Fq "$machine" <<<"$header"; then
      echo "Expected a Linux ELF64 shared library for $arch: $library" >&2
      exit 1
    fi
    symbols=$(readelf --dyn-syms --wide "$library")
    for symbol in "${exports[@]}"; do
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
    ;;
  darwin)
    case "$arch" in
      amd64) machine=x86_64 ;;
      arm64) machine=arm64 ;;
    esac
    architectures=$(lipo -archs "$library")
    header=$(otool -hv "$library")
    if [[ $architectures != "$machine" ]] ||
       ! awk '$1 == "MH_MAGIC_64" && $5 == "DYLIB" { found=1 } END { exit !found }' <<<"$header"; then
      echo "Expected a macOS Mach-O dynamic library for $arch: $library" >&2
      exit 1
    fi
    symbols=$(nm -gU "$library")
    for symbol in "${exports[@]}"; do
      if ! awk -v name="_$symbol" '$NF == name && $(NF-1) == "T" { found=1 } END { exit !found }' <<<"$symbols"; then
        echo "Missing exported C ABI function: $symbol" >&2
        exit 1
      fi
    done
    echo "Verified macOS/$arch C ABI exports"
    ;;
  windows)
    case "$arch" in
      amd64) machine=IMAGE_FILE_MACHINE_AMD64 ;;
      arm64) machine=IMAGE_FILE_MACHINE_ARM64 ;;
    esac
    header=$(llvm-readobj --file-headers --coff-exports "$library")
    if ! awk -v machine="$machine" '$1 == "Machine:" && $2 == machine { found=1 } END { exit !found }' <<<"$header" ||
       ! grep -Eq '^[[:space:]]+IMAGE_FILE_DLL[[:space:]]' <<<"$header" ||
       ! grep -Eq '^[[:space:]]+Magic: 0x20B$' <<<"$header" ||
       ! grep -Eq '^Format: COFF-' <<<"$header"; then
      echo "Expected a Windows PE DLL for $arch: $library" >&2
      exit 1
    fi
    for symbol in "${exports[@]}"; do
      if ! awk -v name="$symbol" '$1 == "Name:" && $2 == name { found=1 } END { exit !found }' <<<"$header"; then
        echo "Missing exported C ABI function: $symbol" >&2
        exit 1
      fi
    done
    echo "Verified Windows/$arch C ABI exports"
    ;;
esac

mkdir -p "$output"
output=$(cd "$output" && pwd)
stage=$(mktemp -d)
trap 'rm -rf -- "$stage"' EXIT
install -m 0755 "$library" "$stage/$library_name"
install -m 0644 README.md config.example.yaml LICENSE "$stage/"
archive="deepseek-search_${version}_${os}_${arch}.zip"
rm -f -- "$output/$archive"
(cd "$stage" && zip -X -q "$output/$archive" "$library_name" README.md config.example.yaml LICENSE)
if command -v sha256sum >/dev/null 2>&1; then
  (cd "$output" && sha256sum "$archive" > checksums.txt)
else
  (cd "$output" && shasum -a 256 "$archive" > checksums.txt)
fi
echo "Created $output/$archive"
