#!/usr/bin/env bash
set -euo pipefail

# Publish graphrun then graphrun-cli to crates.io. Does not publish graphrun-e2e.
# Usage: scripts/publish.sh [--dry-run]

dry=0
if [[ "${1:-}" == "--dry-run" ]]; then
  dry=1
elif [[ -n "${1:-}" ]]; then
  echo "usage: $0 [--dry-run]" >&2
  exit 2
fi

root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"

version=$(python3 - <<'PY'
import json, subprocess, sys
d = json.loads(
    subprocess.check_output(
        [
            "cargo",
            "metadata",
            "--format-version",
            "1",
            "--no-deps",
            "--offline",
            "--locked",
        ]
    )
)
pkgs = {p["name"]: p for p in d["packages"]}
if pkgs["graphrun-e2e"].get("publish") != []:
    sys.exit("graphrun-e2e must not be publishable")
print(pkgs["graphrun"]["version"])
PY
)

if [[ "${GITHUB_REF_TYPE:-}" == "tag" ]]; then
  tag=${GITHUB_REF_NAME#v}
  if [[ "$tag" != "$version" ]]; then
    echo "tag ${GITHUB_REF_NAME} does not match workspace version ${version}" >&2
    exit 1
  fi
fi

already() {
  curl -fsS -A "graphrun-ci (https://github.com/danielgerlag/graphrun)" \
    "https://crates.io/api/v1/crates/$1/$2" >/dev/null
}

if [[ "$dry" -eq 1 ]]; then
  cargo publish -p graphrun --dry-run --locked
  cargo package -p graphrun-cli --list
  if already graphrun "$version"; then
    cargo publish -p graphrun-cli --dry-run --locked --no-verify
  else
    echo "skipping graphrun-cli publish dry-run until graphrun ${version} is on crates.io"
  fi
  echo "dry-run ok ${version}"
  exit 0
fi

if already graphrun "$version"; then
  echo "graphrun ${version} already on crates.io"
else
  cargo publish -p graphrun --locked
fi

if already graphrun-cli "$version"; then
  echo "graphrun-cli ${version} already on crates.io"
  exit 0
fi

n=0
until cargo publish -p graphrun-cli --locked; do
  n=$((n + 1))
  if [[ "$n" -ge 24 ]]; then
    echo "graphrun-cli ${version} publish failed after ${n} attempts" >&2
    exit 1
  fi
  echo "waiting for crates.io to index graphrun ${version} (attempt ${n})"
  sleep 15
done
