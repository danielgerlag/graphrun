#!/usr/bin/env bash
set -euo pipefail

revision=f64d503a7217b1f314a23379da1f4c6bec70c267

if [[ $# -ne 4 ]]; then
	printf 'Usage: bash %s CHECKOUT SOURCE_PATH FIRST_LINE LAST_LINE\n' "$0" >&2
	exit 2
fi

checkout=$1
source_path=$2
first=$3
last=$4

if [[ ! "$first" =~ ^[1-9][0-9]*$ || ! "$last" =~ ^[1-9][0-9]*$ ]]; then
	printf 'Line numbers must be positive integers without leading zeros.\n' >&2
	exit 2
fi

if (( first > last )); then
	printf 'FIRST_LINE must not exceed LAST_LINE.\n' >&2
	exit 2
fi

git -C "$checkout" cat-file -e "$revision^{commit}"
printf '%s\n' "https://github.com/danielgerlag/workflow-core/blob/$revision/$source_path#L$first-L$last"
git -C "$checkout" --no-pager show "$revision:$source_path" |
	awk -v first="$first" -v last="$last" '
		NR >= first && NR <= last { printf "%5d %s\n", NR, $0 }
		END {
			if (NR < last) {
				print "Requested range extends past the end of the source file." > "/dev/stderr"
				exit 1
			}
		}
	'
