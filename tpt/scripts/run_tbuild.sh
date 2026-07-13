#!/bin/bash
# Preprocess a tbuild script (substituting $(VAR) from the container environment),
# write to a temp file, and execute with tbuild. Ported from the proven pattern in
# the reference repo (Pibbers/teradata-streaming-demo tpt/scripts/run_tbuild.sh).
# Usage (inside the tpt container): bash run_tbuild.sh /tpt/tbuild/job.tbuild [tbuild-args...]
set -e

TBUILD_FILE="$1"; shift

TMPSCRIPT=$(mktemp /tmp/tbuild_XXXXXX.tbuild)
trap 'rm -f "$TMPSCRIPT"' EXIT

perl -pe 's/\$\(([^)]+)\)/defined $ENV{$1} ? $ENV{$1} : "(UNDEF:$1)"/ge' \
  "$TBUILD_FILE" > "$TMPSCRIPT"

exec tbuild -f "$TMPSCRIPT" "$@"
