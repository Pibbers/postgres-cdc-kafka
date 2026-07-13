#!/bin/bash
# Preprocess a BTEQ script (substituting ${VAR} from the container environment)
# and pipe it into bteq. Ported from the proven pattern in the reference repo
# (Pibbers/teradata-streaming-demo tpt/scripts/run_bteq.sh).
# Usage (inside the tpt container): bash run_bteq.sh /scripts/some_script.bteq
set -e

BTEQ_FILE="$1"

perl -pe 's/\$\{([^}]+)\}/defined $ENV{$1} ? $ENV{$1} : "(UNDEF:$1)"/ge' "$BTEQ_FILE" | bteq
exit "${PIPESTATUS[1]}"
