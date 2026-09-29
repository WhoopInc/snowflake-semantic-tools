#!/usr/bin/env bash
# Read the tool call from stdin and refuse DROP, DELETE, or TRUNCATE.
set -euo pipefail
payload="$(cat)"
if printf '%s' "$payload" | grep -Eiq '\b(drop|delete|truncate)\b'; then
  echo "Refusing destructive SQL without confirmation." >&2
  exit 2
fi
