#!/usr/bin/env bash
# shellcheck disable=SC1090
set -euo pipefail

usage() {
  local cmd
  cmd="$(basename "${BASH_SOURCE[0]}")"
  cat <<USAGE
Usage (export in current shell):
  source jobs/${cmd} /path/to/yahoo_oauth2.json

Usage (persist to repo .env for future runs):
  jobs/${cmd} /path/to/yahoo_oauth2.json
USAGE
}

TOKEN_PATH="${1:-}"
if [[ -z "${TOKEN_PATH}" ]]; then
  usage >&2
  if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    exit 1
  else
    return 1
  fi
fi

if [[ ! -f "${TOKEN_PATH}" ]]; then
  echo "Token file not found at: ${TOKEN_PATH}" >&2
  if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    exit 1
  else
    return 1
  fi
fi

TOKEN_JSON=$(python3 - "$TOKEN_PATH" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
try:
    payload = json.loads(path.read_text())
except json.JSONDecodeError as exc:
    print(f"Invalid JSON in {path}: {exc}", file=sys.stderr)
    sys.exit(1)

# emit compact JSON on a single line so .env parsing stays simple
print(json.dumps(payload, separators=(",", ":")))
PY
)

if [[ -z "${TOKEN_JSON}" ]]; then
  echo "Failed to parse JSON from ${TOKEN_PATH}." >&2
  if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    exit 1
  else
    return 1
  fi
fi

if [[ "${BASH_SOURCE[0]}" != "${0}" ]]; then
  export YAHOO_OAUTH2_JSON="${TOKEN_JSON}"
  export YAHOO_OAUTH2_JSON_PATH="${TOKEN_PATH}"
  echo "Exported YAHOO_OAUTH2_JSON and YAHOO_OAUTH2_JSON_PATH from ${TOKEN_PATH}."
  return 0
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${REPO_ROOT}/.env"
TMP_FILE="$(mktemp)"

if [[ -f "${ENV_FILE}" ]]; then
  # shellcheck disable=SC2129
  grep -Ev '^(YAHOO_OAUTH2_JSON|YAHOO_OAUTH2_JSON_PATH)=' "${ENV_FILE}" >"${TMP_FILE}" || true
else
  : >"${TMP_FILE}"
fi

{
  printf "YAHOO_OAUTH2_JSON='%s'\n" "${TOKEN_JSON}"
  printf "YAHOO_OAUTH2_JSON_PATH=%s\n" "${TOKEN_PATH}"
} >>"${TMP_FILE}"

mv "${TMP_FILE}" "${ENV_FILE}"

echo "Updated ${ENV_FILE} with YAHOO_OAUTH2_JSON and YAHOO_OAUTH2_JSON_PATH."
echo "Future runs will load the token automatically from that file."
