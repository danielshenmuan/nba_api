#!/usr/bin/env bash
# shellcheck disable=SC1090
set -euo pipefail

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  echo "\nThis script must be sourced so that YAHOO_OAUTH2_JSON remains available in your shell." >&2
  echo "Usage: source jobs/set_yahoo_oauth_env.sh /path/to/yahoo_oauth2.json" >&2
  exit 1
fi

TOKEN_PATH="${1:-}"
if [[ -z "${TOKEN_PATH}" ]]; then
  echo "Usage: source jobs/set_yahoo_oauth_env.sh /path/to/yahoo_oauth2.json" >&2
  return 1
fi

if [[ ! -f "${TOKEN_PATH}" ]]; then
  echo "Token file not found at: ${TOKEN_PATH}" >&2
  return 1
fi

export YAHOO_OAUTH2_JSON="$(cat "${TOKEN_PATH}")"
export YAHOO_OAUTH2_JSON_PATH="${TOKEN_PATH}"

echo "Exported YAHOO_OAUTH2_JSON from ${TOKEN_PATH}."
