#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mode="${1:-share}"
if [[ "$mode" != "share" && "$mode" != "classic" ]]; then
  echo 'Usage: scripts/run.sh [share|classic]' >&2; exit 2
fi
export POC_MODE="$mode" SLOW_MS="${SLOW_MS:-20000}" FAST_MS="${FAST_MS:-100}" RENEW_LOCKS="${RENEW_LOCKS:-false}"
# WARNING: removes all containers/volumes of this PoC's Compose project.
docker compose down --volumes --remove-orphans
docker compose up -d --wait kafka
docker compose --profile "$mode" up -d --build "$mode-slow"
sleep 8
docker compose run --rm --build -e POC_START=0 -e POC_COUNT=1 publisher
sleep 2
docker compose --profile "$mode" up -d --build "$mode-fast"
sleep 5
docker compose run --rm -e POC_START=1 -e POC_COUNT="${REMAINING_MESSAGES:-19}" publisher
printf '\nWatch: docker compose logs -f %s-slow %s-fast\n' "$mode" "$mode"
printf 'Analyze after completion: docker compose logs --no-color %s-slow %s-fast > %s.log && python scripts/analyze.py %s.log\n' "$mode" "$mode" "$mode" "$mode"
