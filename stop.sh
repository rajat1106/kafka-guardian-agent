#!/usr/bin/env bash
# Stop Kafka Guardian. Pass --clean to also wipe incident history.
set -euo pipefail
cd "$(dirname "$0")"

if [ "${1:-}" = "--clean" ]; then
  echo "Stopping and removing all data (incident history, plugin settings)…"
  docker compose down -v
else
  echo "Stopping. Data is kept — use ./stop.sh --clean to wipe it."
  docker compose down
fi
echo "Done."
