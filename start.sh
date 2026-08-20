#!/usr/bin/env bash
# Start Kafka Guardian. One command, no prior knowledge assumed.
set -euo pipefail

cd "$(dirname "$0")"

bold=$'\033[1m'; dim=$'\033[2m'; green=$'\033[32m'; red=$'\033[31m'
yellow=$'\033[33m'; reset=$'\033[0m'

say()  { printf '%s\n' "$*"; }
ok()   { printf '  %s✓%s %s\n' "$green" "$reset" "$*"; }
warn() { printf '  %s!%s %s\n' "$yellow" "$reset" "$*"; }
die()  { printf '\n  %s✗ %s%s\n\n' "$red" "$*" "$reset"; exit 1; }

say ""
say "${bold}Kafka Guardian${reset}"
say "${dim}starting the full stack — this takes about a minute the first time${reset}"
say ""

# ── prerequisites ────────────────────────────────────────────────────
command -v docker >/dev/null 2>&1 || die "Docker is not installed. Get it from https://docker.com/get-started"

if ! docker info >/dev/null 2>&1; then
  warn "Docker is installed but not running — trying to start it"
  if [[ "$OSTYPE" == darwin* ]]; then
    open -a Docker || true
    for _ in $(seq 1 60); do
      docker info >/dev/null 2>&1 && break
      sleep 2
    done
  fi
  docker info >/dev/null 2>&1 || die "Could not start Docker. Open Docker Desktop, then run this again."
fi
ok "Docker is running"

if [ ! -f .env ]; then
  cp .env.example .env
  ok "Created .env from the template (no API key needed — the agent works without one)"
else
  ok "Using your existing .env"
fi

# ── build and start ──────────────────────────────────────────────────
say ""
say "${bold}Starting containers…${reset}"
docker compose up -d --build

# ── wait for the pieces that matter ──────────────────────────────────
say ""
say "${bold}Waiting for services…${reset}"

wait_for() {  # wait_for <label> <url> <seconds>
  local label=$1 url=$2 limit=${3:-90} waited=0
  while [ "$waited" -lt "$limit" ]; do
    if curl -fsS --max-time 3 "$url" >/dev/null 2>&1; then
      ok "$label"
      return 0
    fi
    sleep 3; waited=$((waited + 3))
  done
  warn "$label did not respond within ${limit}s — check: docker compose logs"
  return 1
}

wait_for "Kafka broker"  "http://localhost:8081/health" 150 || true
wait_for "Agent API"     "http://localhost:8080/api/health" 90 || true
wait_for "Kafka console" "http://localhost:8090" 90 || true
wait_for "Dashboard"     "http://localhost:5173" 90 || true

say ""
say "${bold}${green}Ready.${reset}"
say ""
say "  ${bold}Dashboard      →  http://localhost:5173${reset}"
say "  ${bold}Kafka console  →  http://localhost:8090${reset}  ${dim}(browse topics and messages)${reset}"
say ""
say "  ${dim}Overview     what the agent has done, in plain English"
say "  Topology     live map of producers, topics and consumers"
say "  Operations   raw telemetry and the full audit trail"
say "  Connections  plug in your own Kafka, LLM or Slack${reset}"
say ""
say "  ${dim}Within a couple of minutes the chaos engine breaks something"
say "  and you can watch the agent detect, diagnose and fix it.${reset}"
say ""
say "  ${dim}Stop it with:  ./stop.sh    (or: docker compose down)${reset}"
say ""

if [[ "$OSTYPE" == darwin* ]] && [ "${NO_OPEN:-}" != "1" ]; then
  open http://localhost:5173 2>/dev/null || true
fi
