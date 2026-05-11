#!/usr/bin/env bash
set -euo pipefail

APPHOST="TeamActivity.AppHost/TeamActivity.AppHost.csproj"
JUDGE_URL="http://localhost:5076"
ORGANIZER_KEY="${ORGANIZER_KEY:-}"

chaos_post() {
  local path="$1"
  local body="${2:-}"
  if [ -n "$body" ]; then
    curl -fsS -X POST "${JUDGE_URL}${path}" \
      -H "Content-Type: application/json" \
      ${ORGANIZER_KEY:+-H "X-Organizer-Key: ${ORGANIZER_KEY}"} \
      -d "$body" >/dev/null
  else
    curl -fsS -X POST "${JUDGE_URL}${path}" \
      ${ORGANIZER_KEY:+-H "X-Organizer-Key: ${ORGANIZER_KEY}"} \
      >/dev/null
  fi
}

cleanup() {
  chaos_post "/api/chaos/event/end" 2>/dev/null || true
  chaos_post "/api/chaos/disable" 2>/dev/null || true
  aspire stop --apphost "$APPHOST" --non-interactive --nologo >/dev/null 2>&1 || true
}
trap cleanup EXIT

dotnet build TeamActivity.sln --nologo --tl:off -v:minimal
cleanup

aspire start --apphost "$APPHOST" --non-interactive --nologo --format Json
aspire wait processor --apphost "$APPHOST" --status healthy --timeout 120 --non-interactive --nologo

echo "Triggering chaos run..."
curl -fsS -X POST "${JUDGE_URL}/api/run/start" \
  -H "Content-Type: application/json" \
  -d '{"deviceCount":3,"intervalMs":250,"runWindowSeconds":10,"chaosEnabled":true}'
echo ""

sleep 5
echo "Injecting chaos event: processor-restart..."
chaos_post "/api/chaos/event/start" '{"type":"processor-restart","description":"Deterministic smoke-test restart"}'

aspire resource processor restart --apphost "$APPHOST" --non-interactive --nologo
aspire wait processor --apphost "$APPHOST" --status healthy --timeout 120 --non-interactive --nologo

echo "Chaos event complete — clearing signal..."
chaos_post "/api/chaos/event/end"

sleep 18

aspire describe --apphost "$APPHOST" --non-interactive --nologo
scores="$(curl -fsS ${JUDGE_URL}/api/scores)"
echo "$scores"

if ! grep -Eq '"teamId":"team-template"' <<<"$scores"; then
  echo "Expected team-template score entry after chaos run." >&2
  exit 1
fi

if ! curl -fsS http://localhost:5216/api/scores >/dev/null; then
  echo "Scoreboard did not stay responsive after chaos run." >&2
  exit 1
fi

chaos_post "/api/chaos/disable"
echo "Chaos mode disabled. Smoke test passed."
