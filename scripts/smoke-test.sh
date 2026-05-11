#!/usr/bin/env bash
set -euo pipefail

APPHOST="TeamActivity.AppHost/TeamActivity.AppHost.csproj"

cleanup() {
  aspire stop --apphost "$APPHOST" --non-interactive --nologo >/dev/null 2>&1 || true
}
trap cleanup EXIT

dotnet build TeamActivity.sln --nologo --tl:off -v:minimal
cleanup

aspire start --apphost "$APPHOST" --non-interactive --nologo --format Json
sleep 5

aspire describe --apphost "$APPHOST" --non-interactive --nologo

# Trigger a short run (10 seconds) via the Judge API
curl -fsS -X POST http://localhost:5076/api/run/start \
  -H 'Content-Type: application/json' \
  -d '{"deviceCount":3,"intervalMs":250,"runWindowSeconds":10,"chaosEnabled":false}'
echo ""

sleep 18

scores="$(curl -fsS http://localhost:5076/api/scores)"
echo "$scores"

if ! grep -Eq '"correct":[1-9]' <<<"$scores"; then
  echo "Expected at least one correct scored aggregate." >&2
  exit 1
fi

if ! grep -Eq '"teamId":"team-template"' <<<"$scores"; then
  echo "Expected team-template score entry." >&2
  exit 1
fi

bash scripts/install-playwright.sh
RUN_SCOREBOARD_UI_TESTS="true" SCOREBOARD_BASE_URL="http://localhost:5216" dotnet test tests/TeamActivity.Scoreboard.Tests/TeamActivity.Scoreboard.Tests.csproj \
  --no-build \
  --nologo \
  --logger "console;verbosity=minimal"
