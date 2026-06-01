#!/usr/bin/env bash
# Reproduces and verifies the stop→restart scoreboard bug.
#
# Flow:
#   1. Start a 25 s run.
#   2. Stop it after 5 s (well before the window ends).
#   3. Assert publisher-complete appears for the stopped run within 15 s.
#   4. Immediately start a new 10 s run.
#   5. Assert the new run appears in /api/scores within 20 s.
#   6. Wait for the new run to finish and assert it has at least one correct score.

set -euo pipefail

APPHOST="TeamActivity.AppHost/TeamActivity.AppHost.csproj"
JUDGE_URL="http://localhost:5076"
SCOREBOARD_URL="http://localhost:5216"

cleanup() {
  aspire stop --apphost "$APPHOST" --non-interactive --nologo >/dev/null 2>&1 || true
}
trap cleanup EXIT

dotnet build TeamActivity.sln --nologo --tl:off -v:minimal
cleanup

aspire start --apphost "$APPHOST" --non-interactive --nologo --format Json
sleep 5

aspire describe --apphost "$APPHOST" --non-interactive --nologo

echo ""
echo "=== Step 1: Start a 25 s run ==="
first_run_response="$(curl -fsS -X POST "$JUDGE_URL/api/run/start" \
  -H 'Content-Type: application/json' \
  -d '{"deviceCount":2,"intervalMs":500,"runWindowSeconds":25,"chaosEnabled":false}')"
echo "$first_run_response"
first_run_id="$(echo "$first_run_response" | grep -oE '"runId":"[^"]+"' | head -1 | cut -d'"' -f4)"
echo "First run ID: $first_run_id"

echo ""
echo "=== Step 2: Wait 5 s, then stop the run ==="
sleep 5
stop_response="$(curl -fsS -o /dev/null -w "%{http_code}" -X POST "$JUDGE_URL/api/run/stop")"
if [ "$stop_response" != "200" ]; then
  echo "Stop returned HTTP $stop_response — expected 200" >&2
  exit 1
fi
echo "Run stopped (HTTP 200). Checking for publisher-complete within 15 s..."

echo ""
echo "=== Step 3: Assert publisher-complete arrives for the stopped run ==="
publisher_complete_seen=false
for i in $(seq 1 15); do
  sleep 1
  messages="$(curl -fsS "$JUDGE_URL/api/runs/$first_run_id/messages" 2>/dev/null || echo '[]')"
  if echo "$messages" | grep -q '"publisher-complete"'; then
    publisher_complete_seen=true
    echo "publisher-complete seen after ${i}s ✓"
    break
  fi
  echo "  ... waiting for publisher-complete ($i s)"
done

if [ "$publisher_complete_seen" = "false" ]; then
  echo "FAIL: publisher-complete did not appear within 15 s for run $first_run_id" >&2
  exit 1
fi

echo ""
echo "=== Step 4: Immediately start a new 10 s run ==="
second_run_response="$(curl -fsS -X POST "$JUDGE_URL/api/run/start" \
  -H 'Content-Type: application/json' \
  -d '{"deviceCount":2,"intervalMs":500,"runWindowSeconds":10,"chaosEnabled":false}')"
echo "$second_run_response"
second_run_id="$(echo "$second_run_response" | grep -oE '"runId":"[^"]+"' | head -1 | cut -d'"' -f4)"
echo "Second run ID: $second_run_id"

echo ""
echo "=== Step 5: Assert new run appears in /api/scores within 20 s ==="
new_run_seen=false
for i in $(seq 1 20); do
  sleep 1
  scores="$(curl -fsS "$JUDGE_URL/api/scores" 2>/dev/null || echo '[]')"
  if echo "$scores" | grep -q "\"$second_run_id\""; then
    new_run_seen=true
    echo "New run $second_run_id appears in scores after ${i}s ✓"
    break
  fi
  echo "  ... waiting for new run in scores ($i s)"
done

if [ "$new_run_seen" = "false" ]; then
  echo "FAIL: New run $second_run_id did not appear in /api/scores within 20 s" >&2
  echo "Current scores:" >&2
  curl -fsS "$JUDGE_URL/api/scores" >&2 || true
  exit 1
fi

echo ""
echo "=== Step 6: Wait for new run to finish and check for correct scores ==="
sleep 15

final_scores="$(curl -fsS "$JUDGE_URL/api/scores")"
echo "$final_scores"

if ! echo "$final_scores" | grep -q "\"$second_run_id\""; then
  echo "FAIL: Second run $second_run_id not found in final scores" >&2
  exit 1
fi

if ! echo "$final_scores" | grep -Eq '"correct":[1-9]'; then
  echo "FAIL: Expected at least one correct score for the new run" >&2
  exit 1
fi

echo ""
echo "=== Step 7: Run Playwright UI assertion ==="
bash scripts/install-playwright.sh
RUN_SCOREBOARD_UI_TESTS="true" SCOREBOARD_BASE_URL="$SCOREBOARD_URL" dotnet test \
  tests/TeamActivity.Scoreboard.Tests/TeamActivity.Scoreboard.Tests.csproj \
  --no-build \
  --nologo \
  --logger "console;verbosity=minimal"

echo ""
echo "✅ Stop→restart test passed!"
echo "   Stopped run:  $first_run_id"
echo "   New run:      $second_run_id"
