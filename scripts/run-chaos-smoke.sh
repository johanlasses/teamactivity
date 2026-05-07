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
aspire wait processor --apphost "$APPHOST" --status healthy --timeout 120 --non-interactive --nologo

sleep 5
echo "Restarting processor resource as deterministic chaos event..."
aspire resource processor restart --apphost "$APPHOST" --non-interactive --nologo
aspire wait processor --apphost "$APPHOST" --status healthy --timeout 120 --non-interactive --nologo

sleep 28

aspire describe --apphost "$APPHOST" --non-interactive --nologo
scores="$(curl -fsS http://localhost:5076/api/scores)"
echo "$scores"

if ! grep -Eq '"teamId":"team-template"' <<<"$scores"; then
  echo "Expected team-template score entry after chaos run." >&2
  exit 1
fi

if ! curl -fsS http://localhost:5216/api/scores >/dev/null; then
  echo "Scoreboard did not stay responsive after chaos run." >&2
  exit 1
fi
