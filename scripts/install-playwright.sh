#!/usr/bin/env bash
set -euo pipefail

dotnet build tests/TeamActivity.Scoreboard.Tests/TeamActivity.Scoreboard.Tests.csproj --nologo --tl:off -v:minimal

script="$(find tests/TeamActivity.Scoreboard.Tests/bin -name playwright.ps1 | head -1)"
if [[ -n "${script:-}" && -x "$(command -v pwsh || true)" ]]; then
  pwsh "$script" install chromium
  exit 0
fi

package_root="$HOME/.nuget/packages/microsoft.playwright"
version="$(ls "$package_root" | sort | tail -1)"
case "$(uname -s)-$(uname -m)" in
  Darwin-arm64) node="$package_root/$version/.playwright/node/darwin-arm64/node" ;;
  Darwin-x86_64) node="$package_root/$version/.playwright/node/darwin-x64/node" ;;
  Linux-aarch64|Linux-arm64) node="$package_root/$version/.playwright/node/linux-arm64/node" ;;
  Linux-x86_64) node="$package_root/$version/.playwright/node/linux-x64/node" ;;
  *) echo "Unsupported platform for Playwright install fallback. Install PowerShell and rerun." >&2; exit 1 ;;
esac

"$node" "$package_root/$version/.playwright/package/cli.js" install chromium
