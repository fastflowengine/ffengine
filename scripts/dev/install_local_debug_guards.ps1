param(
    [switch]$Force
)

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$hooksDir = Join-Path $repoRoot ".git\hooks"
if (-not (Test-Path $hooksDir)) {
    throw "Git hooks dizini bulunamadi: $hooksDir"
}

$pattern = "debugpy|ENABLE_DEBUG|DEBUGPY_|5678|5679|DEBUG_BREAKPOINT_MARKER"

$preCommitPath = Join-Path $hooksDir "pre-commit"
$prePushPath = Join-Path $hooksDir "pre-push"

$preCommitContent = @'
#!/usr/bin/env sh
set -eu

PATTERN='__PATTERN__'
EXCLUDED=':(exclude)docker/docker-compose.local.debug.yml'

if git diff --cached -U0 -- . "$EXCLUDED" | grep -E "^\+.*($PATTERN)" >/dev/null 2>&1; then
  echo "ERROR: Debug kalintisi staged diff icinde bulundu."
  echo "Temizleyin veya local debug override dosyasina tasiyin: docker/docker-compose.local.debug.yml"
  exit 1
fi
'@

$prePushContent = @'
#!/usr/bin/env sh
set -eu

PATTERN='__PATTERN__'
EXCLUDED=':(exclude)docker/docker-compose.local.debug.yml'
ZERO='0000000000000000000000000000000000000000'

while read local_ref local_sha remote_ref remote_sha
do
  [ -z "$local_sha" ] && continue
  RANGE="$local_sha"
  if [ "$remote_sha" != "$ZERO" ]; then
    RANGE="$remote_sha..$local_sha"
  fi
  if git diff -U0 "$RANGE" -- . "$EXCLUDED" | grep -E "^\+.*($PATTERN)" >/dev/null 2>&1; then
    echo "ERROR: Push edilen commit diff icinde debug kalintisi bulundu: $RANGE"
    echo "Temizleyin veya local debug override dosyasina tasiyin: docker/docker-compose.local.debug.yml"
    exit 1
  fi
done
'@

$preCommitContent = $preCommitContent.Replace("__PATTERN__", $pattern)
$prePushContent = $prePushContent.Replace("__PATTERN__", $pattern)

if ((Test-Path $preCommitPath) -and -not $Force) {
    throw "pre-commit hook zaten var. Uzerine yazmak icin -Force kullanin."
}
if ((Test-Path $prePushPath) -and -not $Force) {
    throw "pre-push hook zaten var. Uzerine yazmak icin -Force kullanin."
}

Set-Content -Path $preCommitPath -Value $preCommitContent -Encoding UTF8
Set-Content -Path $prePushPath -Value $prePushContent -Encoding UTF8

Write-Host "Local debug guard hook'lari yazildi:"
Write-Host " - $preCommitPath"
Write-Host " - $prePushPath"
Write-Host "Not: Windows'ta dosya executable biti zorunlu degil; Git Bash ortaminda kullanilir."
