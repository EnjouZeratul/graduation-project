param(
  # Keep ASCII-only defaults for Windows PowerShell 5.1 compatibility.
  # When empty or missing, we auto-detect from the repo.
  [string]$InputPath = "",
  [string]$OutputPath = "backend\\app\\data\\tianqi_slug_overrides.json"
)

$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")

function Find-TianqiInputFile([string]$root) {
  # Robust auto-detect:
  # - Search the repo for *.htm/*.html whose path contains "tianqi"
  # - Pick the largest file (the national city index snapshot is typically large)
  $files =
    Get-ChildItem -Path $root -Recurse -File -Include *.htm,*.html -ErrorAction SilentlyContinue |
    Where-Object { $_.FullName -match "(?i)tianqi" }
  if (-not $files) { return $null }
  return ($files | Sort-Object Length -Descending | Select-Object -First 1)
}

if (-not $InputPath -or -not (Test-Path $InputPath)) {
  $auto = Find-TianqiInputFile -root $repoRoot
  if ($auto) {
    $InputPath = $auto.FullName
    Write-Host ("Auto-detected input: {0}" -f $InputPath)
  } else {
    throw "Input not found. Pass -InputPath explicitly, or place the snapshot under a folder named like 'tianqi.com*'."
  }
}

$outDir = Split-Path -Parent $OutputPath
if ($outDir -and -not (Test-Path $outDir)) {
  New-Item -ItemType Directory -Path $outDir | Out-Null
}

# Keep this blocklist small; backend also filters invalid slugs.
$block = @(
  "province","chinacity","worldcity","news","air","video","plugin","alarmnews","jingdian","tag","toutiao","latest","zhuanti","changshi"
) | ForEach-Object { $_.ToLowerInvariant() }

$text = Get-Content -Raw -Encoding utf8 $InputPath

# The saved snapshot in this repo may contain URLs broken by whitespace/newlines inside angle brackets, e.g.:
#   <https://www.tianqi.com/ \n wuyuan1/>
# Normalize by removing whitespace inside "<...>" URL blocks.
$text = [regex]::Replace(
  $text,
  "<(https?://[^>]+)>",
  { param($m) "<" + (($m.Groups[1].Value) -replace "\s+", "") + ">" },
  [System.Text.RegularExpressions.RegexOptions]::IgnoreCase
)

# The saved page in this repo is a text-like dump that contains patterns like:
#   吉林 <https://www.tianqi.com/jilinshi/>
# We intentionally skip "/province/..." links to avoid collisions (e.g. 吉林 province vs 吉林 city).
$re = [regex]::new(
  "([\u4e00-\u9fff]{2,32})\s*<https?://(?:www\.)?tianqi\.com/([^/\s>]{2,64})/",
  [System.Text.RegularExpressions.RegexOptions]::IgnoreCase
)
$matches = $re.Matches($text)

$map = @{}
$skippedProvince = 0
$skippedInvalid = 0

foreach ($m in $matches) {
  $full = [string]$m.Value
  if ($full -match "(?i)/province/") {
    $skippedProvince++
    continue
  }

  # Windows PowerShell 5.1 compatible (no null-coalescing operator '??').
  $label = [string]$m.Groups[1].Value
  $label = $label.Trim()
  $slug = [string]$m.Groups[2].Value
  $slug = $slug.Trim().ToLowerInvariant()

  if (-not $label -or -not $slug) { $skippedInvalid++; continue }
  if ($slug -in $block) { $skippedInvalid++; continue }
  if ($slug -notmatch "^[a-z0-9_-]{2,64}$") { $skippedInvalid++; continue }
  if ($slug -notmatch "[a-z]") { $skippedInvalid++; continue } # avoid pure digits (e.g. admin codes)

  # Later matches overwrite earlier ones.
  $map[$label] = $slug
}

($map | ConvertTo-Json -Depth 4) | Out-File -Encoding utf8 $OutputPath

Write-Host ("Wrote {0} overrides to {1}" -f $map.Count, $OutputPath)
Write-Host ("Skipped province links: {0}; skipped invalid: {1}" -f $skippedProvince, $skippedInvalid)
