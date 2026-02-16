$ErrorActionPreference = "Stop"

# Ensure UTF-8 output so Chinese doesn't become mojibake in JSON exports.
try {
  chcp 65001 | Out-Null
  [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new()
  $OutputEncoding = [System.Text.UTF8Encoding]::new()
} catch { }

$base = "http://localhost"
$debugUrl = "$base/api/warnings/debug/last-collection"

$latin1 = $null
try { $latin1 = [System.Text.Encoding]::GetEncoding("ISO-8859-1") } catch { }

function Fix-Mojibake([string]$s) {
  if (-not $s) { return $s }
  if ($s -match "[\u4e00-\u9fff]") { return $s }
  if (-not $latin1) { return $s }
  try {
    $bytes = $latin1.GetBytes($s)
    $fixed = [System.Text.Encoding]::UTF8.GetString($bytes)
    if ($fixed -match "[\u4e00-\u9fff]") { return $fixed }
  } catch { }
  return $s
}

$debug = Invoke-RestMethod $debugUrl
if (-not $debug -or -not $debug.results) {
  Write-Host "No debug results found at $debugUrl"
  exit 1
}

$rows = foreach ($r in $debug.results) {
  $e = $null
  try { $e = $r.meteorology.source_status.errors.weather_scraper } catch { $e = $null }
  if (-not $e -or -not $e.error) { continue }

  [pscustomobject]@{
    region_name = Fix-Mojibake([string]$r.region_name)
    region_code = $r.region_code
    weather_scraper_error = [pscustomobject]@{
      error           = $e.error
      message         = $e.message
      status_code     = $e.status_code
      url             = $e.url
      slug_candidates = $e.slug_candidates
    }
  }
}

$outPath = Join-Path (Get-Location) "failed-weather-scraper.json"
$rows | ConvertTo-Json -Depth 10 | Set-Content -Path $outPath -Encoding utf8

Write-Host ("results_count=" + $debug.results.Count)
Write-Host ("error_rows=" + $rows.Count)
Write-Host ("exported=" + $outPath)

$rows |
  Group-Object { $_.weather_scraper_error.error } |
  Sort-Object Count -Descending |
  Format-Table -Auto
