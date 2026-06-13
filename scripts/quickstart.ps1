# Windows quickstart: start a local Redis (Docker) and run the all-in-one process.
# Usage:  powershell -ExecutionPolicy Bypass -File scripts\quickstart.ps1

$ErrorActionPreference = "Stop"

Write-Host "[dtq] starting Redis container 'dtq-redis' on :6379 ..."
$existing = docker ps -a --filter "name=dtq-redis" --format "{{.Names}}"
if ($existing -eq "dtq-redis") {
    docker start dtq-redis | Out-Null
} else {
    docker run -d --name dtq-redis -p 6379:6379 redis:7-alpine | Out-Null
}

$env:DTQ_REDIS_URL = "redis://localhost:6379/0"
$env:DTQ_IMPORT = "dtq.tasks"

Write-Host "[dtq] launching api + worker + scheduler (Ctrl+C to stop) ..."
Write-Host "[dtq] docs: http://localhost:8000/docs"
python -m dtq all
