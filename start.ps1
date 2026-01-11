# FAME Financial Data Space - Complete Startup Script (PowerShell)
# Architecture: Data Lake + Data Fabric + Data Warehouse (EtLT)

param(
    [switch]$SkipPull,
    [switch]$RunPipeline,
    [switch]$OpenBrowser
)

$ErrorActionPreference = "Continue"

# Functions
function Log-Info { Write-Host "[INFO] " -ForegroundColor Cyan -NoNewline; Write-Host $args }
function Log-Success { Write-Host "[OK] " -ForegroundColor Green -NoNewline; Write-Host $args }
function Log-Warning { Write-Host "[WARN] " -ForegroundColor Yellow -NoNewline; Write-Host $args }
function Log-Error { Write-Host "[ERROR] " -ForegroundColor Red -NoNewline; Write-Host $args }

function Log-Step {
    param([string]$Message)
    Write-Host ""
    Write-Host ("=" * 80) -ForegroundColor Magenta
    Write-Host "  $Message" -ForegroundColor Magenta
    Write-Host ("=" * 80) -ForegroundColor Magenta
    Write-Host ""
}

function Wait-ForService {
    param(
        [string]$ServiceName,
        [string]$Url,
        [int]$MaxAttempts = 30
    )
    
    Write-Host "  Waiting for $ServiceName" -NoNewline
    
    for ($i = 1; $i -le $MaxAttempts; $i++) {
        try {
            if ($Url -match "^http") {
                $response = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 2 -ErrorAction SilentlyContinue
                if ($response.StatusCode -eq 200) {
                    Write-Host " OK" -ForegroundColor Green
                    return $true
                }
            }
        } catch { }
        Write-Host "." -NoNewline
        Start-Sleep -Seconds 2
    }
    
    Write-Host " TIMEOUT" -ForegroundColor Yellow
    return $false
}

# Banner
Clear-Host
Write-Host ""
Write-Host "=========================================================================" -ForegroundColor Cyan
Write-Host "                    FAME Financial Data Space                            " -ForegroundColor Cyan
Write-Host "          Data Lake + Data Fabric + Data Warehouse (EtLT)                " -ForegroundColor Cyan
Write-Host "                        Master M2 Project                                " -ForegroundColor Cyan
Write-Host "=========================================================================" -ForegroundColor Cyan
Write-Host ""

# STEP 1: Check Prerequisites
Log-Step "STEP 1/6: Checking Prerequisites"

try {
    $dockerVersion = docker --version
    Log-Success "Docker installed: $dockerVersion"
} catch {
    Log-Error "Docker is not installed. Please install Docker Desktop."
    exit 1
}

try {
    docker info 2>$null | Out-Null
    Log-Success "Docker daemon is running"
} catch {
    Log-Error "Docker daemon is not running. Please start Docker Desktop."
    exit 1
}

# STEP 2: Create Directories
Log-Step "STEP 2/6: Creating Directory Structure"

$directories = @("data\bronze", "data\silver", "data\gold", "data\warehouse", "data\rdf", "logs")

foreach ($dir in $directories) {
    if (-not (Test-Path $dir)) {
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
    }
    Log-Success "Directory: $dir"
}

# STEP 3: Stop Existing Containers
Log-Step "STEP 3/6: Cleaning Up Existing Containers"

Log-Info "Stopping existing containers..."
docker compose down --remove-orphans 2>$null
Log-Success "Cleanup complete"

# STEP 4: Start Docker Services
Log-Step "STEP 4/6: Starting Docker Services"

if (-not $SkipPull) {
    Log-Info "Pulling latest images..."
    docker compose pull 2>$null
}

Log-Info "Starting services..."
docker compose up -d

Write-Host ""
Log-Info "Container Status:"
docker compose ps

# STEP 5: Wait for Services
Log-Step "STEP 5/6: Waiting for Services to be Ready"

Log-Info "Checking service health (this may take 1-2 minutes)..."

Wait-ForService "MinIO" "http://localhost:9000/minio/health/live" 30
Wait-ForService "Kafka UI" "http://localhost:8080" 60
Wait-ForService "MinIO Console" "http://localhost:9001" 30
Wait-ForService "Prometheus" "http://localhost:9090" 30
Wait-ForService "Grafana" "http://localhost:3001" 30

Log-Success "Core services are running!"

# STEP 6: Create Kafka Topics
Log-Step "STEP 6/6: Creating Kafka Topics"

$topics = @("fame.market.stocks", "fame.forex.rates", "fame.transactions")

foreach ($topic in $topics) {
    docker exec fame-kafka kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --topic $topic --partitions 3 --replication-factor 1 2>$null
    Log-Success "Topic: $topic"
}

# Open Browser
if ($OpenBrowser) {
    Start-Process "http://localhost:3001"
    Start-Process "http://localhost:8080"
}

# Final Summary
Write-Host ""
Write-Host "=========================================================================" -ForegroundColor Green
Write-Host "                    FAME DATA SPACE IS RUNNING!                          " -ForegroundColor Green
Write-Host "=========================================================================" -ForegroundColor Green
Write-Host ""

Write-Host "Access URLs:" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Core Services:" -ForegroundColor Yellow
Write-Host "    Kafka UI:        http://localhost:8080"
Write-Host "    MinIO Console:   http://localhost:9001  (fame_admin / fame_secret_2024)"
Write-Host "    PostgreSQL:      localhost:5432         (fame_user / fame_password)"
Write-Host "    Fuseki SPARQL:   http://localhost:3030  (admin / admin123)"
Write-Host ""
Write-Host "  Innovation Services:" -ForegroundColor Yellow
Write-Host "    Grafana:         http://localhost:3001  (admin / fame_grafana_2024)"
Write-Host "    Superset:        http://localhost:8088  (admin / fame_admin_2024)"
Write-Host "    Prometheus:      http://localhost:9090"
Write-Host "    Traefik:         http://localhost:8082"
Write-Host ""
Write-Host "  Application:" -ForegroundColor Yellow
Write-Host "    Dashboard:       http://localhost:8501"
Write-Host "    Spark UI:        http://localhost:8083"
Write-Host ""

Write-Host "Commands:" -ForegroundColor Cyan
Write-Host "  Stop:      docker compose down"
Write-Host "  Logs:      docker compose logs -f [service]"
Write-Host "  Pipeline:  python main.py pipeline"
Write-Host ""
