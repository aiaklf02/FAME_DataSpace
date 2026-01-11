@echo off
REM ═══════════════════════════════════════════════════════════════════════════════
REM FAME Financial Data Space - Quick Start (Windows Batch)
REM ═══════════════════════════════════════════════════════════════════════════════

echo.
echo ╔═══════════════════════════════════════════════════════════════════════════════╗
echo ║                    FAME Financial Data Space                                  ║
echo ║          Data Lake + Data Fabric + Data Warehouse (EtLT)                      ║
echo ╚═══════════════════════════════════════════════════════════════════════════════╝
echo.

REM Check if PowerShell is available (preferred)
where powershell >nul 2>&1
if %ERRORLEVEL% EQU 0 (
    echo [INFO] Running with PowerShell...
    powershell -ExecutionPolicy Bypass -File "%~dp0start.ps1"
    goto :end
)

REM Fallback to basic Docker Compose commands
echo [INFO] PowerShell not available, using basic startup...
echo.

echo [1/4] Creating directories...
if not exist "data\bronze" mkdir data\bronze
if not exist "data\silver" mkdir data\silver
if not exist "data\gold" mkdir data\gold
if not exist "data\warehouse" mkdir data\warehouse
if not exist "logs" mkdir logs
echo [OK] Directories created

echo.
echo [2/4] Stopping existing containers...
docker compose down --remove-orphans 2>nul
echo [OK] Cleanup complete

echo.
echo [3/4] Starting Docker services...
docker compose up -d
echo [OK] Services started

echo.
echo [4/4] Waiting for services to initialize (30 seconds)...
timeout /t 30 /nobreak >nul
echo [OK] Services should be ready

echo.
echo ═══════════════════════════════════════════════════════════════════════════════
echo                         FAME DATA SPACE IS RUNNING!
echo ═══════════════════════════════════════════════════════════════════════════════
echo.
echo Access URLs:
echo   - Grafana:    http://localhost:3001  (admin / fame_grafana_2024)
echo   - Superset:   http://localhost:8088  (admin / fame_admin_2024)
echo   - Kafka UI:   http://localhost:8080
echo   - MinIO:      http://localhost:9001  (fame_admin / fame_secret_2024)
echo   - Prometheus: http://localhost:9090
echo   - Fuseki:     http://localhost:3030  (admin / admin123)
echo.

:end
pause
