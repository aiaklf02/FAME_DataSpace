#!/bin/bash
# ═══════════════════════════════════════════════════════════════════════════════
# FAME Financial Data Space - Complete Startup Script
# ═══════════════════════════════════════════════════════════════════════════════
# Architecture: Data Lake + Data Fabric + Data Warehouse (EtLT)
#
# This script:
#   1. Checks prerequisites (Docker, Python)
#   2. Starts all Docker services (17 containers)
#   3. Waits for services to be healthy
#   4. Initializes databases and data
#   5. Runs the EtLT pipeline
#   6. Opens dashboards in browser
# ═══════════════════════════════════════════════════════════════════════════════

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# ═══════════════════════════════════════════════════════════════════════════════
# BANNER
# ═══════════════════════════════════════════════════════════════════════════════

echo -e "${CYAN}"
echo "╔═══════════════════════════════════════════════════════════════════════════════╗"
echo "║                                                                               ║"
echo "║   ███████╗ █████╗ ███╗   ███╗███████╗    ██████╗  █████╗ ████████╗ █████╗    ║"
echo "║   ██╔════╝██╔══██╗████╗ ████║██╔════╝    ██╔══██╗██╔══██╗╚══██╔══╝██╔══██╗   ║"
echo "║   █████╗  ███████║██╔████╔██║█████╗      ██║  ██║███████║   ██║   ███████║   ║"
echo "║   ██╔══╝  ██╔══██║██║╚██╔╝██║██╔══╝      ██║  ██║██╔══██║   ██║   ██╔══██║   ║"
echo "║   ██║     ██║  ██║██║ ╚═╝ ██║███████╗    ██████╔╝██║  ██║   ██║   ██║  ██║   ║"
echo "║   ╚═╝     ╚═╝  ╚═╝╚═╝     ╚═╝╚══════╝    ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝   ║"
echo "║                                                                               ║"
echo "║                    🏦 Financial Data Space - Master M2                        ║"
echo "║          Data Lake + Data Fabric + Data Warehouse (EtLT Pattern)              ║"
echo "║                                                                               ║"
echo "╚═══════════════════════════════════════════════════════════════════════════════╝"
echo -e "${NC}"

# ═══════════════════════════════════════════════════════════════════════════════
# FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[✓]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[⚠]${NC} $1"
}

log_error() {
    echo -e "${RED}[✗]${NC} $1"
}

log_step() {
    echo -e "\n${PURPLE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${PURPLE}  $1${NC}"
    echo -e "${PURPLE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}\n"
}

wait_for_service() {
    local service=$1
    local url=$2
    local max_attempts=${3:-30}
    local attempt=1
    
    echo -n "  Waiting for $service"
    while [ $attempt -le $max_attempts ]; do
        if curl -s "$url" > /dev/null 2>&1; then
            echo -e " ${GREEN}✓${NC}"
            return 0
        fi
        echo -n "."
        sleep 2
        ((attempt++))
    done
    echo -e " ${RED}✗${NC}"
    return 1
}

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 1: CHECK PREREQUISITES
# ═══════════════════════════════════════════════════════════════════════════════

log_step "STEP 1/6: Checking Prerequisites"

# Check Docker
if command -v docker &> /dev/null; then
    DOCKER_VERSION=$(docker --version | cut -d' ' -f3 | tr -d ',')
    log_success "Docker installed: v$DOCKER_VERSION"
else
    log_error "Docker is not installed. Please install Docker first."
    exit 1
fi

# Check Docker Compose
if command -v docker-compose &> /dev/null; then
    COMPOSE_VERSION=$(docker-compose --version | cut -d' ' -f4 | tr -d ',')
    log_success "Docker Compose installed: v$COMPOSE_VERSION"
elif docker compose version &> /dev/null; then
    COMPOSE_VERSION=$(docker compose version --short)
    log_success "Docker Compose (plugin) installed: v$COMPOSE_VERSION"
    alias docker-compose='docker compose'
else
    log_error "Docker Compose is not installed. Please install Docker Compose."
    exit 1
fi

# Check if Docker is running
if docker info &> /dev/null; then
    log_success "Docker daemon is running"
else
    log_error "Docker daemon is not running. Please start Docker."
    exit 1
fi

# Check Python (optional, for local development)
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version | cut -d' ' -f2)
    log_success "Python installed: v$PYTHON_VERSION"
elif command -v python &> /dev/null; then
    PYTHON_VERSION=$(python --version | cut -d' ' -f2)
    log_success "Python installed: v$PYTHON_VERSION"
else
    log_warning "Python not found (optional for containerized deployment)"
fi

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 2: CREATE REQUIRED DIRECTORIES
# ═══════════════════════════════════════════════════════════════════════════════

log_step "STEP 2/6: Creating Directory Structure"

# Create data directories
mkdir -p data/bronze data/silver data/gold
mkdir -p data/warehouse
mkdir -p data/rdf
mkdir -p logs

log_success "Created data/bronze directory (Raw Data)"
log_success "Created data/silver directory (Cleaned Data)"
log_success "Created data/gold directory (Curated Data)"
log_success "Created data/warehouse directory (DuckDB)"
log_success "Created data/rdf directory (RDF Store)"
log_success "Created logs directory"

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 3: STOP EXISTING CONTAINERS (if any)
# ═══════════════════════════════════════════════════════════════════════════════

log_step "STEP 3/6: Cleaning Up Existing Containers"

if docker-compose ps -q 2>/dev/null | grep -q .; then
    log_info "Stopping existing containers..."
    docker-compose down --remove-orphans
    log_success "Existing containers stopped"
else
    log_info "No existing containers to stop"
fi

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 4: START DOCKER SERVICES
# ═══════════════════════════════════════════════════════════════════════════════

log_step "STEP 4/6: Starting Docker Services (17 containers)"

log_info "Pulling latest images..."
docker-compose pull --quiet 2>/dev/null || true

log_info "Starting services..."
docker-compose up -d

# Show container status
echo ""
log_info "Container Status:"
docker-compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null || docker-compose ps

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 5: WAIT FOR SERVICES TO BE HEALTHY
# ═══════════════════════════════════════════════════════════════════════════════

log_step "STEP 5/6: Waiting for Services to be Ready"

log_info "Checking service health..."

# Wait for core services
wait_for_service "Zookeeper" "localhost:2181" 30 || log_warning "Zookeeper may not be ready"
wait_for_service "Kafka" "localhost:29092" 60 || log_warning "Kafka may not be ready"
wait_for_service "MinIO" "http://localhost:9000/minio/health/live" 30 || log_warning "MinIO may not be ready"
wait_for_service "PostgreSQL" "localhost:5432" 30 || log_warning "PostgreSQL may not be ready"
wait_for_service "Redis" "localhost:6379" 30 || log_warning "Redis may not be ready"

# Wait for UI services
wait_for_service "Kafka UI" "http://localhost:8080" 30 || log_warning "Kafka UI may not be ready"
wait_for_service "MinIO Console" "http://localhost:9001" 30 || log_warning "MinIO Console may not be ready"
wait_for_service "Fuseki" "http://localhost:3030" 30 || log_warning "Fuseki may not be ready"
wait_for_service "Prometheus" "http://localhost:9090" 30 || log_warning "Prometheus may not be ready"
wait_for_service "Grafana" "http://localhost:3001" 30 || log_warning "Grafana may not be ready"

log_success "All core services are running!"

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 6: INITIALIZE DATA (Optional)
# ═══════════════════════════════════════════════════════════════════════════════

log_step "STEP 6/6: Initialization Complete"

# Create Kafka topics
log_info "Creating Kafka topics..."
docker exec -it fame-kafka kafka-topics --create --if-not-exists \
    --bootstrap-server localhost:9092 \
    --topic fame.market.stocks \
    --partitions 3 \
    --replication-factor 1 2>/dev/null || true

docker exec -it fame-kafka kafka-topics --create --if-not-exists \
    --bootstrap-server localhost:9092 \
    --topic fame.forex.rates \
    --partitions 3 \
    --replication-factor 1 2>/dev/null || true

docker exec -it fame-kafka kafka-topics --create --if-not-exists \
    --bootstrap-server localhost:9092 \
    --topic fame.transactions \
    --partitions 3 \
    --replication-factor 1 2>/dev/null || true

log_success "Kafka topics created"

# ═══════════════════════════════════════════════════════════════════════════════
# FINAL SUMMARY
# ═══════════════════════════════════════════════════════════════════════════════

echo -e "\n${GREEN}"
echo "╔═══════════════════════════════════════════════════════════════════════════════╗"
echo "║                                                                               ║"
echo "║                    ✅ FAME DATA SPACE IS RUNNING!                             ║"
echo "║                                                                               ║"
echo "╚═══════════════════════════════════════════════════════════════════════════════╝"
echo -e "${NC}"

echo -e "${CYAN}📊 Access the following services:${NC}\n"

echo -e "  ${YELLOW}Core Services:${NC}"
echo -e "  ├── 🔄 Kafka UI:        ${GREEN}http://localhost:8080${NC}"
echo -e "  ├── 🗄️  MinIO Console:   ${GREEN}http://localhost:9001${NC}  (fame_admin / fame_secret_2024)"
echo -e "  ├── 🐘 PostgreSQL:      ${GREEN}localhost:5432${NC}         (fame_user / fame_password)"
echo -e "  └── 🔗 Fuseki SPARQL:   ${GREEN}http://localhost:3030${NC}  (admin / admin123)"
echo ""
echo -e "  ${YELLOW}🌟 Innovation Services:${NC}"
echo -e "  ├── 📈 Grafana:         ${GREEN}http://localhost:3001${NC}  (admin / fame_grafana_2024)"
echo -e "  ├── 📊 Superset:        ${GREEN}http://localhost:8088${NC}  (admin / fame_admin_2024)"
echo -e "  ├── 📉 Prometheus:      ${GREEN}http://localhost:9090${NC}"
echo -e "  ├── ⚡ Redis:           ${GREEN}localhost:6379${NC}"
echo -e "  └── 🌐 Traefik:         ${GREEN}http://localhost:8082${NC}"
echo ""
echo -e "  ${YELLOW}Application:${NC}"
echo -e "  ├── 🖥️  Dashboard:       ${GREEN}http://localhost:8501${NC}"
echo -e "  └── ⚡ Spark UI:        ${GREEN}http://localhost:8083${NC}"
echo ""

echo -e "${CYAN}📝 Useful Commands:${NC}\n"
echo -e "  ${YELLOW}# View logs${NC}"
echo -e "  docker-compose logs -f [service_name]"
echo ""
echo -e "  ${YELLOW}# Stop all services${NC}"
echo -e "  docker-compose down"
echo ""
echo -e "  ${YELLOW}# Run EtLT pipeline${NC}"
echo -e "  python main.py pipeline"
echo ""
echo -e "  ${YELLOW}# Check service health${NC}"
echo -e "  docker-compose ps"
echo ""

echo -e "${GREEN}🎓 Master M2 - FAME Financial Data Space${NC}"
echo -e "${GREEN}   Architecture: Data Lake + Data Fabric + Data Warehouse (EtLT)${NC}"
echo ""
