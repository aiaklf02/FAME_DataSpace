#!/bin/bash
# ═══════════════════════════════════════════════════════════════════════════════
# FAME Financial Data Space - Stop Script
# ═══════════════════════════════════════════════════════════════════════════════

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

echo -e "${CYAN}"
echo "╔═══════════════════════════════════════════════════════════════════════════════╗"
echo "║                    FAME Financial Data Space - Shutdown                       ║"
echo "╚═══════════════════════════════════════════════════════════════════════════════╝"
echo -e "${NC}"

# Check for flags
REMOVE_VOLUMES=false
REMOVE_DATA=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --volumes|-v)
            REMOVE_VOLUMES=true
            shift
            ;;
        --clean|-c)
            REMOVE_DATA=true
            REMOVE_VOLUMES=true
            shift
            ;;
        *)
            shift
            ;;
    esac
done

echo -e "${YELLOW}[INFO]${NC} Stopping all FAME services..."

if [ "$REMOVE_VOLUMES" = true ]; then
    echo -e "${YELLOW}[WARN]${NC} Removing Docker volumes (--volumes flag set)"
    docker-compose down --remove-orphans --volumes
else
    docker-compose down --remove-orphans
fi

if [ "$REMOVE_DATA" = true ]; then
    echo -e "${YELLOW}[WARN]${NC} Removing local data directories (--clean flag set)"
    rm -rf data/bronze data/silver data/gold data/warehouse data/rdf logs
    echo -e "${GREEN}[✓]${NC} Data directories removed"
fi

echo -e "${GREEN}"
echo "╔═══════════════════════════════════════════════════════════════════════════════╗"
echo "║                    ✅ FAME DATA SPACE STOPPED                                 ║"
echo "╚═══════════════════════════════════════════════════════════════════════════════╝"
echo -e "${NC}"

echo -e "${CYAN}Options used:${NC}"
echo -e "  Remove volumes: $REMOVE_VOLUMES"
echo -e "  Remove data:    $REMOVE_DATA"
echo ""
echo -e "${CYAN}To restart:${NC}"
echo -e "  ./start.sh"
echo ""
