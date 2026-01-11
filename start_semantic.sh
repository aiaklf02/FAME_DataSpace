#!/bin/bash
# ═══════════════════════════════════════════════════════════════════════════════
# FAME Data Space - Complete Semantic Integration Startup
# ═══════════════════════════════════════════════════════════════════════════════
#
# This script:
# 1. Starts Docker services (including Fuseki)
# 2. Waits for Fuseki to be ready
# 3. Loads Protégé ontology files into named graphs
# 4. Validates the loaded data
#
# Usage:
#   ./start_semantic.sh              # Full startup
#   ./start_semantic.sh --load-only  # Only load data (assumes Docker is running)
#   ./start_semantic.sh --validate   # Only validate existing data
# ═══════════════════════════════════════════════════════════════════════════════

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Configuration
FUSEKI_HOST="${FUSEKI_HOST:-localhost}"
FUSEKI_PORT="${FUSEKI_PORT:-3030}"
FUSEKI_DATASET="${FUSEKI_DATASET:-fame}"
FUSEKI_USER="${FUSEKI_USER:-admin}"
FUSEKI_PASSWORD="${FUSEKI_PASSWORD:-admin123}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SEMANTIC_DIR="$SCRIPT_DIR/semantic"

echo -e "${CYAN}"
echo "═══════════════════════════════════════════════════════════════════════════════"
echo "   🚀 FAME Data Space - Semantic Layer Integration"
echo "   📚 Protégé + Apache Fuseki"
echo "═══════════════════════════════════════════════════════════════════════════════"
echo -e "${NC}"

# Parse arguments
LOAD_ONLY=false
VALIDATE_ONLY=false

for arg in "$@"; do
    case $arg in
        --load-only)
            LOAD_ONLY=true
            shift
            ;;
        --validate)
            VALIDATE_ONLY=true
            shift
            ;;
    esac
done

# Function to wait for Fuseki
wait_for_fuseki() {
    echo -e "${YELLOW}⏳ Waiting for Fuseki to be ready...${NC}"
    
    max_attempts=30
    attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        if curl -s "http://${FUSEKI_HOST}:${FUSEKI_PORT}/\$/ping" > /dev/null 2>&1; then
            echo -e "${GREEN}✅ Fuseki is ready!${NC}"
            return 0
        fi
        
        echo "   Attempt $attempt/$max_attempts..."
        sleep 2
        ((attempt++))
    done
    
    echo -e "${RED}❌ Fuseki did not become ready in time${NC}"
    return 1
}

# Function to load RDF file into named graph
load_rdf_file() {
    local file_path=$1
    local graph_uri=$2
    local content_type=$3
    
    if [ ! -f "$file_path" ]; then
        echo -e "${YELLOW}   ⚠️  File not found: $file_path${NC}"
        return 1
    fi
    
    local filename=$(basename "$file_path")
    echo -e "${BLUE}   📤 Loading: $filename → $graph_uri${NC}"
    
    response=$(curl -s -w "%{http_code}" -X PUT \
        -u "${FUSEKI_USER}:${FUSEKI_PASSWORD}" \
        -H "Content-Type: $content_type" \
        --data-binary "@$file_path" \
        "http://${FUSEKI_HOST}:${FUSEKI_PORT}/${FUSEKI_DATASET}/data?graph=$graph_uri")
    
    http_code="${response: -3}"
    
    if [ "$http_code" = "200" ] || [ "$http_code" = "201" ] || [ "$http_code" = "204" ]; then
        echo -e "${GREEN}   ✅ Loaded successfully${NC}"
        return 0
    else
        echo -e "${RED}   ❌ Failed with HTTP $http_code${NC}"
        return 1
    fi
}

# Function to count triples in graph
count_triples() {
    local graph_uri=$1
    
    local query="SELECT (COUNT(*) as ?count) WHERE { GRAPH <$graph_uri> { ?s ?p ?o } }"
    
    result=$(curl -s -X POST \
        -u "${FUSEKI_USER}:${FUSEKI_PASSWORD}" \
        -H "Accept: application/sparql-results+json" \
        --data-urlencode "query=$query" \
        "http://${FUSEKI_HOST}:${FUSEKI_PORT}/${FUSEKI_DATASET}/query")
    
    echo "$result" | python3 -c "import sys,json; print(json.load(sys.stdin)['results']['bindings'][0]['count']['value'])" 2>/dev/null || echo "0"
}

# Function to validate loaded data
validate_data() {
    echo -e "\n${CYAN}🔍 Validating loaded data...${NC}"
    
    # Count triples per graph
    local graphs=(
        "http://fame.eu/graph/ontology"
        "http://fame.eu/graph/data"
        "http://fame.eu/graph/vocabulary"
    )
    
    local total=0
    
    for graph in "${graphs[@]}"; do
        count=$(count_triples "$graph")
        total=$((total + count))
        graph_name=$(echo "$graph" | sed 's/.*\///')
        echo -e "   📊 $graph_name: ${GREEN}$count${NC} triples"
    done
    
    echo -e "\n   📈 Total triples: ${GREEN}$total${NC}"
    
    # Test query
    echo -e "\n${CYAN}🧪 Testing sample query...${NC}"
    
    local test_query="PREFIX fame: <http://fame.eu/ontology#> SELECT (COUNT(?stock) as ?count) WHERE { ?stock a fame:Stock }"
    
    result=$(curl -s -X POST \
        -u "${FUSEKI_USER}:${FUSEKI_PASSWORD}" \
        -H "Accept: application/sparql-results+json" \
        --data-urlencode "query=$test_query" \
        "http://${FUSEKI_HOST}:${FUSEKI_PORT}/${FUSEKI_DATASET}/query")
    
    stock_count=$(echo "$result" | python3 -c "import sys,json; print(json.load(sys.stdin)['results']['bindings'][0]['count']['value'])" 2>/dev/null || echo "0")
    echo -e "   🏢 Stocks found: ${GREEN}$stock_count${NC}"
}

# Main execution
main() {
    # Start Docker if not load-only or validate-only
    if [ "$LOAD_ONLY" = false ] && [ "$VALIDATE_ONLY" = false ]; then
        echo -e "\n${CYAN}🐳 Starting Docker services...${NC}"
        docker-compose up -d fuseki
        sleep 5
    fi
    
    # Wait for Fuseki
    if [ "$VALIDATE_ONLY" = false ]; then
        wait_for_fuseki || exit 1
    fi
    
    # Load data if not validate-only
    if [ "$VALIDATE_ONLY" = false ]; then
        echo -e "\n${CYAN}📂 Loading Protégé files into Fuseki...${NC}"
        echo "────────────────────────────────────────────────────────────"
        
        # Load Ontology (OWL)
        echo -e "\n${YELLOW}📦 Loading ONTOLOGY (TBox)...${NC}"
        load_rdf_file "$SEMANTIC_DIR/fame_data_protege.owl" "http://fame.eu/graph/ontology" "application/rdf+xml"
        load_rdf_file "$SEMANTIC_DIR/fame_ontology.owl" "http://fame.eu/graph/ontology" "application/rdf+xml"
        
        # Load Instance Data (RDF)
        echo -e "\n${YELLOW}📝 Loading INSTANCE DATA (ABox)...${NC}"
        load_rdf_file "$SEMANTIC_DIR/FAME-RDF.rdf" "http://fame.eu/graph/data" "application/rdf+xml"
        
        # Load Vocabulary (SKOS)
        echo -e "\n${YELLOW}📖 Loading VOCABULARY (SKOS)...${NC}"
        load_rdf_file "$SEMANTIC_DIR/FAME-SKOS.ttl" "http://fame.eu/graph/vocabulary" "text/turtle"
        load_rdf_file "$SEMANTIC_DIR/fame_vocabulary.skos" "http://fame.eu/graph/vocabulary" "text/turtle"
    fi
    
    # Validate
    validate_data
    
    # Summary
    echo -e "\n${CYAN}═══════════════════════════════════════════════════════════════════════════════${NC}"
    echo -e "${GREEN}✅ FAME Semantic Layer Integration Complete!${NC}"
    echo -e "${CYAN}═══════════════════════════════════════════════════════════════════════════════${NC}"
    echo ""
    echo -e "   🌐 Fuseki UI:     ${BLUE}http://localhost:3030${NC}"
    echo -e "   📊 SPARQL Query:  ${BLUE}http://localhost:3030/fame/query${NC}"
    echo -e "   📝 SPARQL Update: ${BLUE}http://localhost:3030/fame/update${NC}"
    echo ""
    echo -e "   📚 Named Graphs:"
    echo -e "      • Ontology:   http://fame.eu/graph/ontology"
    echo -e "      • Data:       http://fame.eu/graph/data"
    echo -e "      • Vocabulary: http://fame.eu/graph/vocabulary"
    echo ""
}

main "$@"
