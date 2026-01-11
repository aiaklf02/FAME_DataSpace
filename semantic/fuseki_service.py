"""
FAME Data Space - Fuseki Service
==================================
Python service for CRUD operations and SPARQL queries against Apache Fuseki.

This service provides:
- CRUD operations for RDF data
- SPARQL query execution (SELECT, ASK, CONSTRUCT, DESCRIBE)
- SPARQL updates (INSERT, DELETE, UPDATE)
- Named graph management
- Inference support queries
- Integration with Data Fabric

Usage:
    from semantic.fuseki_service import FusekiService
    
    service = FusekiService()
    results = service.query("SELECT * WHERE { ?s ?p ?o } LIMIT 10")
"""

import os
import json
import logging
from typing import Optional, Dict, List, Any, Union
from dataclasses import dataclass
from enum import Enum

import requests
from requests.auth import HTTPBasicAuth

# ============================================================================
# CONFIGURATION
# ============================================================================

# Environment variables with defaults
FUSEKI_CONFIG = {
    "host": os.environ.get("FUSEKI_HOST", "localhost"),
    "port": os.environ.get("FUSEKI_PORT", "3030"),
    "dataset": os.environ.get("FUSEKI_DATASET", "fame"),
    "username": os.environ.get("FUSEKI_USER", "admin"),
    "password": os.environ.get("FUSEKI_PASSWORD", "admin123")
}

# Named graphs
class NamedGraph(str, Enum):
    """Named graphs in the FAME knowledge base."""
    ONTOLOGY = "http://fame.eu/graph/ontology"
    DATA = "http://fame.eu/graph/data"
    VOCABULARY = "http://fame.eu/graph/vocabulary"
    INFERRED = "http://fame.eu/graph/inferred"
    DEFAULT = "default"

# SPARQL prefixes
SPARQL_PREFIXES = """
PREFIX fame: <http://fame.eu/ontology#>
PREFIX fdata: <http://fame.eu/data#>
PREFIX fskos: <http://fame.eu/skos#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX schema: <http://schema.org/>
"""

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class SPARQLResult:
    """Container for SPARQL query results."""
    success: bool
    query_type: str
    data: Any
    error: Optional[str] = None
    execution_time_ms: float = 0.0


@dataclass
class Triple:
    """RDF Triple representation."""
    subject: str
    predicate: str
    obj: str  # 'object' is reserved
    graph: Optional[str] = None
    
    def to_sparql(self) -> str:
        """Convert to SPARQL triple pattern."""
        s = f"<{self.subject}>" if self.subject.startswith("http") else self.subject
        p = f"<{self.predicate}>" if self.predicate.startswith("http") else self.predicate
        
        # Handle object (could be URI or literal)
        if self.obj.startswith("http"):
            o = f"<{self.obj}>"
        elif self.obj.startswith('"'):
            o = self.obj  # Already formatted literal
        else:
            o = f'"{self.obj}"'
        
        return f"{s} {p} {o}"


# ============================================================================
# FUSEKI SERVICE CLASS
# ============================================================================

class FusekiService:
    """
    Service for interacting with Apache Fuseki triple store.
    
    Provides CRUD operations, SPARQL queries, and graph management.
    """
    
    def __init__(self, host: str = None, port: str = None, dataset: str = None,
                 username: str = None, password: str = None):
        """Initialize Fuseki service with connection parameters."""
        self.host = host or FUSEKI_CONFIG["host"]
        self.port = port or FUSEKI_CONFIG["port"]
        self.dataset = dataset or FUSEKI_CONFIG["dataset"]
        self.username = username or FUSEKI_CONFIG["username"]
        self.password = password or FUSEKI_CONFIG["password"]
        
        # Build URLs
        self.base_url = f"http://{self.host}:{self.port}"
        self.query_url = f"{self.base_url}/{self.dataset}/query"
        self.update_url = f"{self.base_url}/{self.dataset}/update"
        self.data_url = f"{self.base_url}/{self.dataset}/data"
        self.upload_url = f"{self.base_url}/{self.dataset}/upload"
        
        # Session with auth
        self.session = requests.Session()
        if self.username and self.password:
            self.session.auth = HTTPBasicAuth(self.username, self.password)
        
        logger.info(f"FusekiService initialized: {self.base_url}/{self.dataset}")
    
    # ────────────────────────────────────────────────────────────────────────
    # Connection & Health
    # ────────────────────────────────────────────────────────────────────────
    
    def is_available(self) -> bool:
        """Check if Fuseki server is available."""
        try:
            response = self.session.get(f"{self.base_url}/$/ping", timeout=5)
            return response.status_code == 200
        except requests.exceptions.RequestException:
            return False
    
    def get_server_info(self) -> Dict:
        """Get Fuseki server information."""
        try:
            response = self.session.get(f"{self.base_url}/$/server")
            if response.status_code == 200:
                return response.json()
            return {}
        except Exception as e:
            logger.error(f"Failed to get server info: {e}")
            return {}
    
    def get_dataset_info(self) -> Dict:
        """Get dataset statistics."""
        try:
            response = self.session.get(f"{self.base_url}/$/datasets/{self.dataset}")
            if response.status_code == 200:
                return response.json()
            return {}
        except Exception as e:
            logger.error(f"Failed to get dataset info: {e}")
            return {}
    
    # ────────────────────────────────────────────────────────────────────────
    # SPARQL Query Operations
    # ────────────────────────────────────────────────────────────────────────
    
    def query(self, sparql: str, include_prefixes: bool = True,
              timeout: int = 30) -> SPARQLResult:
        """
        Execute a SPARQL SELECT/ASK query.
        
        Args:
            sparql: SPARQL query string
            include_prefixes: Prepend standard prefixes
            timeout: Query timeout in seconds
            
        Returns:
            SPARQLResult with query results
        """
        import time
        start_time = time.time()
        
        # Add prefixes if needed
        if include_prefixes and not sparql.strip().upper().startswith("PREFIX"):
            sparql = SPARQL_PREFIXES + "\n" + sparql
        
        try:
            response = self.session.post(
                self.query_url,
                data={"query": sparql},
                headers={"Accept": "application/sparql-results+json"},
                timeout=timeout
            )
            
            execution_time = (time.time() - start_time) * 1000
            
            if response.status_code == 200:
                result = response.json()
                
                # Determine query type
                if "boolean" in result:
                    query_type = "ASK"
                    data = result["boolean"]
                else:
                    query_type = "SELECT"
                    data = self._parse_select_results(result)
                
                return SPARQLResult(
                    success=True,
                    query_type=query_type,
                    data=data,
                    execution_time_ms=execution_time
                )
            else:
                return SPARQLResult(
                    success=False,
                    query_type="UNKNOWN",
                    data=None,
                    error=f"HTTP {response.status_code}: {response.text[:200]}",
                    execution_time_ms=execution_time
                )
                
        except Exception as e:
            return SPARQLResult(
                success=False,
                query_type="UNKNOWN",
                data=None,
                error=str(e),
                execution_time_ms=(time.time() - start_time) * 1000
            )
    
    def _parse_select_results(self, result: Dict) -> List[Dict]:
        """Parse SPARQL SELECT results into list of dictionaries."""
        bindings = result.get("results", {}).get("bindings", [])
        parsed = []
        
        for binding in bindings:
            row = {}
            for var, value_info in binding.items():
                value = value_info.get("value")
                datatype = value_info.get("datatype")
                
                # Convert to appropriate Python type
                if datatype:
                    if "integer" in datatype:
                        value = int(value)
                    elif "decimal" in datatype or "float" in datatype or "double" in datatype:
                        value = float(value)
                    elif "boolean" in datatype:
                        value = value.lower() == "true"
                    elif "date" in datatype:
                        value = value  # Keep as string for now
                
                row[var] = value
            parsed.append(row)
        
        return parsed
    
    def construct(self, sparql: str, format: str = "turtle") -> SPARQLResult:
        """Execute a SPARQL CONSTRUCT query."""
        if not sparql.strip().upper().startswith("PREFIX"):
            sparql = SPARQL_PREFIXES + "\n" + sparql
        
        accept_types = {
            "turtle": "text/turtle",
            "rdfxml": "application/rdf+xml",
            "ntriples": "application/n-triples",
            "jsonld": "application/ld+json"
        }
        
        try:
            response = self.session.post(
                self.query_url,
                data={"query": sparql},
                headers={"Accept": accept_types.get(format, "text/turtle")}
            )
            
            if response.status_code == 200:
                return SPARQLResult(
                    success=True,
                    query_type="CONSTRUCT",
                    data=response.text
                )
            else:
                return SPARQLResult(
                    success=False,
                    query_type="CONSTRUCT",
                    data=None,
                    error=response.text
                )
                
        except Exception as e:
            return SPARQLResult(
                success=False,
                query_type="CONSTRUCT",
                data=None,
                error=str(e)
            )
    
    def describe(self, resource_uri: str) -> SPARQLResult:
        """Get all triples about a resource."""
        sparql = f"DESCRIBE <{resource_uri}>"
        return self.construct(sparql)
    
    # ────────────────────────────────────────────────────────────────────────
    # SPARQL Update Operations
    # ────────────────────────────────────────────────────────────────────────
    
    def update(self, sparql: str) -> SPARQLResult:
        """Execute a SPARQL UPDATE operation."""
        if not sparql.strip().upper().startswith("PREFIX"):
            sparql = SPARQL_PREFIXES + "\n" + sparql
        
        try:
            response = self.session.post(
                self.update_url,
                data={"update": sparql},
                headers={"Content-Type": "application/x-www-form-urlencoded"}
            )
            
            success = response.status_code in [200, 204]
            
            return SPARQLResult(
                success=success,
                query_type="UPDATE",
                data=None if success else response.text,
                error=None if success else f"HTTP {response.status_code}"
            )
            
        except Exception as e:
            return SPARQLResult(
                success=False,
                query_type="UPDATE",
                data=None,
                error=str(e)
            )
    
    def insert_triple(self, triple: Triple) -> SPARQLResult:
        """Insert a single triple."""
        if triple.graph:
            sparql = f"""
            INSERT DATA {{
                GRAPH <{triple.graph}> {{
                    {triple.to_sparql()} .
                }}
            }}
            """
        else:
            sparql = f"INSERT DATA {{ {triple.to_sparql()} . }}"
        
        return self.update(sparql)
    
    def insert_triples(self, triples: List[Triple], graph: str = None) -> SPARQLResult:
        """Insert multiple triples."""
        triple_patterns = " .\n        ".join(t.to_sparql() for t in triples)
        
        if graph:
            sparql = f"""
            INSERT DATA {{
                GRAPH <{graph}> {{
                    {triple_patterns} .
                }}
            }}
            """
        else:
            sparql = f"INSERT DATA {{ {triple_patterns} . }}"
        
        return self.update(sparql)
    
    def delete_triple(self, triple: Triple) -> SPARQLResult:
        """Delete a single triple."""
        if triple.graph:
            sparql = f"""
            DELETE DATA {{
                GRAPH <{triple.graph}> {{
                    {triple.to_sparql()} .
                }}
            }}
            """
        else:
            sparql = f"DELETE DATA {{ {triple.to_sparql()} . }}"
        
        return self.update(sparql)
    
    def clear_graph(self, graph: Union[str, NamedGraph]) -> SPARQLResult:
        """Clear all triples from a named graph."""
        graph_uri = graph.value if isinstance(graph, NamedGraph) else graph
        return self.update(f"CLEAR GRAPH <{graph_uri}>")
    
    # ────────────────────────────────────────────────────────────────────────
    # CRUD Operations for FAME Entities
    # ────────────────────────────────────────────────────────────────────────
    
    def get_all_stocks(self, limit: int = 100) -> List[Dict]:
        """Get all stocks with their properties."""
        sparql = f"""
        SELECT ?stock ?symbol ?name ?price ?currency ?exchange
        WHERE {{
            ?stock a fame:Stock .
            OPTIONAL {{ ?stock fame:symbol ?symbol }}
            OPTIONAL {{ ?stock rdfs:label ?name }}
            OPTIONAL {{ ?stock fame:price ?price }}
            OPTIONAL {{ ?stock fame:hasCurrency/fame:currencyCode ?currency }}
            OPTIONAL {{ ?stock fame:tradedOn/rdfs:label ?exchange }}
        }}
        LIMIT {limit}
        """
        result = self.query(sparql)
        return result.data if result.success else []
    
    def get_stock_by_symbol(self, symbol: str) -> Optional[Dict]:
        """Get a stock by its ticker symbol."""
        sparql = f"""
        SELECT ?stock ?name ?price ?currency ?marketCap ?exchange
        WHERE {{
            ?stock a fame:Stock ;
                   fame:symbol "{symbol}" .
            OPTIONAL {{ ?stock rdfs:label ?name }}
            OPTIONAL {{ ?stock fame:price ?price }}
            OPTIONAL {{ ?stock fame:hasCurrency/fame:currencyCode ?currency }}
            OPTIONAL {{ ?stock fame:marketCap ?marketCap }}
            OPTIONAL {{ ?stock fame:tradedOn/rdfs:label ?exchange }}
        }}
        """
        result = self.query(sparql)
        return result.data[0] if result.success and result.data else None
    
    def get_all_currencies(self) -> List[Dict]:
        """Get all currencies."""
        sparql = """
        SELECT ?currency ?code ?name ?symbol
        WHERE {
            ?currency a fame:Currency .
            OPTIONAL { ?currency fame:currencyCode ?code }
            OPTIONAL { ?currency rdfs:label ?name }
            OPTIONAL { ?currency fame:symbol ?symbol }
        }
        ORDER BY ?code
        """
        result = self.query(sparql)
        return result.data if result.success else []
    
    def get_exchange_rates(self, base: str = "EUR", limit: int = 50) -> List[Dict]:
        """Get exchange rates for a base currency."""
        sparql = f"""
        SELECT ?rate ?target ?value ?date
        WHERE {{
            ?rate a fame:ExchangeRate ;
                  fame:baseCurrency/fame:currencyCode "{base}" ;
                  fame:targetCurrency/fame:currencyCode ?target ;
                  fame:rate ?value .
            OPTIONAL {{ ?rate fame:date ?date }}
        }}
        ORDER BY ?target
        LIMIT {limit}
        """
        result = self.query(sparql)
        return result.data if result.success else []
    
    def get_companies_by_sector(self, sector: str) -> List[Dict]:
        """Get companies in a specific sector."""
        sparql = f"""
        SELECT ?company ?name ?stock ?marketCap
        WHERE {{
            ?company a fame:Company ;
                     fame:belongsToSector ?sector .
            ?sector rdfs:label ?sectorLabel .
            FILTER(CONTAINS(LCASE(?sectorLabel), LCASE("{sector}")))
            OPTIONAL {{ ?company rdfs:label ?name }}
            OPTIONAL {{ ?company fame:hasStock ?stock }}
            OPTIONAL {{ ?stock fame:marketCap ?marketCap }}
        }}
        ORDER BY DESC(?marketCap)
        """
        result = self.query(sparql)
        return result.data if result.success else []
    
    def get_transactions(self, status: str = None, min_amount: float = None,
                        limit: int = 100) -> List[Dict]:
        """Get transactions with optional filters."""
        filters = []
        if status:
            filters.append(f'FILTER(?status = "{status}")')
        if min_amount:
            filters.append(f'FILTER(?amount > {min_amount})')
        
        filter_clause = "\n            ".join(filters)
        
        sparql = f"""
        SELECT ?tx ?txId ?amount ?currency ?status ?date
        WHERE {{
            ?tx a fame:Transaction .
            OPTIONAL {{ ?tx fame:transactionId ?txId }}
            OPTIONAL {{ ?tx fame:amount ?amount }}
            OPTIONAL {{ ?tx fame:transactionCurrency/fame:currencyCode ?currency }}
            OPTIONAL {{ ?tx fame:status ?status }}
            OPTIONAL {{ ?tx fame:date ?date }}
            {filter_clause}
        }}
        ORDER BY DESC(?date)
        LIMIT {limit}
        """
        result = self.query(sparql)
        return result.data if result.success else []
    
    def create_stock(self, symbol: str, name: str, price: float, currency: str = "USD",
                    exchange: str = None, market_cap: float = None) -> SPARQLResult:
        """Create a new stock entity."""
        stock_uri = f"http://fame.eu/data#{symbol}"
        
        triples = [
            Triple(stock_uri, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", 
                   "http://fame.eu/ontology#Stock"),
            Triple(stock_uri, "http://fame.eu/ontology#symbol", symbol),
            Triple(stock_uri, "http://www.w3.org/2000/01/rdf-schema#label", name),
            Triple(stock_uri, "http://fame.eu/ontology#price", f'"{price}"^^xsd:decimal'),
        ]
        
        if currency:
            currency_uri = f"http://fame.eu/data#{currency}"
            triples.append(Triple(stock_uri, "http://fame.eu/ontology#hasCurrency", currency_uri))
        
        if exchange:
            exchange_uri = f"http://fame.eu/data#{exchange}"
            triples.append(Triple(stock_uri, "http://fame.eu/ontology#tradedOn", exchange_uri))
        
        if market_cap:
            triples.append(Triple(stock_uri, "http://fame.eu/ontology#marketCap", 
                                 f'"{market_cap}"^^xsd:decimal'))
        
        return self.insert_triples(triples, NamedGraph.DATA.value)
    
    # ────────────────────────────────────────────────────────────────────────
    # SKOS Vocabulary Queries
    # ────────────────────────────────────────────────────────────────────────
    
    def get_concept_scheme(self) -> Dict:
        """Get SKOS concept scheme information."""
        sparql = """
        SELECT ?scheme ?title ?description
        WHERE {
            ?scheme a skos:ConceptScheme .
            OPTIONAL { ?scheme dc:title ?title }
            OPTIONAL { ?scheme dcterms:description ?description }
        }
        """
        result = self.query(sparql)
        return result.data[0] if result.success and result.data else {}
    
    def get_top_concepts(self) -> List[Dict]:
        """Get top-level SKOS concepts."""
        sparql = """
        SELECT ?concept ?prefLabel ?definition
        WHERE {
            ?scheme skos:hasTopConcept ?concept .
            ?concept skos:prefLabel ?prefLabel .
            OPTIONAL { ?concept skos:definition ?definition }
            FILTER(LANG(?prefLabel) = "en" || LANG(?prefLabel) = "")
        }
        """
        result = self.query(sparql)
        return result.data if result.success else []
    
    def get_narrower_concepts(self, concept_uri: str) -> List[Dict]:
        """Get narrower concepts for a given concept."""
        sparql = f"""
        SELECT ?concept ?prefLabel ?altLabel ?definition
        WHERE {{
            <{concept_uri}> skos:narrower ?concept .
            ?concept skos:prefLabel ?prefLabel .
            OPTIONAL {{ ?concept skos:altLabel ?altLabel }}
            OPTIONAL {{ ?concept skos:definition ?definition }}
            FILTER(LANG(?prefLabel) = "en" || LANG(?prefLabel) = "")
        }}
        """
        result = self.query(sparql)
        return result.data if result.success else []
    
    def search_concepts(self, keyword: str) -> List[Dict]:
        """Search concepts by keyword in labels and definitions."""
        sparql = f"""
        SELECT DISTINCT ?concept ?prefLabel ?altLabel ?definition
        WHERE {{
            ?concept a skos:Concept .
            {{
                ?concept skos:prefLabel ?prefLabel .
                FILTER(CONTAINS(LCASE(?prefLabel), LCASE("{keyword}")))
            }}
            UNION
            {{
                ?concept skos:altLabel ?altLabel .
                FILTER(CONTAINS(LCASE(?altLabel), LCASE("{keyword}")))
            }}
            UNION
            {{
                ?concept skos:definition ?definition .
                FILTER(CONTAINS(LCASE(?definition), LCASE("{keyword}")))
            }}
            OPTIONAL {{ ?concept skos:prefLabel ?prefLabel }}
            OPTIONAL {{ ?concept skos:altLabel ?altLabel }}
            OPTIONAL {{ ?concept skos:definition ?definition }}
        }}
        LIMIT 50
        """
        result = self.query(sparql)
        return result.data if result.success else []
    
    # ────────────────────────────────────────────────────────────────────────
    # Statistics & Analytics
    # ────────────────────────────────────────────────────────────────────────
    
    def get_statistics(self) -> Dict:
        """Get knowledge base statistics."""
        stats = {}
        
        # Total triples
        result = self.query("SELECT (COUNT(*) as ?count) WHERE { ?s ?p ?o }")
        stats["total_triples"] = result.data[0]["count"] if result.success and result.data else 0
        
        # Triples per graph
        graph_query = """
        SELECT ?graph (COUNT(*) as ?count)
        WHERE {
            GRAPH ?graph { ?s ?p ?o }
        }
        GROUP BY ?graph
        """
        result = self.query(graph_query)
        stats["graphs"] = {r["graph"]: r["count"] for r in result.data} if result.success else {}
        
        # Count by class
        class_query = """
        SELECT ?class (COUNT(?instance) as ?count)
        WHERE {
            ?instance a ?class .
            FILTER(STRSTARTS(STR(?class), "http://fame.eu/"))
        }
        GROUP BY ?class
        ORDER BY DESC(?count)
        """
        result = self.query(class_query)
        if result.success:
            stats["classes"] = {
                r["class"].split("#")[-1]: r["count"] 
                for r in result.data
            }
        
        return stats
    
    def get_inference_statistics(self) -> Dict:
        """Get statistics about inferred triples."""
        sparql = """
        SELECT (COUNT(*) as ?inferred)
        WHERE {
            GRAPH <http://fame.eu/graph/inferred> { ?s ?p ?o }
        }
        """
        result = self.query(sparql)
        return {
            "inferred_triples": result.data[0]["inferred"] if result.success and result.data else 0
        }


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def get_fuseki_service() -> FusekiService:
    """Get a configured Fuseki service instance."""
    return FusekiService()


def execute_sparql(query: str) -> List[Dict]:
    """Execute a SPARQL query and return results."""
    service = FusekiService()
    result = service.query(query)
    return result.data if result.success else []


# ============================================================================
# MAIN - Test functionality
# ============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("🔗 FAME Fuseki Service - Connection Test")
    print("=" * 60)
    
    service = FusekiService()
    
    # Test connection
    if service.is_available():
        print("✅ Fuseki is available")
        
        # Get statistics
        stats = service.get_statistics()
        print(f"\n📊 Knowledge Base Statistics:")
        print(f"   Total triples: {stats.get('total_triples', 0)}")
        
        if stats.get('classes'):
            print(f"\n   Classes:")
            for cls, count in list(stats['classes'].items())[:10]:
                print(f"      {cls}: {count}")
        
        # Test stock query
        print("\n🔍 Sample Stocks:")
        stocks = service.get_all_stocks(limit=5)
        for stock in stocks:
            print(f"   {stock.get('symbol', 'N/A')}: {stock.get('name', 'N/A')} - ${stock.get('price', 'N/A')}")
        
    else:
        print("❌ Fuseki is not available")
        print("   Make sure Fuseki is running: docker-compose up fuseki")
