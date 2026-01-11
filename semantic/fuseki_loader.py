"""
FAME Data Space - Fuseki RDF Loader
====================================
Automatically loads Protégé-created ontology files (OWL, RDF, SKOS) into Apache Fuseki.

This script:
1. Connects to Apache Fuseki SPARQL endpoint
2. Loads ontology (TBox) into named graph
3. Loads instance data (ABox) into named graph
4. Loads SKOS vocabulary into named graph
5. Validates loaded data with test queries

Usage:
    python fuseki_loader.py                    # Load all files
    python fuseki_loader.py --ontology-only    # Load only ontology
    python fuseki_loader.py --clear            # Clear all graphs first
"""

import os
import sys
import time
import logging
import argparse
import requests
from pathlib import Path
from typing import Optional, Dict, List, Tuple

# ============================================================================
# CONFIGURATION
# ============================================================================

# Fuseki endpoints
FUSEKI_HOST = os.environ.get("FUSEKI_HOST", "localhost")
FUSEKI_PORT = os.environ.get("FUSEKI_PORT", "3030")
FUSEKI_DATASET = os.environ.get("FUSEKI_DATASET", "fame")
FUSEKI_USER = os.environ.get("FUSEKI_USER", "admin")
FUSEKI_PASSWORD = os.environ.get("FUSEKI_PASSWORD", "admin123")

# Construct URLs
FUSEKI_BASE_URL = f"http://{FUSEKI_HOST}:{FUSEKI_PORT}"
FUSEKI_DATA_URL = f"{FUSEKI_BASE_URL}/{FUSEKI_DATASET}/data"
FUSEKI_QUERY_URL = f"{FUSEKI_BASE_URL}/{FUSEKI_DATASET}/query"
FUSEKI_UPDATE_URL = f"{FUSEKI_BASE_URL}/{FUSEKI_DATASET}/update"

# Named graphs for different file types
NAMED_GRAPHS = {
    "ontology": "http://fame.eu/graph/ontology",      # OWL TBox (classes, properties)
    "data": "http://fame.eu/graph/data",              # RDF ABox (instances)
    "vocabulary": "http://fame.eu/graph/vocabulary",  # SKOS concepts
    "inferred": "http://fame.eu/graph/inferred"       # Inferred triples
}

# File mappings (relative to semantic/ directory)
SEMANTIC_FILES = {
    "ontology": [
        "fame_data_protege.owl",   # Main Protégé ontology
        "fame_ontology.owl"        # Additional ontology
    ],
    "data": [
        "FAME-RDF.rdf"             # RDF instance data
    ],
    "vocabulary": [
        "FAME-SKOS.ttl",          # SKOS vocabulary
        "fame_vocabulary.skos"     # Additional SKOS
    ]
}

# Content types for different file formats
CONTENT_TYPES = {
    ".owl": "application/rdf+xml",
    ".rdf": "application/rdf+xml",
    ".ttl": "text/turtle",
    ".n3": "text/n3",
    ".nt": "application/n-triples",
    ".jsonld": "application/ld+json",
    ".skos": "text/turtle"  # SKOS files are typically Turtle
}

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# FUSEKI CLIENT CLASS
# ============================================================================

class FusekiLoader:
    """Client for loading RDF data into Apache Fuseki."""
    
    def __init__(self, base_url: str = FUSEKI_BASE_URL, dataset: str = FUSEKI_DATASET,
                 username: str = FUSEKI_USER, password: str = FUSEKI_PASSWORD):
        self.base_url = base_url
        self.dataset = dataset
        self.auth = (username, password) if username else None
        self.session = requests.Session()
        if self.auth:
            self.session.auth = self.auth
        
        # URLs
        self.data_url = f"{base_url}/{dataset}/data"
        self.query_url = f"{base_url}/{dataset}/query"
        self.update_url = f"{base_url}/{dataset}/update"
    
    def wait_for_fuseki(self, timeout: int = 60, interval: int = 5) -> bool:
        """Wait for Fuseki to be available."""
        logger.info(f"⏳ Waiting for Fuseki at {self.base_url}...")
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                response = self.session.get(f"{self.base_url}/$/ping")
                if response.status_code == 200:
                    logger.info("✅ Fuseki is ready!")
                    return True
            except requests.exceptions.ConnectionError:
                pass
            
            logger.info(f"   Waiting... ({int(time.time() - start_time)}s)")
            time.sleep(interval)
        
        logger.error(f"❌ Fuseki not available after {timeout}s")
        return False
    
    def clear_graph(self, graph_uri: str) -> bool:
        """Clear all triples from a named graph."""
        logger.info(f"🗑️  Clearing graph: {graph_uri}")
        
        sparql = f"CLEAR GRAPH <{graph_uri}>"
        
        try:
            response = self.session.post(
                self.update_url,
                data={"update": sparql},
                headers={"Content-Type": "application/x-www-form-urlencoded"}
            )
            
            if response.status_code in [200, 204]:
                logger.info(f"   ✅ Graph cleared: {graph_uri}")
                return True
            else:
                logger.warning(f"   ⚠️  Failed to clear graph: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"   ❌ Error clearing graph: {e}")
            return False
    
    def clear_all_graphs(self) -> bool:
        """Clear all named graphs."""
        logger.info("🗑️  Clearing all graphs...")
        success = True
        
        for graph_type, graph_uri in NAMED_GRAPHS.items():
            if not self.clear_graph(graph_uri):
                success = False
        
        return success
    
    def load_file(self, file_path: Path, graph_uri: str) -> Tuple[bool, int]:
        """
        Load an RDF file into a named graph.
        
        Args:
            file_path: Path to the RDF file
            graph_uri: URI of the target named graph
            
        Returns:
            Tuple of (success, triple_count)
        """
        if not file_path.exists():
            logger.warning(f"   ⚠️  File not found: {file_path}")
            return False, 0
        
        # Determine content type from extension
        ext = file_path.suffix.lower()
        content_type = CONTENT_TYPES.get(ext, "application/rdf+xml")
        
        logger.info(f"📤 Loading: {file_path.name} → {graph_uri}")
        logger.info(f"   Content-Type: {content_type}")
        
        try:
            # Read file content
            with open(file_path, "rb") as f:
                content = f.read()
            
            # Upload to Fuseki
            response = self.session.put(
                f"{self.data_url}?graph={graph_uri}",
                data=content,
                headers={"Content-Type": content_type}
            )
            
            if response.status_code in [200, 201, 204]:
                # Count triples in graph
                count = self.count_triples(graph_uri)
                logger.info(f"   ✅ Loaded successfully! ({count} triples)")
                return True, count
            else:
                logger.error(f"   ❌ Failed: {response.status_code} - {response.text[:200]}")
                return False, 0
                
        except Exception as e:
            logger.error(f"   ❌ Error loading file: {e}")
            return False, 0
    
    def load_file_append(self, file_path: Path, graph_uri: str) -> Tuple[bool, int]:
        """Load an RDF file into a named graph (append mode)."""
        if not file_path.exists():
            logger.warning(f"   ⚠️  File not found: {file_path}")
            return False, 0
        
        ext = file_path.suffix.lower()
        content_type = CONTENT_TYPES.get(ext, "application/rdf+xml")
        
        logger.info(f"📤 Appending: {file_path.name} → {graph_uri}")
        
        try:
            with open(file_path, "rb") as f:
                content = f.read()
            
            # POST for append (vs PUT for replace)
            response = self.session.post(
                f"{self.data_url}?graph={graph_uri}",
                data=content,
                headers={"Content-Type": content_type}
            )
            
            if response.status_code in [200, 201, 204]:
                count = self.count_triples(graph_uri)
                logger.info(f"   ✅ Appended successfully! (total: {count} triples)")
                return True, count
            else:
                logger.error(f"   ❌ Failed: {response.status_code}")
                return False, 0
                
        except Exception as e:
            logger.error(f"   ❌ Error: {e}")
            return False, 0
    
    def count_triples(self, graph_uri: Optional[str] = None) -> int:
        """Count triples in a graph or entire dataset."""
        if graph_uri:
            query = f"SELECT (COUNT(*) as ?count) WHERE {{ GRAPH <{graph_uri}> {{ ?s ?p ?o }} }}"
        else:
            query = "SELECT (COUNT(*) as ?count) WHERE { ?s ?p ?o }"
        
        try:
            response = self.session.post(
                self.query_url,
                data={"query": query},
                headers={"Accept": "application/sparql-results+json"}
            )
            
            if response.status_code == 200:
                results = response.json()
                return int(results["results"]["bindings"][0]["count"]["value"])
            return 0
            
        except Exception:
            return 0
    
    def execute_query(self, sparql: str) -> Dict:
        """Execute a SPARQL query and return results."""
        try:
            response = self.session.post(
                self.query_url,
                data={"query": sparql},
                headers={"Accept": "application/sparql-results+json"}
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Query failed: {response.status_code}")
                return {}
                
        except Exception as e:
            logger.error(f"Query error: {e}")
            return {}
    
    def validate_data(self) -> Dict[str, any]:
        """Validate loaded data with test queries."""
        logger.info("\n🔍 Validating loaded data...")
        
        validation_results = {
            "graphs": {},
            "classes": [],
            "properties": [],
            "instances": {}
        }
        
        # Count triples per graph
        for graph_type, graph_uri in NAMED_GRAPHS.items():
            count = self.count_triples(graph_uri)
            validation_results["graphs"][graph_type] = count
            logger.info(f"   📊 {graph_type}: {count} triples")
        
        # Get class hierarchy
        class_query = """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        PREFIX fame: <http://fame.eu/ontology#>
        
        SELECT DISTINCT ?class ?label
        WHERE {
            { ?class a owl:Class }
            UNION
            { ?class a rdfs:Class }
            OPTIONAL { ?class rdfs:label ?label }
            FILTER(STRSTARTS(STR(?class), "http://fame.eu/"))
        }
        ORDER BY ?class
        """
        
        results = self.execute_query(class_query)
        if results.get("results", {}).get("bindings"):
            classes = [b.get("class", {}).get("value", "") for b in results["results"]["bindings"]]
            validation_results["classes"] = classes
            logger.info(f"   📦 Classes found: {len(classes)}")
        
        # Count instances by type
        instance_query = """
        PREFIX fame: <http://fame.eu/ontology#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        
        SELECT ?type (COUNT(?instance) as ?count)
        WHERE {
            ?instance rdf:type ?type .
            FILTER(STRSTARTS(STR(?type), "http://fame.eu/"))
        }
        GROUP BY ?type
        ORDER BY DESC(?count)
        """
        
        results = self.execute_query(instance_query)
        if results.get("results", {}).get("bindings"):
            for binding in results["results"]["bindings"]:
                type_uri = binding.get("type", {}).get("value", "")
                count = int(binding.get("count", {}).get("value", 0))
                type_name = type_uri.split("#")[-1] if "#" in type_uri else type_uri.split("/")[-1]
                validation_results["instances"][type_name] = count
                logger.info(f"   📝 {type_name}: {count} instances")
        
        return validation_results


# ============================================================================
# MAIN LOADING FUNCTION
# ============================================================================

def load_all_semantic_files(clear_first: bool = False, ontology_only: bool = False,
                            data_only: bool = False, vocab_only: bool = False) -> Dict:
    """
    Load all semantic files into Fuseki.
    
    Args:
        clear_first: Clear all graphs before loading
        ontology_only: Load only ontology files
        data_only: Load only instance data files
        vocab_only: Load only vocabulary files
        
    Returns:
        Dictionary with loading results
    """
    logger.info("=" * 70)
    logger.info("🚀 FAME Data Space - Fuseki RDF Loader")
    logger.info("=" * 70)
    
    # Initialize loader
    loader = FusekiLoader()
    
    # Wait for Fuseki
    if not loader.wait_for_fuseki():
        return {"success": False, "error": "Fuseki not available"}
    
    # Clear graphs if requested
    if clear_first:
        loader.clear_all_graphs()
    
    # Determine semantic directory
    semantic_dir = Path(__file__).parent
    logger.info(f"\n📁 Semantic directory: {semantic_dir}")
    
    results = {
        "success": True,
        "files_loaded": [],
        "files_failed": [],
        "triple_counts": {}
    }
    
    # Determine which file types to load
    file_types_to_load = []
    if ontology_only:
        file_types_to_load = ["ontology"]
    elif data_only:
        file_types_to_load = ["data"]
    elif vocab_only:
        file_types_to_load = ["vocabulary"]
    else:
        file_types_to_load = ["ontology", "data", "vocabulary"]
    
    # Load files for each type
    for file_type in file_types_to_load:
        graph_uri = NAMED_GRAPHS.get(file_type, NAMED_GRAPHS["data"])
        files = SEMANTIC_FILES.get(file_type, [])
        
        logger.info(f"\n{'─' * 50}")
        logger.info(f"📂 Loading {file_type.upper()} files into: {graph_uri}")
        logger.info(f"{'─' * 50}")
        
        # Clear graph for this type first (to avoid duplicates on re-run)
        if not clear_first:
            loader.clear_graph(graph_uri)
        
        for filename in files:
            file_path = semantic_dir / filename
            
            # Also check in parent directory (for transferred files)
            if not file_path.exists():
                alt_paths = [
                    semantic_dir.parent / filename,
                    semantic_dir / "protege" / filename,
                    Path(f"c:/Users/ayakh/Downloads/{filename}")
                ]
                for alt_path in alt_paths:
                    if alt_path.exists():
                        file_path = alt_path
                        break
            
            success, count = loader.load_file_append(file_path, graph_uri)
            
            if success:
                results["files_loaded"].append(str(file_path))
                results["triple_counts"][filename] = count
            else:
                results["files_failed"].append(str(file_path))
    
    # Validate loaded data
    validation = loader.validate_data()
    results["validation"] = validation
    
    # Summary
    logger.info("\n" + "=" * 70)
    logger.info("📊 LOADING SUMMARY")
    logger.info("=" * 70)
    logger.info(f"✅ Files loaded: {len(results['files_loaded'])}")
    logger.info(f"❌ Files failed: {len(results['files_failed'])}")
    logger.info(f"📈 Total triples: {sum(validation['graphs'].values())}")
    
    if results["files_failed"]:
        logger.warning("\n⚠️  Failed files:")
        for f in results["files_failed"]:
            logger.warning(f"   - {f}")
        results["success"] = False
    
    return results


# ============================================================================
# CLI INTERFACE
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Load Protégé RDF/OWL/SKOS files into Apache Fuseki"
    )
    parser.add_argument(
        "--clear", "-c",
        action="store_true",
        help="Clear all graphs before loading"
    )
    parser.add_argument(
        "--ontology-only", "-o",
        action="store_true",
        help="Load only ontology files (OWL)"
    )
    parser.add_argument(
        "--data-only", "-d",
        action="store_true",
        help="Load only instance data files (RDF)"
    )
    parser.add_argument(
        "--vocab-only", "-v",
        action="store_true",
        help="Load only vocabulary files (SKOS)"
    )
    parser.add_argument(
        "--host",
        default="localhost",
        help="Fuseki host (default: localhost)"
    )
    parser.add_argument(
        "--port",
        default="3030",
        help="Fuseki port (default: 3030)"
    )
    
    args = parser.parse_args()
    
    # Update globals from args
    global FUSEKI_HOST, FUSEKI_PORT
    FUSEKI_HOST = args.host
    FUSEKI_PORT = args.port
    
    # Run loader
    results = load_all_semantic_files(
        clear_first=args.clear,
        ontology_only=args.ontology_only,
        data_only=args.data_only,
        vocab_only=args.vocab_only
    )
    
    # Exit code based on success
    sys.exit(0 if results.get("success") else 1)


if __name__ == "__main__":
    main()
