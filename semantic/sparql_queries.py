"""
FAME Data Space - SPARQL Queries
=================================
Pre-built SPARQL queries for the FAME Financial Data Space.

Demonstrates:
- Basic queries (SELECT, ASK, DESCRIBE)
- Federated queries (across domains)
- Aggregate queries
- Filtering and pattern matching
"""

from typing import List, Dict, Optional

# ============================================================================
# NAMESPACE PREFIXES (include in all queries)
# ============================================================================

SPARQL_PREFIXES = """
PREFIX fame: <http://fame.eu/ontology#>
PREFIX fdata: <http://fame.eu/data/>
PREFIX fvocab: <http://fame.eu/vocabulary#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
"""


# ============================================================================
# QUERY 1: Basic Stock Retrieval
# Purpose: Get all stocks with their prices
# Domain: Market Data
# ============================================================================

QUERY_ALL_STOCKS = """
# QUERY: Get all stocks with basic information
# Expected Results: List of stocks with ticker, label, price, currency

SELECT ?stock ?ticker ?label ?price ?currency
WHERE {
    ?stock rdf:type fame:Stock .
    
    OPTIONAL { ?stock fame:hasTicker ?ticker }
    OPTIONAL { ?stock rdfs:label ?label }
    OPTIONAL { ?stock fame:price ?price }
    OPTIONAL { 
        ?stock fame:hasCurrency ?currencyUri .
        ?currencyUri rdfs:label ?currency .
    }
}
ORDER BY ?ticker
"""


# ============================================================================
# QUERY 2: High-Value Transactions
# Purpose: Find transactions above a threshold
# Domain: Payments
# ============================================================================

QUERY_HIGH_VALUE_TRANSACTIONS = """
# QUERY: Find high-value transactions (>10000 EUR)
# Expected Results: Transactions with sender, receiver, amount

SELECT ?tx ?amount ?sender_name ?receiver_name ?status
WHERE {
    ?tx rdf:type fame:Transaction .
    ?tx fame:amountEUR ?amount .
    
    ?tx fame:hasSender ?sender .
    ?sender rdfs:label ?sender_name .
    
    ?tx fame:hasReceiver ?receiver .
    ?receiver rdfs:label ?receiver_name .
    
    OPTIONAL { ?tx fame:status ?status }
    
    FILTER (?amount > 10000)
}
ORDER BY DESC(?amount)
LIMIT 50
"""


# ============================================================================
# QUERY 3: Cross-Border Payments Analysis
# Purpose: Find all cross-border transactions
# Domain: Payments
# ============================================================================

QUERY_CROSS_BORDER_TRANSACTIONS = """
# QUERY: Analyze cross-border transactions
# Expected Results: Cross-border tx with countries and amounts

SELECT ?tx ?amount_eur ?sender_country ?receiver_country ?channel
WHERE {
    ?tx rdf:type fame:Transaction .
    ?tx fame:isCrossBorder true .
    ?tx fame:amountEUR ?amount_eur .
    
    ?tx fame:hasSender ?sender .
    ?sender fame:country ?sender_country .
    
    ?tx fame:hasReceiver ?receiver .
    ?receiver fame:country ?receiver_country .
    
    OPTIONAL { ?tx fame:usesChannel ?channel }
}
ORDER BY DESC(?amount_eur)
"""


# ============================================================================
# QUERY 4: Exchange Rates from ECB
# Purpose: Get current EUR exchange rates
# Domain: Foreign Exchange
# ============================================================================

QUERY_ECB_EXCHANGE_RATES = """
# QUERY: Get all ECB exchange rates
# Expected Results: Currency pairs with rates

SELECT ?from_currency ?to_currency ?rate ?to_label
WHERE {
    ?fx rdf:type fame:ExchangeRate .
    ?fx fame:rate ?rate .
    
    ?fx fame:fromCurrency ?from_uri .
    ?from_uri skos:notation ?from_currency .
    
    ?fx fame:toCurrency ?to_uri .
    ?to_uri skos:notation ?to_currency .
    ?to_uri rdfs:label ?to_label .
}
ORDER BY ?to_currency
"""


# ============================================================================
# QUERY 5: Company Financial Performance
# Purpose: Get financial metrics for companies
# Domain: Corporate Finance
# ============================================================================

QUERY_COMPANY_FINANCIALS = """
# QUERY: Get company financial statements
# Expected Results: Companies with revenue, profit margin, ROE

SELECT ?company ?name ?sector ?year ?quarter ?revenue ?profit_margin ?roe
WHERE {
    ?company rdf:type fame:FinancialInstitution .
    ?company rdfs:label ?name .
    
    OPTIONAL { ?company fame:belongsToSector ?sector }
    
    ?company fame:hasFinancialStatement ?statement .
    
    OPTIONAL { ?statement fame:fiscalYear ?year }
    OPTIONAL { ?statement fame:fiscalQuarter ?quarter }
    OPTIONAL { ?statement fame:revenue ?revenue }
    OPTIONAL { ?statement fame:profitMargin ?profit_margin }
    OPTIONAL { ?statement fame:returnOnEquity ?roe }
}
ORDER BY ?name ?year DESC(?quarter)
"""


# ============================================================================
# QUERY 6: Top Performing Companies by ROE
# Purpose: Find companies with highest Return on Equity
# Domain: Corporate Finance
# ============================================================================

QUERY_TOP_ROE_COMPANIES = """
# QUERY: Top companies by Return on Equity
# Expected Results: Companies ranked by ROE

SELECT ?name ?sector ?roe ?revenue
WHERE {
    ?company rdf:type fame:FinancialInstitution .
    ?company rdfs:label ?name .
    ?company fame:belongsToSector ?sector .
    
    ?company fame:hasFinancialStatement ?statement .
    ?statement fame:returnOnEquity ?roe .
    ?statement fame:revenue ?revenue .
    
    FILTER (?roe > 5)
}
ORDER BY DESC(?roe)
LIMIT 10
"""


# ============================================================================
# QUERY 7: Multi-Domain Query - Transaction with Currency Conversion
# Purpose: Join transaction data with forex rates
# Domain: Cross-Domain (Payments + Forex)
# ============================================================================

QUERY_TRANSACTIONS_WITH_FX = """
# QUERY: Transactions with original and EUR amounts (cross-domain)
# Expected Results: Transactions showing currency conversion

SELECT ?tx ?original_amount ?currency ?rate ?amount_eur
WHERE {
    ?tx rdf:type fame:Transaction .
    ?tx fame:amount ?original_amount .
    ?tx fame:amountEUR ?amount_eur .
    
    ?tx fame:hasCurrency ?curr_uri .
    ?curr_uri skos:notation ?currency .
    
    OPTIONAL {
        ?fx rdf:type fame:ExchangeRate .
        ?fx fame:toCurrency ?curr_uri .
        ?fx fame:rate ?rate .
    }
}
LIMIT 20
"""


# ============================================================================
# QUERY 8: Aggregate - Total Transaction Volume by Country
# Purpose: Aggregate transactions by sender country
# Domain: Payments
# ============================================================================

QUERY_TX_VOLUME_BY_COUNTRY = """
# QUERY: Total transaction volume by sender country
# Expected Results: Countries with total volume and tx count

SELECT ?country (SUM(?amount) AS ?total_volume) (COUNT(?tx) AS ?tx_count)
WHERE {
    ?tx rdf:type fame:Transaction .
    ?tx fame:amountEUR ?amount .
    
    ?tx fame:hasSender ?sender .
    ?sender fame:country ?country .
}
GROUP BY ?country
ORDER BY DESC(?total_volume)
"""


# ============================================================================
# QUERY 9: ASK Query - Check if High-Risk Transaction Exists
# Purpose: Boolean check for compliance monitoring
# Domain: Payments / Risk
# ============================================================================

QUERY_ASK_HIGH_RISK = """
# QUERY: Check if any transaction exceeds 100,000 EUR (compliance check)
# Expected Results: true/false

ASK {
    ?tx rdf:type fame:Transaction .
    ?tx fame:amountEUR ?amount .
    FILTER (?amount > 100000)
}
"""


# ============================================================================
# QUERY 10: DESCRIBE Query - Full Entity Details
# Purpose: Get all triples about a specific entity
# Domain: Any
# ============================================================================

QUERY_DESCRIBE_ENTITY = """
# QUERY: Get all information about a specific stock
# Note: Replace APPLE_URI with actual URI

DESCRIBE <http://fame.eu/data/stock/AAPL>
"""


# ============================================================================
# QUERY 11: CONSTRUCT - Build Custom Graph
# Purpose: Create a simplified view of data
# Domain: Market Data
# ============================================================================

QUERY_CONSTRUCT_SIMPLE_STOCKS = """
# QUERY: Construct simplified stock graph for external use
# Expected Results: New RDF graph with simplified structure

CONSTRUCT {
    ?stock a fame:SimpleStock .
    ?stock fame:ticker ?ticker .
    ?stock fame:priceEUR ?price .
}
WHERE {
    ?stock rdf:type fame:Stock .
    ?stock fame:hasTicker ?ticker .
    ?stock fame:priceEUR ?price .
}
"""


# ============================================================================
# QUERY 12: Semantic Search - Find Related Entities
# Purpose: Use SKOS to find related concepts
# Domain: Vocabulary / Semantic
# ============================================================================

QUERY_SEMANTIC_RELATED = """
# QUERY: Find semantically related concepts using SKOS
# Expected Results: Concepts with their related terms

SELECT ?concept ?label ?broader ?narrower ?related
WHERE {
    ?concept rdf:type skos:Concept .
    ?concept skos:prefLabel ?label .
    
    OPTIONAL {
        ?concept skos:broader ?broader_concept .
        ?broader_concept skos:prefLabel ?broader .
    }
    
    OPTIONAL {
        ?concept skos:narrower ?narrower_concept .
        ?narrower_concept skos:prefLabel ?narrower .
    }
    
    OPTIONAL {
        ?concept skos:related ?related_concept .
        ?related_concept skos:prefLabel ?related .
    }
    
    FILTER (lang(?label) = 'en' || lang(?label) = '')
}
"""


# ============================================================================
# QUERY 13: Time-Series Analysis
# Purpose: Analyze transactions over time
# Domain: Payments
# ============================================================================

QUERY_TX_TIME_SERIES = """
# QUERY: Transaction volume by day
# Expected Results: Daily aggregates

SELECT ?date (SUM(?amount) AS ?daily_volume) (COUNT(?tx) AS ?tx_count)
WHERE {
    ?tx rdf:type fame:Transaction .
    ?tx fame:amountEUR ?amount .
    ?tx fame:timestamp ?timestamp .
    
    BIND(xsd:date(?timestamp) AS ?date)
}
GROUP BY ?date
ORDER BY ?date
"""


# ============================================================================
# QUERY 14: Find Duplicate/Related Customers
# Purpose: Entity resolution using names
# Domain: Payments
# ============================================================================

QUERY_CUSTOMER_MATCHING = """
# QUERY: Find customers with similar names (for entity resolution)
# Expected Results: Customer pairs with similar labels

SELECT ?customer1 ?name1 ?customer2 ?name2
WHERE {
    ?customer1 rdf:type fame:Customer .
    ?customer1 rdfs:label ?name1 .
    
    ?customer2 rdf:type fame:Customer .
    ?customer2 rdfs:label ?name2 .
    
    FILTER (?customer1 != ?customer2)
    FILTER (CONTAINS(LCASE(?name1), LCASE(?name2)) || CONTAINS(LCASE(?name2), LCASE(?name1)))
}
LIMIT 20
"""


# ============================================================================
# QUERY 15: Full Data Catalog
# Purpose: List all data types and counts
# Domain: Metadata
# ============================================================================

QUERY_DATA_CATALOG = """
# QUERY: Data catalog - counts by entity type
# Expected Results: Entity types with counts

SELECT ?type (COUNT(?entity) AS ?count)
WHERE {
    ?entity rdf:type ?type .
    FILTER (STRSTARTS(STR(?type), "http://fame.eu/ontology"))
}
GROUP BY ?type
ORDER BY DESC(?count)
"""


# ============================================================================
# Query Collection for Easy Access
# ============================================================================

FAME_QUERIES = {
    "all_stocks": {
        "name": "All Stocks",
        "description": "Get all stocks with basic information",
        "domain": "Market Data",
        "query": SPARQL_PREFIXES + QUERY_ALL_STOCKS
    },
    "high_value_transactions": {
        "name": "High-Value Transactions",
        "description": "Find transactions above 10,000 EUR",
        "domain": "Payments",
        "query": SPARQL_PREFIXES + QUERY_HIGH_VALUE_TRANSACTIONS
    },
    "cross_border": {
        "name": "Cross-Border Transactions",
        "description": "Analyze cross-border payment flows",
        "domain": "Payments",
        "query": SPARQL_PREFIXES + QUERY_CROSS_BORDER_TRANSACTIONS
    },
    "exchange_rates": {
        "name": "ECB Exchange Rates",
        "description": "Current EUR exchange rates from ECB",
        "domain": "Foreign Exchange",
        "query": SPARQL_PREFIXES + QUERY_ECB_EXCHANGE_RATES
    },
    "company_financials": {
        "name": "Company Financials",
        "description": "Financial statements by company",
        "domain": "Corporate Finance",
        "query": SPARQL_PREFIXES + QUERY_COMPANY_FINANCIALS
    },
    "top_roe": {
        "name": "Top Companies by ROE",
        "description": "Companies with highest Return on Equity",
        "domain": "Corporate Finance",
        "query": SPARQL_PREFIXES + QUERY_TOP_ROE_COMPANIES
    },
    "transactions_with_fx": {
        "name": "Transactions with FX",
        "description": "Cross-domain: transactions with currency conversion",
        "domain": "Cross-Domain",
        "query": SPARQL_PREFIXES + QUERY_TRANSACTIONS_WITH_FX
    },
    "volume_by_country": {
        "name": "Volume by Country",
        "description": "Aggregate transaction volume by country",
        "domain": "Payments",
        "query": SPARQL_PREFIXES + QUERY_TX_VOLUME_BY_COUNTRY
    },
    "high_risk_check": {
        "name": "High Risk Check",
        "description": "ASK query for compliance monitoring",
        "domain": "Risk/Compliance",
        "query": SPARQL_PREFIXES + QUERY_ASK_HIGH_RISK
    },
    "describe_entity": {
        "name": "Describe Entity",
        "description": "Full details about a specific entity",
        "domain": "Any",
        "query": SPARQL_PREFIXES + QUERY_DESCRIBE_ENTITY
    },
    "construct_simple": {
        "name": "Construct Simple Graph",
        "description": "Build simplified stock data view",
        "domain": "Market Data",
        "query": SPARQL_PREFIXES + QUERY_CONSTRUCT_SIMPLE_STOCKS
    },
    "semantic_related": {
        "name": "Semantic Related",
        "description": "Find semantically related concepts via SKOS",
        "domain": "Vocabulary",
        "query": SPARQL_PREFIXES + QUERY_SEMANTIC_RELATED
    },
    "time_series": {
        "name": "Transaction Time Series",
        "description": "Daily transaction volume analysis",
        "domain": "Payments",
        "query": SPARQL_PREFIXES + QUERY_TX_TIME_SERIES
    },
    "customer_matching": {
        "name": "Customer Matching",
        "description": "Entity resolution via name similarity",
        "domain": "Payments",
        "query": SPARQL_PREFIXES + QUERY_CUSTOMER_MATCHING
    },
    "data_catalog": {
        "name": "Data Catalog",
        "description": "Overview of all entity types and counts",
        "domain": "Metadata",
        "query": SPARQL_PREFIXES + QUERY_DATA_CATALOG
    }
}


def get_query(query_id: str) -> Optional[str]:
    """Get a query by ID."""
    if query_id in FAME_QUERIES:
        return FAME_QUERIES[query_id]["query"]
    return None


def list_queries() -> List[Dict]:
    """List all available queries."""
    return [
        {
            "id": qid,
            "name": q["name"],
            "description": q["description"],
            "domain": q["domain"]
        }
        for qid, q in FAME_QUERIES.items()
    ]


def execute_query(sparql_endpoint: str, query: str) -> List[Dict]:
    """
    Execute SPARQL query against an endpoint.
    
    Args:
        sparql_endpoint: URL of SPARQL endpoint (e.g., http://localhost:3030/fame/sparql)
        query: SPARQL query string
        
    Returns:
        List of result dictionaries
    """
    try:
        from SPARQLWrapper import SPARQLWrapper, JSON
        
        sparql = SPARQLWrapper(sparql_endpoint)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        
        results = sparql.query().convert()
        
        rows = []
        for result in results["results"]["bindings"]:
            row = {}
            for var in result:
                row[var] = result[var]["value"]
            rows.append(row)
        
        return rows
    except ImportError:
        print("⚠️ SPARQLWrapper not installed. Run: pip install SPARQLWrapper")
        return []
    except Exception as e:
        print(f"❌ Query error: {e}")
        return []


# CLI Test
if __name__ == "__main__":
    print("=" * 70)
    print("FAME Data Space - SPARQL Query Library")
    print("=" * 70)
    
    print("\n📋 Available Queries:\n")
    
    for q in list_queries():
        print(f"  [{q['id']}]")
        print(f"    📌 {q['name']}")
        print(f"    📝 {q['description']}")
        print(f"    🏷️  Domain: {q['domain']}")
        print()
    
    print("\n💡 Example Usage:")
    print("   from sparql_queries import FAME_QUERIES, execute_query")
    print("   ")
    print("   # Get a query")
    print("   query = FAME_QUERIES['all_stocks']['query']")
    print("   ")
    print("   # Execute against Fuseki")
    print("   results = execute_query('http://localhost:3030/fame/sparql', query)")
