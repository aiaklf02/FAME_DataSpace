"""
FAME Data Space - Grafana SPARQL Queries
=========================================
Pre-built SPARQL queries optimized for Grafana dashboards.

These queries are designed for:
- Time-series visualization
- Pie charts and bar charts
- Table displays
- Stat panels
- Alert conditions

Usage with Grafana:
    1. Install Grafana SPARQL datasource plugin
    2. Configure Fuseki endpoint: http://fuseki:3030/fame/query
    3. Use these queries in dashboard panels
"""

# ============================================================================
# GRAFANA-OPTIMIZED QUERIES
# ============================================================================

# Stock market overview - Pie Chart
GRAFANA_STOCKS_BY_SECTOR = """
PREFIX fame: <http://fame.eu/ontology#>
PREFIX fdata: <http://fame.eu/data#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?sector (COUNT(?stock) AS ?count)
WHERE {
    ?stock a fame:Stock ;
           fame:belongsToSector ?sectorUri .
    ?sectorUri rdfs:label ?sector .
}
GROUP BY ?sector
ORDER BY DESC(?count)
"""

# Stock prices - Table
GRAFANA_STOCK_PRICES = """
PREFIX fame: <http://fame.eu/ontology#>
PREFIX fdata: <http://fame.eu/data#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?symbol ?name ?price ?marketCap ?exchange
WHERE {
    ?stock a fame:Stock ;
           fame:symbol ?symbol .
    OPTIONAL { ?stock rdfs:label ?name }
    OPTIONAL { ?stock fame:price ?price }
    OPTIONAL { ?stock fame:marketCap ?marketCap }
    OPTIONAL { ?stock fame:tradedOn/rdfs:label ?exchange }
}
ORDER BY DESC(?marketCap)
LIMIT 50
"""

# Exchange rates - Table/Stat
GRAFANA_EXCHANGE_RATES = """
PREFIX fame: <http://fame.eu/ontology#>
PREFIX fdata: <http://fame.eu/data#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?baseCurrency ?targetCurrency ?rate
WHERE {
    ?fx a fame:ExchangeRate ;
        fame:baseCurrency/fame:currencyCode ?baseCurrency ;
        fame:targetCurrency/fame:currencyCode ?targetCurrency ;
        fame:rate ?rate .
}
ORDER BY ?targetCurrency
"""

# Transaction statistics - Stat Panel
GRAFANA_TRANSACTION_STATS = """
PREFIX fame: <http://fame.eu/ontology#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT 
    (COUNT(?tx) AS ?total_transactions)
    (SUM(?amount) AS ?total_volume)
    (AVG(?amount) AS ?avg_amount)
    (MAX(?amount) AS ?max_amount)
WHERE {
    ?tx a fame:Transaction ;
        fame:amount ?amount .
}
"""

# Transactions by status - Pie Chart
GRAFANA_TX_BY_STATUS = """
PREFIX fame: <http://fame.eu/ontology#>

SELECT ?status (COUNT(?tx) AS ?count)
WHERE {
    ?tx a fame:Transaction ;
        fame:status ?status .
}
GROUP BY ?status
ORDER BY DESC(?count)
"""

# Companies by sector - Bar Chart
GRAFANA_COMPANIES_BY_SECTOR = """
PREFIX fame: <http://fame.eu/ontology#>
PREFIX fdata: <http://fame.eu/data#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?sector (COUNT(?company) AS ?count)
WHERE {
    ?company a fame:Company ;
             fame:belongsToSector ?sectorUri .
    ?sectorUri rdfs:label ?sector .
}
GROUP BY ?sector
ORDER BY DESC(?count)
"""

# Knowledge base statistics - Stat Panel
GRAFANA_KB_STATS = """
PREFIX fame: <http://fame.eu/ontology#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

SELECT 
    ?metric ?value
WHERE {
    {
        SELECT ("Total Triples" AS ?metric) (COUNT(*) AS ?value)
        WHERE { ?s ?p ?o }
    }
    UNION
    {
        SELECT ("OWL Classes" AS ?metric) (COUNT(DISTINCT ?class) AS ?value)
        WHERE { ?class a owl:Class . FILTER(STRSTARTS(STR(?class), "http://fame.eu/")) }
    }
    UNION
    {
        SELECT ("Stocks" AS ?metric) (COUNT(?stock) AS ?value)
        WHERE { ?stock a fame:Stock }
    }
    UNION
    {
        SELECT ("Companies" AS ?metric) (COUNT(?company) AS ?value)
        WHERE { ?company a fame:Company }
    }
    UNION
    {
        SELECT ("Currencies" AS ?metric) (COUNT(?currency) AS ?value)
        WHERE { ?currency a fame:Currency }
    }
    UNION
    {
        SELECT ("Transactions" AS ?metric) (COUNT(?tx) AS ?value)
        WHERE { ?tx a fame:Transaction }
    }
    UNION
    {
        SELECT ("SKOS Concepts" AS ?metric) (COUNT(?concept) AS ?value)
        WHERE { ?concept a skos:Concept }
    }
}
"""

# Top stocks by market cap - Bar Chart
GRAFANA_TOP_STOCKS = """
PREFIX fame: <http://fame.eu/ontology#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?symbol ?name ?marketCap
WHERE {
    ?stock a fame:Stock ;
           fame:symbol ?symbol ;
           fame:marketCap ?marketCap .
    OPTIONAL { ?stock rdfs:label ?name }
}
ORDER BY DESC(?marketCap)
LIMIT 10
"""

# SKOS concept hierarchy - Table
GRAFANA_SKOS_CONCEPTS = """
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX fame: <http://fame.eu/skos#>
PREFIX dc: <http://purl.org/dc/elements/1.1/>

SELECT ?concept ?prefLabel ?broaderLabel ?narrowerCount
WHERE {
    ?concept a skos:Concept ;
             skos:prefLabel ?prefLabel .
    OPTIONAL { 
        ?concept skos:broader ?broader .
        ?broader skos:prefLabel ?broaderLabel .
    }
    OPTIONAL {
        SELECT ?concept (COUNT(?narrower) AS ?narrowerCount)
        WHERE { ?narrower skos:broader ?concept }
        GROUP BY ?concept
    }
    FILTER(LANG(?prefLabel) = "en" || LANG(?prefLabel) = "")
}
ORDER BY ?broaderLabel ?prefLabel
"""

# Data sources - Table
GRAFANA_DATA_SOURCES = """
PREFIX fame: <http://fame.eu/ontology#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX dcterms: <http://purl.org/dc/terms/>

SELECT ?source ?label ?format ?description
WHERE {
    ?source a fame:DataSource .
    OPTIONAL { ?source rdfs:label ?label }
    OPTIONAL { ?source dcterms:format ?format }
    OPTIONAL { ?source rdfs:comment ?description }
}
"""

# Alert query - High value transactions
GRAFANA_ALERT_HIGH_VALUE = """
PREFIX fame: <http://fame.eu/ontology#>

SELECT (COUNT(?tx) AS ?high_value_count)
WHERE {
    ?tx a fame:Transaction ;
        fame:amount ?amount .
    FILTER(?amount > 50000)
}
"""

# ============================================================================
# QUERY COLLECTION FOR GRAFANA
# ============================================================================

GRAFANA_QUERIES = {
    "stocks_by_sector": {
        "name": "Stocks by Sector",
        "panel_type": "piechart",
        "query": GRAFANA_STOCKS_BY_SECTOR
    },
    "stock_prices": {
        "name": "Stock Prices",
        "panel_type": "table",
        "query": GRAFANA_STOCK_PRICES
    },
    "exchange_rates": {
        "name": "Exchange Rates",
        "panel_type": "table",
        "query": GRAFANA_EXCHANGE_RATES
    },
    "transaction_stats": {
        "name": "Transaction Statistics",
        "panel_type": "stat",
        "query": GRAFANA_TRANSACTION_STATS
    },
    "tx_by_status": {
        "name": "Transactions by Status",
        "panel_type": "piechart",
        "query": GRAFANA_TX_BY_STATUS
    },
    "companies_by_sector": {
        "name": "Companies by Sector",
        "panel_type": "barchart",
        "query": GRAFANA_COMPANIES_BY_SECTOR
    },
    "kb_stats": {
        "name": "Knowledge Base Statistics",
        "panel_type": "stat",
        "query": GRAFANA_KB_STATS
    },
    "top_stocks": {
        "name": "Top Stocks by Market Cap",
        "panel_type": "barchart",
        "query": GRAFANA_TOP_STOCKS
    },
    "skos_concepts": {
        "name": "SKOS Concepts",
        "panel_type": "table",
        "query": GRAFANA_SKOS_CONCEPTS
    },
    "data_sources": {
        "name": "Data Sources",
        "panel_type": "table",
        "query": GRAFANA_DATA_SOURCES
    },
    "alert_high_value": {
        "name": "High Value Alert",
        "panel_type": "alert",
        "query": GRAFANA_ALERT_HIGH_VALUE
    }
}


if __name__ == "__main__":
    print("FAME Data Space - Grafana SPARQL Queries")
    print("=" * 50)
    for qid, q in GRAFANA_QUERIES.items():
        print(f"\n📊 {q['name']} [{q['panel_type']}]")
        print(f"   ID: {qid}")
