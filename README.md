# 🏦 FAME Financial Data Space

## Finance & Embedded Finance - Secure Financial Data Sharing Platform

![Architecture](https://img.shields.io/badge/Architecture-Data%20Lake%20+%20Data%20Fabric%20+%20DW-blue)
![Streaming](https://img.shields.io/badge/Streaming-Apache%20Kafka-orange)
![Semantic](https://img.shields.io/badge/Semantic-RDF%20/%20OWL%20/%20SPARQL-green)
![Warehouse](https://img.shields.io/badge/Warehouse-DuckDB-yellow)
![Docker](https://img.shields.io/badge/Container-Docker-2496ED)
![Grafana](https://img.shields.io/badge/Dashboards-Grafana-F46800)
![Redis](https://img.shields.io/badge/Cache-Redis-DC382D)
![Prometheus](https://img.shields.io/badge/Metrics-Prometheus-E6522C)

---

## 🌟 INNOVATION FEATURES

> **Ce projet va au-delà du cahier des charges avec des fonctionnalités innovantes**

| Innovation | Technology | Description |
|------------|------------|-------------|
| 🔴 **Real-time Dashboards** | Grafana | Monitoring temps réel avec auto-provisioning YAML |
| ⚡ **Caching Layer** | Redis | Cache intelligent pour performances optimales |
| 📈 **Metrics Collection** | Prometheus | Collecte métriques pipeline, qualité, KPIs financiers |
| 🚨 **Alerting System** | Custom Python | Détection d'anomalies + alertes multi-canal |
| 🔄 **EtLT Pattern** | DuckDB | Transformation IN-warehouse (plus moderne que ETL) |
| 🌐 **Real Data** | Yahoo Finance, ECB | Données réelles Internet (pas de mock data) |

---

## 📋 Project Overview

This project implements a **sectoral Data Space** for financial services (FAME - Finance & Embedded Finance), enabling:

- ✅ **Integration of 4 heterogeneous data sources** (API, XML, CSV, SQL)
- ✅ **Real-time streaming** with Apache Kafka + Spark
- ✅ **Data Lake (Bronze/Silver/Gold)** for raw data storage & processing
- ✅ **Data Fabric** for unified governance, metadata & data catalog
- ✅ **Data Warehouse (DuckDB + PostgreSQL)** for fast analytical queries & BI
- ✅ **Semantic interoperability** using RDF, SKOS, OWL, and SPARQL
- ✅ **Containerized deployment** with Docker Compose
- ✅ **Grafana Dashboards** with YAML auto-provisioning

---

## 📊 Data Sources (4 Heterogeneous Sources) - REAL DATA

| # | Source | Type | Format | Data | Volume |
|---|--------|------|--------|------|--------|
| 1 | **Yahoo Finance API** | REST API | JSON | 13 stocks temps réel | AAPL, MSFT, GOOGL, AMZN, NVDA... |
| 2 | **ECB Exchange Rates** | XML Feed | XML | 215,699 forex rates | 20 ans d'historique EUR |
| 3 | **Company Financials** | File System | CSV | 22,614 companies | SP500, NYSE, NASDAQ |
| 4 | **Transactions DB** | PostgreSQL | SQL | 759 transactions | Banking transactions |

### Source Details

#### Source 1: Yahoo Finance API (Real-time JSON)
- **Provider**: Yahoo Finance (yfinance)
- **Data**: Stock quotes, prices, volumes, market cap
- **Symbols**: AAPL, MSFT, GOOGL, AMZN, NVDA, META, TSLA, JPM, V, JNJ, WMT, PG, MA
- **Kafka Topic**: `fame-stocks`

#### Source 2: ECB Exchange Rates (XML Feed)
- **Provider**: European Central Bank
- **Data**: Official EUR exchange rates for 30+ currencies
- **History**: 20 years of daily rates (2004-2024)
- **File**: `data/bronze/xml/ecb_historical_20years.xml`

#### Source 3: Company Financials (CSV Batch)
- **Sources**: SP500, NYSE, NASDAQ listings + World GDP
- **Data**: Company info, sectors, market cap, PE ratios
- **Files**: `sp500_companies.csv`, `nyse_listings.csv`, `nasdaq_listings.csv`, `world_gdp.csv`

#### Source 4: Financial Transactions (PostgreSQL)
- **Type**: Transactional database
- **Data**: SEPA transfers, payments, card transactions
- **Features**: Multi-currency, cross-border flagging

---

## 🏗️ Architecture: Data Lake + Data Fabric + Data Warehouse

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                      FAME DATA SPACE ARCHITECTURE v3.0                            │
│                   Data Lake + Data Fabric + Data Warehouse + Grafana              │
├──────────────────────────────────────────────────────────────────────────────────┤
│                                                                                   │
│  ┌─────────────────────────── DATA SOURCES (REAL DATA) ─────────────────────────┐│
│  │  ┌───────────┐   ┌───────────┐   ┌───────────┐   ┌───────────┐              ││
│  │  │ SOURCE 1  │   │ SOURCE 2  │   │ SOURCE 3  │   │ SOURCE 4  │              ││
│  │  │Yahoo API  │   │ ECB XML   │   │ CSV Files │   │ PostgreSQL│              ││
│  │  │  (JSON)   │   │  (XML)    │   │   (CSV)   │   │   (SQL)   │              ││
│  │  │ 13 stocks │   │ 215K forex│   │ 22K co.   │   │ 759 tx    │              ││
│  │  └─────┬─────┘   └─────┬─────┘   └─────┬─────┘   └─────┬─────┘              ││
│  └────────┼───────────────┼───────────────┼───────────────┼─────────────────────┘│
│           │               │               │               │                      │
│           ▼               ▼               ▼               ▼                      │
│  ┌──────────────────────────────────────────────────────────────────────────────┐│
│  │                         APACHE KAFKA (Streaming)                             ││
│  │            fame-stocks              fame-alerts              fame-forex      ││
│  │                                                                              ││
│  │  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                   ││
│  │  │   Kafka UI   │    │   Zookeeper  │    │    Spark     │                   ││
│  │  │   :8080      │    │    :2181     │    │    :8081     │                   ││
│  │  └──────────────┘    └──────────────┘    └──────────────┘                   ││
│  └────────────────────────────────┬─────────────────────────────────────────────┘│
│                                   │                                              │
│  ┌────────────────────────────────┼─────────────────────────────────────────────┐│
│  │                    DATA FABRIC LAYER (Governance)                            ││
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     ││
│  │  │   Metadata   │  │ Data Catalog │  │   Lineage    │  │  Data Quality│     ││
│  │  │  Management  │  │   (Search)   │  │   Tracking   │  │    Rules     │     ││
│  │  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘     ││
│  └────────────────────────────────┬─────────────────────────────────────────────┘│
│                                   │                                              │
│                                   ▼                                              │
│  ┌──────────────────────────────────────────────────────────────────────────────┐│
│  │                        DATA LAKE (Local Storage)                             ││
│  │   ┌────────────┐     ┌────────────┐     ┌────────────┐                       ││
│  │   │   BRONZE   │ ──► │   SILVER   │ ──► │    GOLD    │                       ││
│  │   │  (Raw/JSON │     │ (Cleansed/ │     │  (Curated/ │                       ││
│  │   │  XML/CSV)  │     │   Clean)   │     │  Aggregated)│                      ││
│  │   └────────────┘     └────────────┘     └──────┬─────┘                       ││
│  └───────────────────────────────────────────────┼──────────────────────────────┘│
│                                                  │                               │
│           ┌──────────────────────────────────────┼───────────────────┐          │
│           │                                      │                   │          │
│           ▼                                      ▼                   ▼          │
│  ┌─────────────────┐              ┌─────────────────────┐   ┌────────────────┐  │
│  │  DATA WAREHOUSE │              │   SEMANTIC LAYER    │   │   POSTGRESQL   │  │
│  │    (DuckDB)     │              │                     │   │   (Analytics)  │  │
│  │                 │              │  ┌───────────────┐  │   │                │  │
│  │ ┌─────────────┐ │              │  │ OWL Ontology  │  │   │ fame_analytics │  │
│  │ │ DIM_COMPANY │ │              │  └───────────────┘  │   │ fame_streaming │  │
│  │ │ DIM_CURRENCY│ │              │  ┌───────────────┐  │   │                │  │
│  │ │ DIM_DATE    │ │              │  │SKOS Vocabulary│  │   └───────┬────────┘  │
│  │ └─────────────┘ │              │  └───────────────┘  │           │          │
│  │ ┌─────────────┐ │              │  ┌───────────────┐  │           │          │
│  │ │ FACT_TRADES │ │              │  │  RDF Store    │  │           │          │
│  │ │ FACT_FX     │ │              │  │  (Fuseki)     │  │           │          │
│  │ │FACT_PAYMENTS│ │              │  │   :3030       │  │           │          │
│  │ └─────────────┘ │              │  └───────────────┘  │           │          │
│  └────────┬────────┘              └──────────┬──────────┘           │          │
│           │                                  │                      │          │
│           └──────────────────────────────────┼──────────────────────┘          │
│                                              │                                  │
│                                              ▼                                  │
│  ┌──────────────────────────────────────────────────────────────────────────────┐│
│  │                    📊 GRAFANA DASHBOARDS (Auto-provisioned)                  ││
│  │  ┌──────────────────────────────────────────────────────────────────────┐   ││
│  │  │  📈 Stocks (13)  │  💱 Forex (215K)  │  📊 CSV (22K)  │  🔄 Kafka    │   ││
│  │  │  Yahoo Finance   │   ECB XML Data    │  SP500/NYSE    │  Streaming   │   ││
│  │  └──────────────────────────────────────────────────────────────────────┘   ││
│  │                        http://localhost:3000                                 ││
│  └──────────────────────────────────────────────────────────────────────────────┘│
│                                                                                   │
└──────────────────────────────────────────────────────────────────────────────────┘
```

---

## 🐳 Docker Infrastructure

### Services (12 Containers)

| Service | Image | Port | Description |
|---------|-------|------|-------------|
| **fame-kafka** | confluentinc/cp-kafka:7.5 | 29092 | Message streaming |
| **fame-zookeeper** | confluentinc/cp-zookeeper:7.5 | 2181 | Kafka coordination |
| **fame-kafka-ui** | provectuslabs/kafka-ui | 8080 | Kafka monitoring UI |
| **fame-postgres** | postgres:15-alpine | 5432 | Transaction database + Analytics |
| **fame-fuseki** | stain/jena-fuseki | 3030 | RDF Triple Store / SPARQL |
| **fame-spark-master** | bitnami/spark:3.5 | 8081 | Spark master node |
| **fame-spark-worker** | bitnami/spark:3.5 | - | Spark worker node |
| **fame-grafana** | grafana/grafana:latest | 3000 | 📊 Dashboards (YAML provisioned) |
| **fame-prometheus** | prom/prometheus | 9090 | Metrics collection |
| **fame-redis** | redis:7-alpine | 6379 | Caching layer |

### Quick Start

```powershell
# 1. Clone and navigate
git clone https://github.com/YOUR_REPO/FAME-DataSpace.git
cd FAME-DataSpace

# 2. Start all services
docker-compose up -d

# 3. Wait for services to be healthy
docker-compose ps

# 4. Run the complete ETL pipeline (real data from Internet)
python main.py all

# 5. Access the dashboards
```

### 🌐 Access Points

| Service | URL | Credentials |
|---------|-----|-------------|
| **📊 Grafana** | http://localhost:3000 | admin / admin |
| **📬 Kafka UI** | http://localhost:8080 | - |
| **🔍 Fuseki SPARQL** | http://localhost:3030 | admin / admin |
| **🗄️ PostgreSQL** | localhost:5432 | fame_user / fame_password |

---

## 📁 Project Structure

```
FAME-DataSpace/
│
├── 📂 docker/
│   ├── Dockerfile.etl                    # ETL service container
│   ├── Dockerfile.dashboard              # Dashboard container
│   ├── 📂 grafana/
│   │   ├── 📂 provisioning/
│   │   │   ├── 📂 datasources/
│   │   │   │   └── datasources.yml       # PostgreSQL auto-config
│   │   │   └── 📂 dashboards/
│   │   │       └── dashboards.yml        # Dashboard provider
│   │   └── 📂 dashboards/
│   │       └── fame-overview.json        # Main dashboard (10 panels)
│   ├── 📂 postgres/
│         └── init.sql                      # DB + schemas initialization
│
│
├── 📂 sources/                           # Data Source Connectors
│   ├── source1_stock_api.py              # Yahoo Finance API → Kafka
│   ├── source2_ecb_xml.py                # ECB XML → Kafka
│   ├── source3_financials_csv.py         # CSV Files → Kafka
│   ├── source4_transactions_db.py        # PostgreSQL → Kafka
│   ├── real_data_fetcher.py              # 🌐 Real Internet data fetcher
│   └── api_connector.py                  # API utilities
│
├── 📂 elt/                               # ELT Pipeline
│   ├── extract.py                        # Data extraction
│   ├── transform.py                      # Data transformation
│   ├── load.py                           # Data loading
│   ├── warehouse.py                      # DuckDB warehouse
│   ├── main_pipeline.py                  # Pipeline orchestration
│   └── superset_integration.py           # Export to PostgreSQL for Grafana
│
├── 📂 streaming/                         # Kafka Streaming
│   ├── kafka_producer.py                 # Send to Kafka topics
│   ├── kafka_consumer.py                 # Read from Kafka
│   ├── kafka_postgres_bridge.py          # Kafka → PostgreSQL bridge
│   └── spark_streaming.py                # Spark streaming jobs
│
├── 📂 semantic/                          # Semantic Layer
│   ├── fame_ontology.owl                 # OWL ontology
│   ├── fame_vocabulary.skos              # SKOS vocabulary
│   ├── rdf_generator.py                  # RDF triple generation
│   ├── sparql_queries.py                 # SPARQL query library
│   └── 📂 prototype/
│       └── app.py                        # Streamlit semantic UI
│
├── 📂 fabric/                            # Data Fabric Layer
│   ├── data_fabric.py                    # Metadata management
│   ├── cache_manager.py                  # Redis caching
│   ├── metrics_service.py                # Prometheus metrics
│   └── alert_system.py                   # Alerting system
│
├── 📂 data/
│   ├── 📂 bronze/                        # Raw data (landing zone)
│   │   ├── 📂 api/                       # JSON from Yahoo Finance
│   │   ├── 📂 xml/                       # XML from ECB (215K+ rates)
│   │   ├── 📂 csv/                       # CSV files (SP500, NYSE, NASDAQ)
│   │   └── 📂 sql/                       # SQL exports
│   ├── 📂 silver/                        # Cleansed data
│   ├── 📂 gold/                          # Aggregated data
│   ├── 📂 rdf/                           # RDF serializations
│   │   └── fame_sample.ttl               # Sample RDF triples
│   └── 📂 warehouse/
│       └── fame_warehouse.duckdb         # DuckDB warehouse
│
├── docker-compose.yml                    # 🐳 Full infrastructure (12 services)
├── main.py                               # 🚀 Main entry point
├── requirements.txt                      # Python dependencies
└── README.md                             # This file
```

---

## 🚀 Pipeline Commands

```powershell
# Run complete pipeline (extract + transform + load + warehouse)
python main.py all

# Run individual steps
python main.py extract      # Fetch real data from Internet
python main.py transform    # Clean and transform data
python main.py load         # Load to Data Lake layers
python main.py warehouse    # Build DuckDB warehouse

# Streaming mode
python main.py streaming --mode produce   # Start Kafka producer
python main.py streaming --mode consume   # Start Kafka consumer

# Export to PostgreSQL for Grafana
python -c "from elt.superset_integration import export_all_to_postgres; export_all_to_postgres()"
```

---

## 📊 Grafana Dashboard Panels

The FAME Financial Dashboard includes **10 panels** showing all data sources:

| Panel | Data Source | Type | Description |
|-------|-------------|------|-------------|
| 📈 Stocks (Yahoo API) | `fame_analytics.silver_silver_stocks` | Stat | Count: 13 stocks |
| 💱 Forex (ECB XML) | `fame_analytics.silver_silver_forex` | Stat | Count: 215,699 rates |
| 📊 Financials (CSV) | `fame_analytics.silver_silver_financials` | Stat | Count: 22,614 companies |
| 🔄 Kafka Streaming | `fame_streaming.stock_quotes` | Stat | Real-time quotes |
| 📈 Live Stock Prices | `silver_silver_stocks` | Table | Yahoo Finance data |
| 🔄 Kafka Quotes | `stock_quotes` | Table | Streaming quotes |
| 🚨 Real-Time Alerts | `alerts` | Table | Kafka alerts |
| 📊 Top Companies | `silver_silver_financials` | Table | SP500/NYSE/NASDAQ |
| 💱 Forex Rates | `silver_silver_forex` | Table | ECB exchange rates |
| 💳 Transactions | `silver_silver_transactions` | Table | Banking transactions |

### Grafana Auto-Provisioning

Grafana is configured with **YAML provisioning** - no manual setup required:

```yaml
# docker/grafana/provisioning/datasources/datasources.yml
datasources:
  - name: PostgreSQL
    type: grafana-postgresql-datasource
    uid: postgres
    url: fame-postgres:5432
    database: fame_transactions
    user: fame_user
    secureJsonData:
      password: fame_password
    isDefault: true
```

---

## 📚 Semantic Model (Ontology)

### 🔧 Tools Used

| Tool | Purpose | File |
|------|---------|------|
| **Protégé** | OWL Ontology Editor (Stanford) | `semantic/fame_ontology.owl` |
| **Apache Jena Fuseki** | RDF Triple Store + SPARQL Server | Docker :3030 |
| **RDFLib** | Python RDF manipulation | `semantic/rdf_generator.py` |

> 💡 **Protégé** est utilisé pour créer et éditer l'ontologie OWL. Téléchargez-le sur: https://protege.stanford.edu/

### Domains & Concepts

| Domain | Concepts | Relations |
|--------|----------|-----------|
| **Market Data** | Stock, Exchange, Price, Volume | hasTicker, tradedOn, hasPrice |
| **Foreign Exchange** | Currency, ExchangeRate, CentralBank | convertTo, publishedBy |
| **Corporate Finance** | Company, FinancialStatement, Ratio | hasReport, belongsToSector |
| **Transactions** | Transaction, Account, Bank | hasSender, hasReceiver, processedBy |

### SPARQL Endpoint

Access Fuseki at http://localhost:3030 to query RDF data:

```sparql
PREFIX fame: <http://fame.eu/ontology#>

SELECT ?company ?stockPrice ?sector
WHERE {
  ?company a fame:FinancialInstitution ;
           fame:hasTicker ?ticker ;
           fame:hasStockPrice ?stockPrice ;
           fame:hasSector ?sector .
}
LIMIT 10
```

---

## 🔧 Kafka Topics

```
fame-stocks         # Real-time stock prices from Yahoo Finance
fame-alerts         # Trading alerts and anomalies
fame-forex          # Exchange rate updates
```

---

## 📈 Data Volume Summary

| Layer | Table | Records | Source |
|-------|-------|---------|--------|
| **Silver** | silver_silver_stocks | 13 | Yahoo Finance API |
| **Silver** | silver_silver_forex | 215,699 | ECB XML (20 years) |
| **Silver** | silver_silver_financials | 22,614 | CSV (SP500/NYSE/NASDAQ) |
| **Silver** | silver_silver_transactions | 759 | PostgreSQL |
| **Streaming** | stock_quotes | 10+ | Kafka real-time |
| **Streaming** | alerts | 3+ | Kafka alerts |
| **Total** | - | **239,098+** | All sources |

---

## 🛠️ Technologies Used

| Category | Technology | Purpose |
|----------|------------|---------|
| **Streaming** | Apache Kafka | Real-time data ingestion |
| **Processing** | Apache Spark | Batch & stream processing |
| **Storage** | PostgreSQL | Relational data + analytics |
| **Warehouse** | DuckDB | Fast OLAP queries |
| **Semantic** | Apache Jena Fuseki | RDF triple store + SPARQL |
| **Ontology Editor** | Protégé | OWL ontology design & editing |
| **Visualization** | Grafana | Dashboards (YAML provisioned) |
| **Metrics** | Prometheus | Metrics collection |
| **Cache** | Redis | Performance caching |
| **Container** | Docker Compose | Infrastructure orchestration |

---

## 👥 Authors

**Master M2 - Data Space Project 2026**

## 📄 License

Academic Project - All Rights Reserved
