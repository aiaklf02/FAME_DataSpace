# 🏦 FAME Financial Data Space

## Finance & Embedded Finance - Secure Financial Data Sharing Platform

![Architecture](https://img.shields.io/badge/Architecture-Data%20Lake%20+%20Data%20Fabric%20+%20DW-blue)
![Streaming](https://img.shields.io/badge/Streaming-Apache%20Kafka-orange)
![Semantic](https://img.shields.io/badge/Semantic-RDF%20/%20OWL%20/%20SPARQL-green)
![Warehouse](https://img.shields.io/badge/Warehouse-DuckDB-yellow)
![Docker](https://img.shields.io/badge/Container-Docker-2496ED)
![Grafana](https://img.shields.io/badge/Monitoring-Grafana-F46800)
![Superset](https://img.shields.io/badge/BI-Apache%20Superset-red)
![Redis](https://img.shields.io/badge/Cache-Redis-DC382D)
![Prometheus](https://img.shields.io/badge/Metrics-Prometheus-E6522C)

---

## 🌟 INNOVATION FEATURES

> **Ce projet va au-delà du cahier des charges avec des fonctionnalités innovantes**

| Innovation | Technology | Description |
|------------|------------|-------------|
| 🔴 **Real-time Dashboards** | Grafana | Monitoring temps réel avec métriques Prometheus |
| 📊 **BI Analytics** | Apache Superset | Plateforme BI moderne avec visualisations interactives |
| ⚡ **Caching Layer** | Redis | Cache intelligent pour performances optimales |
| 📈 **Metrics Collection** | Prometheus | Collecte métriques pipeline, qualité, KPIs financiers |
| 🚨 **Alerting System** | Custom Python | Détection d'anomalies + alertes multi-canal |
| 🌐 **API Gateway** | Traefik | Routage intelligent, load balancing |
| 🔄 **EtLT Pattern** | DuckDB | Transformation IN-warehouse (plus moderne que ETL) |

---

## 📋 Project Overview

This project implements a **sectoral Data Space** for financial services (FAME - Finance & Embedded Finance), enabling:

- ✅ **Integration of 4 heterogeneous data sources** (API, XML, CSV, SQL)
- ✅ **Real-time streaming** with Apache Kafka
- ✅ **Data Lake (Bronze/Silver/Gold)** for raw data storage & processing
- ✅ **Data Fabric** for unified governance, metadata & data catalog
- ✅ **Data Warehouse (DuckDB)** for fast analytical queries & BI
- ✅ **Semantic interoperability** using RDF, SKOS, OWL, and SPARQL
- ✅ **Containerized deployment** with Docker Compose

---

## 📊 Data Sources (4 Heterogeneous Sources)

| # | Source | Type | Format | Frequency | Volume | Description |
|---|--------|------|--------|-----------|--------|-------------|
| 1 | **Stock Market API** | REST API | JSON | Real-time (5s) | ~17K/day | Alpha Vantage - Stocks, Crypto |
| 2 | **ECB Exchange Rates** | XML Feed | XML | Daily | ~30 pairs | European Central Bank official rates |
| 3 | **Company Financials** | File System | CSV | Quarterly | ~2K/year | Financial statements (15 companies) |
| 4 | **Transactions DB** | PostgreSQL | SQL | Real-time | ~100K/day | Banking transactions (CDC) |

### Source Details

#### Source 1: Stock Market API (Real-time JSON)
- **Provider**: Alpha Vantage / Yahoo Finance
- **Data**: Stock quotes, intraday prices, cryptocurrency rates
- **Symbols**: AAPL, MSFT, GOOGL, BNP.PA, SAN.MC, DB, HSBA.L
- **Kafka Topic**: `fame.market.stocks.quotes`

#### Source 2: ECB Exchange Rates (XML Feed)
- **Provider**: European Central Bank
- **Data**: Official EUR exchange rates for 30+ currencies
- **Update**: Daily at 16:00 CET
- **Kafka Topic**: `fame.forex.ecb.daily`

#### Source 3: Company Financials (CSV Batch)
- **Scope**: 15 European & US financial institutions
- **Data**: Quarterly income statements, balance sheets, ratios
- **Sectors**: Banking, Insurance, Payments, Fintech
- **Kafka Topic**: `fame.corporate.financials.quarterly`

#### Source 4: Financial Transactions (PostgreSQL)
- **Type**: Transactional database with CDC (Change Data Capture)
- **Data**: SEPA transfers, SWIFT payments, card transactions
- **Features**: Multi-currency, cross-border flagging, AML checks
- **Kafka Topic**: `fame.transactions.realtime`

---

## 🏗️ Architecture: Data Lake + Data Fabric + Data Warehouse

### Why This Architecture?

| Architecture | Pros | Cons | **FAME Fit** |
|--------------|------|------|--------------|
| **Data Lake** | Handles heterogeneous raw data, scalable, cost-effective | Can become "data swamp" without governance | ✅ Perfect for 4 formats |
| **Data Fabric** | Unified governance, automatic metadata, data catalog, lineage | Requires proper tooling | ✅ Essential for governance |
| **Data Warehouse** | Fast SQL queries, BI-ready, star schema | Less flexible for raw data | ✅ Perfect for analytics |
| Data Mesh | Domain ownership, decentralized | Complex for academic project | ❌ Overkill |

**Chosen: Data Lake + Data Fabric + Data Warehouse** - Optimal combination providing:
- **Storage flexibility** (Data Lake) for heterogeneous sources
- **Governance & Catalog** (Data Fabric) for metadata management
- **Fast Analytics** (Data Warehouse) for BI and reporting

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                      FAME DATA SPACE ARCHITECTURE v2.0                            │
│                   Data Lake + Data Fabric + Data Warehouse                        │
├──────────────────────────────────────────────────────────────────────────────────┤
│                                                                                   │
│  ┌─────────────────────────── DATA SOURCES ─────────────────────────────────────┐│
│  │  ┌───────────┐   ┌───────────┐   ┌───────────┐   ┌───────────┐              ││
│  │  │ SOURCE 1  │   │ SOURCE 2  │   │ SOURCE 3  │   │ SOURCE 4  │              ││
│  │  │ Stock API │   │ ECB XML   │   │ CSV Files │   │ PostgreSQL│              ││
│  │  │  (JSON)   │   │  (XML)    │   │   (CSV)   │   │   (SQL)   │              ││
│  │  └─────┬─────┘   └─────┬─────┘   └─────┬─────┘   └─────┬─────┘              ││
│  └────────┼───────────────┼───────────────┼───────────────┼─────────────────────┘│
│           │               │               │               │                      │
│           ▼               ▼               ▼               ▼                      │
│  ┌──────────────────────────────────────────────────────────────────────────────┐│
│  │                         APACHE KAFKA (Streaming)                             ││
│  │   fame.market.*    fame.forex.*    fame.corporate.*    fame.transactions.*   ││
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
│  │                        DATA LAKE (MinIO - S3)                                ││
│  │   ┌────────────┐     ┌────────────┐     ┌────────────┐                       ││
│  │   │   BRONZE   │ ──► │   SILVER   │ ──► │    GOLD    │                       ││
│  │   │  (Raw/JSON │     │ (Cleansed/ │     │  (Curated/ │                       ││
│  │   │  XML/CSV)  │     │  Parquet)  │     │   Parquet) │                       ││
│  │   └────────────┘     └────────────┘     └──────┬─────┘                       ││
│  └───────────────────────────────────────────────┼──────────────────────────────┘│
│                                                  │                               │
│           ┌──────────────────────────────────────┼───────────────────┐          │
│           │                                      │                   │          │
│           ▼                                      ▼                   ▼          │
│  ┌─────────────────┐              ┌─────────────────────┐   ┌────────────────┐  │
│  │  DATA WAREHOUSE │              │   SEMANTIC LAYER    │   │     SPARK      │  │
│  │    (DuckDB)     │              │                     │   │   Processing   │  │
│  │                 │              │  ┌───────────────┐  │   │                │  │
│  │ ┌─────────────┐ │              │  │ OWL Ontology  │  │   │  Batch & ML    │  │
│  │ │ DIM_COMPANY │ │              │  └───────────────┘  │   │                │  │
│  │ │ DIM_CURRENCY│ │              │  ┌───────────────┐  │   └────────────────┘  │
│  │ │ DIM_DATE    │ │              │  │SKOS Vocabulary│  │                       │
│  │ └─────────────┘ │              │  └───────────────┘  │                       │
│  │ ┌─────────────┐ │              │  ┌───────────────┐  │                       │
│  │ │ FACT_TRADES │ │              │  │  RDF Store    │  │                       │
│  │ │ FACT_FX     │ │              │  │  (Fuseki)     │  │                       │
│  │ │FACT_PAYMENTS│ │              │  └───────────────┘  │                       │
│  │ └─────────────┘ │              └──────────┬──────────┘                       │
│  └────────┬────────┘                         │                                  │
│           │                                  │                                  │
│           ▼                                  ▼                                  │
│  ┌──────────────────────────────────────────────────────────────────────────────┐│
│  │                           QUERY & VISUALIZATION                              ││
│  │     SQL Analytics (DuckDB)    │    SPARQL Endpoint    │   Streamlit UI      ││
│  └──────────────────────────────────────────────────────────────────────────────┘│
│                                                                                   │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### Architecture Components

| Component | Technology | Purpose |
|-----------|------------|----------|
| **Data Lake** | MinIO (S3) | Store raw & processed data in Bronze/Silver/Gold zones |
| **Data Fabric** | Custom Python | Metadata management, data catalog, lineage tracking |
| **Data Warehouse** | DuckDB | Fast OLAP queries, star schema, BI analytics |
| **Semantic Layer** | Fuseki + RDF | Ontology-based queries, semantic interoperability |
| **Streaming** | Apache Kafka | Real-time data ingestion from all sources |

---

## 🐳 Docker Infrastructure

### Services (17 Containers!)

| Service | Image | Port | Description |
|---------|-------|------|-------------|
| **Kafka** | confluentinc/cp-kafka:7.5 | 29092 | Message streaming |
| **Zookeeper** | confluentinc/cp-zookeeper:7.5 | 2181 | Kafka coordination |
| **Kafka UI** | provectuslabs/kafka-ui | 8080 | Kafka monitoring |
| **MinIO** | minio/minio | 9000, 9001 | S3-compatible Data Lake |
| **PostgreSQL** | postgres:15-alpine | 5432 | Transaction database |
| **Fuseki** | stain/jena-fuseki | 3030 | RDF Triple Store |
| **Spark** | bitnami/spark:3.5 | 8083 | Batch processing |
| **Dashboard** | Custom | 8501 | Streamlit UI |
|  **Grafana** | grafana/grafana | 3001 | Real-time dashboards |
|  **Superset** | apache/superset | 8088 | BI Analytics |
|  **Prometheus** | prom/prometheus | 9090 | Metrics collection |
|  **Redis** | redis:7-alpine | 6379 | Caching layer |
|  **Traefik** | traefik:v3.0 | 80, 8082 | API Gateway |

### Quick Start

```powershell
# 1. Start all services
docker-compose up -d

# 2. Verify services are running
docker-compose ps

# 3. Access UIs
# - Kafka UI:    http://localhost:8080
# - MinIO:       http://localhost:9001 (fame_admin / fame_secret_2024)
# - Fuseki:      http://localhost:3030 (admin / admin123)
# - Dashboard:   http://localhost:8501
# -  Grafana:  http://localhost:3001 (admin / fame_grafana_2024)
# -  Superset: http://localhost:8088 (admin / fame_admin_2024)
# -  Prometheus: http://localhost:9090
# -  Traefik:  http://localhost:8082
```

---

## 📁 Project Structure

```
FAME-DataSpace/
│
├── 📂 docker/
│   ├── Dockerfile.etl              # ETL service container
│   └── Dockerfile.dashboard        # Dashboard container
│
├── 📂 database/
│   └── init.sql                    # PostgreSQL initialization
│
├── 📂 sources/                     # Data Source Connectors
│   ├── source1_stock_api.py        # REST API → Kafka (JSON)
│   ├── source2_ecb_xml.py          # XML Feed → Kafka (XML)
│   ├── source3_financials_csv.py   # CSV Files → Kafka (CSV)
│   ├── source4_transactions_db.py  # PostgreSQL → Kafka (SQL)
│   └── kafka_streaming.py          # Kafka infrastructure
│
├── 📂 etl/
│   ├── extract.py                  # Data extraction
│   ├── transform.py                # Data transformation
│   ├── load.py                     # Data loading
│   └── main_pipeline.py            # ETL orchestration
│
├── 📂 semantic/
│   ├── fame_ontology.owl           # OWL ontology
│   ├── fame_vocabulary.skos        # SKOS vocabulary
│   ├── rdf_generator.py            # RDF generation
│   └── sparql_queries.py           # SPARQL queries
│
├── 📂 data/
│   ├── raw/                        # Raw zone (landing)
│   │   ├── api/                    # JSON from APIs
│   │   ├── xml/                    # XML from ECB
│   │   ├── csv/                    # CSV files
│   │   └── sql/                    # SQL exports
│   ├── processed/                  # Silver zone
│   └── rdf/                        # RDF serializations
│
├── 📂 prototype/
│   ├── app.py                      # Streamlit dashboard
│   └── visualizations.py           # Charts & graphs
│
├── docker-compose.yml              # Full infrastructure
├── requirements.txt                # Python dependencies
└── README.md                       # This file
```

---

## 🚀 Getting Started

### Prerequisites
- Docker Desktop
- Python 3.11+
- 8GB RAM minimum

### Installation

```powershell
# Clone and navigate
cd FAME-DataSpace

# Start Docker infrastructure
docker-compose up -d

# Install Python dependencies
pip install -r requirements.txt

# Generate sample data
python sources/source3_financials_csv.py
python sources/source4_transactions_db.py

# Run ETL pipeline
python etl/main_pipeline.py

# Start dashboard
streamlit run prototype/app.py
```

### Without Docker (Local Development)

```powershell
# Install dependencies
pip install -r requirements.txt

# Run sources individually (offline mode)
python sources/source1_stock_api.py
python sources/source2_ecb_xml.py
python sources/source3_financials_csv.py
python sources/source4_transactions_db.py
```

---

## 📚 Semantic Model (Ontology)

The FAME ontology provides semantic interoperability across all data sources:

### Domains & Concepts

| Domain | Concepts | Relations |
|--------|----------|-----------|
| **Market Data** | Stock, Exchange, Price, Volume | hasTicker, tradedOn, hasPrice |
| **Foreign Exchange** | Currency, ExchangeRate, CentralBank | convertTo, publishedBy |
| **Corporate Finance** | Company, FinancialStatement, Ratio | hasReport, belongsToSector |
| **Transactions** | Transaction, Account, Bank | hasSender, hasReceiver, processedBy |

### Semantic Problems Solved

| Problem | Example | Solution |
|---------|---------|----------|
| **Synonyms** | "Stock" vs "Equity" vs "Share" | SKOS `altLabel` |
| **Homonymes** | "Bank" (institution) vs "Bank" (river) | OWL class hierarchy |
| **Units** | USD vs EUR amounts | Normalized to EUR with `amount_eur` |
| **Identifiers** | ISIN, CUSIP, Ticker | `owl:sameAs` linking |

---

## 🔧 Kafka Topics (Data Mesh)

```
fame.market.stocks.quotes       # Real-time stock prices
fame.market.stocks.intraday     # Intraday time series
fame.market.crypto.rates        # Cryptocurrency rates
fame.forex.ecb.daily            # ECB daily rates
fame.forex.realtime             # Real-time FX
fame.corporate.financials       # Quarterly reports
fame.transactions.realtime      # Live transactions
fame.transactions.cdc           # Change Data Capture
fame.semantic.rdf.triples       # RDF data
fame.datalake.raw               # Raw zone events
fame.datalake.processed         # Processed data
```

---

## 📈 Sample Queries

### SPARQL - Cross-Domain Query
```sparql
PREFIX fame: <http://fame.eu/ontology#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?company ?stockPrice ?revenue ?transactionVolume
WHERE {
  ?company a fame:FinancialInstitution ;
           fame:hasTicker ?ticker ;
           fame:hasStockPrice ?stockPrice ;
           fame:hasRevenue ?revenue .
  
  ?tx a fame:Transaction ;
      fame:involvesBank ?company .
  
  GROUP BY ?company
}
```

---

## 👥 Authors
**Master M2 - Data Space Project 2026**

## 📄 License
Academic Project - All Rights Reserved
