# 🏦 FAME Financial Data Space

> **Finance & Embedded Finance** - Plateforme de Partage de Données Financières Sécurisée

![Architecture](https://img.shields.io/badge/Architecture-Data%20Lake%20+%20Fabric%20+%20Warehouse-blue)
![Streaming](https://img.shields.io/badge/Streaming-Apache%20Kafka-orange)
![Semantic](https://img.shields.io/badge/Semantic-Protégé%20+%20Fuseki-green)
![Docker](https://img.shields.io/badge/Container-Docker%20Compose-2496ED)

---

## 📋 Table des Matières

1. [Vue d'ensemble](#-vue-densemble)
2. [Architecture](#-architecture)
3. [Technologies](#-technologies)
4. [Sources de Données](#-sources-de-données)
5. [Couche Sémantique](#-couche-sémantique-protégé--fuseki)
6. [Services Docker](#-services-docker)
7. [Installation & Commandes](#-installation--commandes)
8. [Structure du Projet](#-structure-du-projet)

---

## 🎯 Vue d'ensemble

Ce projet implémente un **Data Space sectoriel** pour les services financiers (FAME), intégrant :

| Fonctionnalité | Description |
|----------------|-------------|
| **4 Sources Hétérogènes** | API REST, XML, CSV, SQL |
| **Streaming Temps Réel** | Apache Kafka + Spark |
| **Data Lake Medallion** | Bronze → Silver → Gold |
| **Data Warehouse** | DuckDB + PostgreSQL |
| **Couche Sémantique** | Protégé (OWL) + Fuseki (SPARQL) |
| **Dashboards** | Grafana avec auto-provisioning |
| **12 Services Docker** | Infrastructure complète conteneurisée |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         FAME DATA SPACE ARCHITECTURE                         │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                            4 DATA SOURCES                                    │
├─────────────────┬─────────────────┬─────────────────┬───────────────────────┤
│  Yahoo Finance  │    ECB XML      │   CSV Files     │     PostgreSQL        │
│  (REST/JSON)    │  (Exchange Rates)│  (Financials)   │   (Transactions)      │
│   82 stocks     │   215K rates    │   22K records   │     759 tx            │
└────────┬────────┴────────┬────────┴────────┬────────┴──────────┬────────────┘
         │                 │                 │                   │
         ▼                 ▼                 ▼                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           APACHE KAFKA                                       │
│        fame-stocks │ fame-forex │ fame-financials │ fame-transactions       │
└─────────────────────────────────────┬───────────────────────────────────────┘
                                      │
         ┌────────────────────────────┼────────────────────────────┐
         ▼                            ▼                            ▼
┌─────────────────┐      ┌─────────────────────┐      ┌─────────────────────┐
│   DATA LAKE     │      │   SPARK STREAMING   │      │  SEMANTIC LAYER     │
│   (Medallion)   │      │                     │      │  Protégé + Fuseki   │
├─────────────────┤      │  • Parse JSON/XML   │      ├─────────────────────┤
│ 🥉 Bronze (Raw) │      │  • Anomaly Detect   │      │ • OWL Ontology      │
│ 🥈 Silver (Clean)│      │  • Enrichment       │      │ • RDF Instances     │
│ 🥇 Gold (Agg)   │      │                     │      │ • SKOS Vocabulary   │
└────────┬────────┘      └──────────┬──────────┘      └──────────┬──────────┘
         │                          │                            │
         ▼                          ▼                            ▼
┌─────────────────┐      ┌─────────────────────┐      ┌─────────────────────┐
│     DuckDB      │      │     PostgreSQL      │      │   Apache Fuseki     │
│   (Warehouse)   │      │   (Hot Storage)     │      │  (SPARQL Endpoint)  │
└────────┬────────┘      └──────────┬──────────┘      └──────────┬──────────┘
         │                          │                            │
         └──────────────────────────┼────────────────────────────┘
                                    ▼
                         ┌─────────────────────┐
                         │      GRAFANA        │
                         │    Dashboards       │
                         │  localhost:3000     │
                         └─────────────────────┘
```

---

## 🛠️ Technologies

### Stack Technique

| Catégorie | Technologie | Version | Port |
|-----------|-------------|---------|------|
| **Streaming** | Apache Kafka | 7.5.0 | 9092, 29092 |
| **Streaming** | Apache Zookeeper | 7.5.0 | 2181 |
| **Processing** | Apache Spark | 3.5.0 | 8081, 7077 |
| **Database** | PostgreSQL | 15-alpine | 5432 |
| **Warehouse** | DuckDB | Latest | - |
| **Cache** | Redis | 7-alpine | 6379 |
| **Semantic** | Apache Fuseki | Latest | 3030 |
| **Ontology** | Protégé | 5.x | - |
| **Dashboards** | Grafana | Latest | 3000 |
| **Metrics** | Prometheus | Latest | 9090 |
| **Kafka UI** | Provectus | Latest | 8080 |

### Standards Sémantiques

| Standard | Namespace | Usage |
|----------|-----------|-------|
| **RDF** | `http://www.w3.org/1999/02/22-rdf-syntax-ns#` | Modèle de données |
| **RDFS** | `http://www.w3.org/2000/01/rdf-schema#` | Schéma |
| **OWL** | `http://www.w3.org/2002/07/owl#` | Ontologie |
| **SKOS** | `http://www.w3.org/2004/02/skos/core#` | Vocabulaire |
| **FAME** | `http://fame.eu/ontology#` | Domaine métier |

---

## 📊 Sources de Données

### Résumé des 4 Sources

| # | Source | Format | Records | Kafka Topic |
|---|--------|--------|---------|-------------|
| 1 | **Yahoo Finance API** | JSON | 82 stocks | `fame-stocks` |
| 2 | **ECB Exchange Rates** | XML | 215,699 rates | `fame-forex` |
| 3 | **Company Financials** | CSV | 22,614 records | `fame-financials` |
| 4 | **Transactions DB** | SQL | 759 tx | `fame-transactions` |

### Source 1: Yahoo Finance (JSON)

```json
{
  "symbol": "AAPL",
  "price": 259.37,
  "change_percent": 0.13,
  "volume": 45123456,
  "market_cap": 4000000000000,
  "currency": "USD",
  "exchange": "NASDAQ",
  "timestamp": "2026-01-11T10:00:00"
}
```

**Stocks couverts:** AAPL, MSFT, GOOGL, AMZN, META, NVDA, TSLA, JPM, BAC, V, MA, BNP.PA, SAP.DE...

### Source 2: ECB XML (Taux de Change)

```xml
<Cube time="2026-01-10">
  <Cube currency="USD" rate="1.0825"/>
  <Cube currency="GBP" rate="0.83245"/>
  <Cube currency="JPY" rate="162.45"/>
</Cube>
```

**Devises:** USD, GBP, JPY, CHF, AUD, CAD, CNY, HKD + 21 autres

### Source 3: CSV (Financials)

| Fichier | Records | Contenu |
|---------|---------|---------|
| `sp500_companies.csv` | 503 | S&P 500 |
| `nasdaq_listings.csv` | 5,252 | NASDAQ |
| `nyse_listings.csv` | 2,880 | NYSE |
| `world_gdp.csv` | 13,979 | PIB mondial |

### Source 4: PostgreSQL (Transactions)

```sql
CREATE TABLE transactions (
  transaction_id UUID PRIMARY KEY,
  amount DECIMAL(15,2),
  currency VARCHAR(10),
  sender_id VARCHAR(50),
  receiver_id VARCHAR(50),
  status VARCHAR(20),      -- COMPLETED, PENDING, FAILED
  transaction_type VARCHAR(20), -- TRANSFER, PAYMENT, DEPOSIT
  timestamp TIMESTAMP
);
```

---

## 🔗 Couche Sémantique (Protégé + Fuseki)

### Architecture d'Intégration

```
┌─────────────────────────────────────────────────────────────────┐
│                 PROTÉGÉ + FUSEKI INTEGRATION                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌─────────────────┐     Export      ┌─────────────────┐        │
│  │    PROTÉGÉ      │ ──────────────▶ │  APACHE FUSEKI  │        │
│  │ (Design Tool)   │   OWL/RDF/SKOS  │ (Triple Store)  │        │
│  │                 │                  │                 │        │
│  │ • Visual Editor │                  │ • SPARQL API    │        │
│  │ • Reasoning     │                  │ • TDB2 Storage  │        │
│  │ • Validation    │                  │ • Inference     │        │
│  └─────────────────┘                  └────────┬────────┘        │
│                                                │                 │
│                                                ▼                 │
│                              ┌─────────────────────────────┐    │
│                              │       NAMED GRAPHS          │    │
│                              ├─────────────────────────────┤    │
│                              │ ontology  → Classes OWL     │    │
│                              │ data      → Instances RDF   │    │
│                              │ vocabulary → Concepts SKOS  │    │
│                              └─────────────────────────────┘    │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Fichiers Ontologie

| Fichier | Format | Standard | Triples |
|---------|--------|----------|---------|
| `fame_data_protege.owl` | RDF/XML | OWL 2.0 | ~500 |
| `FAME-RDF.rdf` | RDF/XML | RDF 1.1 | ~800 |
| `FAME-SKOS.ttl` | Turtle | SKOS 1.0 | ~300 |

### Hiérarchie des Classes (OWL)

```
fame:FinancialEntity
├── fame:Stock              (Action boursière)
├── fame:Currency           (Devise)
├── fame:ExchangeRate       (Taux de change)
├── fame:Company            (Entreprise)
├── fame:Sector             (Secteur d'activité)
├── fame:StockExchange      (Bourse)
└── fame:Transaction
    ├── fame:Transfer       (Virement)
    ├── fame:Payment        (Paiement)
    └── fame:Deposit        (Dépôt)
```

### Propriétés (Object & Datatype)

```
OBJECT PROPERTIES:
├── fame:hasCurrency        (Stock → Currency)
├── fame:tradedOn           (Stock → StockExchange)
├── fame:issuedBy           (Stock → Company)
├── fame:belongsToSector    (Company → Sector)
├── fame:baseCurrency       (ExchangeRate → Currency)
├── fame:targetCurrency     (ExchangeRate → Currency)
└── fame:transactionCurrency (Transaction → Currency)

DATATYPE PROPERTIES:
├── fame:symbol             (xsd:string)
├── fame:price              (xsd:decimal)
├── fame:volume             (xsd:integer)
├── fame:marketCap          (xsd:decimal)
├── fame:rate               (xsd:decimal)
├── fame:amount             (xsd:decimal)
└── fame:currencyCode       (xsd:string)
```

### Named Graphs (Fuseki)

| Graph URI | Contenu |
|-----------|---------|
| `http://fame.eu/graph/ontology` | TBox (Classes, Propriétés) |
| `http://fame.eu/graph/data` | ABox (Instances) |
| `http://fame.eu/graph/vocabulary` | SKOS Concepts |
| `http://fame.eu/graph/inferred` | Inférences RDFS |

### Exemple SPARQL

```sparql
PREFIX fame: <http://fame.eu/ontology#>
PREFIX fdata: <http://fame.eu/data#>

SELECT ?stock ?symbol ?price ?exchange
WHERE {
  ?stock a fame:Stock ;
         fame:symbol ?symbol ;
         fame:price ?price ;
         fame:tradedOn/rdfs:label ?exchange .
}
ORDER BY DESC(?price)
LIMIT 10
```

---

## 🐳 Services Docker

### Vue d'ensemble (12 Services)

```yaml
services:
  zookeeper:     # Kafka coordination
  kafka:         # Message streaming
  kafka-ui:      # Kafka monitoring
  spark-master:  # Spark master
  spark-worker:  # Spark worker
  postgres:      # Database
  redis:         # Cache
  fuseki:        # SPARQL endpoint
  grafana:       # Dashboards
  prometheus:    # Metrics
```

### Réseau & Volumes

```
Network: fame-dataspace-network (bridge)

Volumes:
├── fame-postgres-data    (PostgreSQL)
├── fame-fuseki-data      (Fuseki TDB2)
├── fame-redis-cache      (Redis)
├── fame-grafana-data     (Grafana)
└── fame-prometheus-data  (Prometheus)
```

---

## 🚀 Installation & Commandes

### Prérequis

- Docker Desktop
- Python 3.10+
- pip

### Commandes Complètes (A à Z)

```powershell
# ═══════════════════════════════════════════════════════════════
# ÉTAPE 1: SETUP
# ═══════════════════════════════════════════════════════════════
cd c:\Users\ayakh\MasterM2\Dataspace\FAME-DataSpace
pip install -r requirements.txt

# ═══════════════════════════════════════════════════════════════
# ÉTAPE 2: DÉMARRER DOCKER (12 services)
# ═══════════════════════════════════════════════════════════════
docker-compose up -d
docker-compose ps                    # Vérifier statut

# ═══════════════════════════════════════════════════════════════
# ÉTAPE 3: CHARGER COUCHE SÉMANTIQUE (Protégé → Fuseki)
# ═══════════════════════════════════════════════════════════════
.\start_semantic.ps1                 # Windows PowerShell
# ./start_semantic.sh                # Linux/Mac

# Ou directement avec Python:
python semantic/fuseki_loader.py --clear

# ═══════════════════════════════════════════════════════════════
# ÉTAPE 4: PIPELINE EtLT (Bronze → Silver → Gold)
# ═══════════════════════════════════════════════════════════════
python main.py pipeline

# ═══════════════════════════════════════════════════════════════
# ÉTAPE 5: STREAMING KAFKA
# ═══════════════════════════════════════════════════════════════
python streaming/kafka_finance_streaming.py --stock-interval 30

# ═══════════════════════════════════════════════════════════════
# ÉTAPE 6: ACCÈS AUX INTERFACES
# ═══════════════════════════════════════════════════════════════
# Grafana:    http://localhost:3000  (admin / admin123)
# Fuseki:     http://localhost:3030  (admin / admin123)
# Kafka UI:   http://localhost:8080
# Spark:      http://localhost:8081
# Prometheus: http://localhost:9090

# ═══════════════════════════════════════════════════════════════
# ÉTAPE 7: ARRÊT
# ═══════════════════════════════════════════════════════════════
docker-compose stop                  # Arrêter (garder données)
docker-compose down                  # Supprimer conteneurs
docker-compose down -v               # Supprimer tout
```

### Quick Start (Une commande)

```powershell
docker-compose up -d; Start-Sleep 30; .\start_semantic.ps1; python main.py pipeline
```

---

## 📁 Structure du Projet

```
FAME-DataSpace/
├── 📄 docker-compose.yml           # 12 services Docker
├── 📄 main.py                      # Point d'entrée principal
├── 📄 requirements.txt             # Dépendances Python
├── 📄 start_semantic.ps1           # Script démarrage Windows
├── 📄 start_semantic.sh            # Script démarrage Linux
│
├── 📁 data/
│   ├── 📁 bronze/                  # Données brutes
│   │   ├── 📁 api/                 # JSON Yahoo Finance
│   │   ├── 📁 csv/                 # Fichiers CSV
│   │   ├── 📁 xml/                 # ECB XML
│   │   └── 📁 sql/                 # Exports SQL
│   ├── 📁 silver/                  # Données nettoyées (Parquet)
│   ├── 📁 gold/                    # Données agrégées (Parquet)
│   ├── 📁 rdf/                     # Fichiers RDF
│   └── 📁 warehouse/
│       └── fame_warehouse.duckdb   # DuckDB
│
├── 📁 docker/
│   ├── 📁 fuseki/
│   │   └── config.ttl              # Configuration Fuseki
│   ├── 📁 grafana/
│   │   ├── 📁 dashboards/          # JSON dashboards
│   │   └── 📁 provisioning/        # Auto-provisioning
│   ├── 📁 postgres/
│   │   └── init.sql                # Initialisation DB
│   └── 📁 prometheus/
│       └── prometheus.yml          # Configuration metrics
│
├── 📁 elt/
│   ├── extract.py                  # Extraction des 4 sources
│   ├── transform.py                # Transformation Silver/Gold
│   ├── load.py                     # Chargement
│   ├── warehouse.py                # DuckDB warehouse
│   └── main_pipeline.py            # Orchestration EtLT
│
├── 📁 semantic/
│   ├── fame_data_protege.owl       # Ontologie OWL (Protégé)
│   ├── FAME-RDF.rdf                # Instances RDF
│   ├── FAME-SKOS.ttl               # Vocabulaire SKOS
│   ├── fuseki_loader.py            # Chargeur Fuseki
│   ├── fuseki_service.py           # Service CRUD/SPARQL
│   ├── grafana_queries.py          # Requêtes pour Grafana
│   └── sparql_queries.py           # Bibliothèque SPARQL
│
├── 📁 streaming/
│   ├── kafka_finance_streaming.py  # Streaming principal
│   ├── kafka_producer.py           # Producteur Kafka
│   ├── kafka_consumer.py           # Consommateur Kafka
│   └── spark_streaming.py          # Processing Spark
│
├── 📁 sources/
│   ├── source1_stock_api.py        # Yahoo Finance API
│   ├── source2_ecb_xml.py          # ECB XML Parser
│   ├── source3_financials_csv.py   # CSV Loader
│   └── source4_transactions_db.py  # PostgreSQL Connector
│
└── 📁 fabric/
    ├── data_fabric.py              # Data Fabric Layer
    ├── cache_manager.py            # Redis Cache
    └── metrics_service.py          # Prometheus Metrics
```

---

## 📈 Volumes de Données

| Couche | Table | Records |
|--------|-------|---------|
| **Bronze** | Raw (JSON/XML/CSV) | - |
| **Silver** | silver_stocks | 82 |
| **Silver** | silver_forex | 215,699 |
| **Silver** | silver_financials | 22,614 |
| **Silver** | silver_transactions | 759 |
| **Gold** | gold_daily_market | 10 |
| **Gold** | gold_tx_summary | 734 |
| **Semantic** | Fuseki triples | ~1,600 |

**Total: 240,000+ records + 1,600+ triples RDF**

---

## 🔧 Dépannage

```powershell
# Vérifier services Docker
docker-compose ps
docker-compose logs fuseki
docker-compose logs kafka

# Tester Fuseki
curl http://localhost:3030/$/ping

# Tester Kafka
docker exec fame-kafka kafka-topics --list --bootstrap-server localhost:9092

# Tester PostgreSQL
docker exec fame-postgres pg_isready -U fame_user

# Redémarrer un service
docker-compose restart fuseki
```

---

## 👥 Auteur

**Master M2 - Projet Data Space**

## 📜 Licence

Projet à but éducatif (Master M2).
