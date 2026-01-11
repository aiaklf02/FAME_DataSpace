"""
FAME Data Space - Data Fabric Module
=====================================
Unified governance layer providing:
- Metadata Management
- Data Catalog
- Data Lineage
- Data Quality Rules
"""

import os
import json
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class DataAsset:
    """Represents a data asset in the catalog."""
    asset_id: str
    name: str
    description: str
    asset_type: str  # table, file, api, stream
    domain: str  # market, forex, corporate, payments
    layer: str  # bronze, silver, gold
    location: str
    format: str
    schema: Dict[str, str]
    owner: str
    created_at: str
    updated_at: str
    tags: List[str]
    quality_score: float = 0.0
    row_count: int = 0


@dataclass
class DataLineage:
    """Tracks data lineage (source → target)."""
    lineage_id: str
    source_asset: str
    target_asset: str
    transformation: str
    created_at: str


@dataclass
class QualityRule:
    """Data quality rule definition."""
    rule_id: str
    name: str
    description: str
    asset_id: str
    rule_type: str  # completeness, validity, uniqueness, accuracy
    expression: str
    threshold: float
    is_active: bool = True


class FAMEDataCatalog:
    """
    Data Catalog - Central registry of all data assets.
    
    Features:
    - Asset registration
    - Search and discovery
    - Schema management
    - Tag-based organization
    """
    
    def __init__(self, catalog_path: str = "data/fabric"):
        """Initialize data catalog."""
        self.catalog_path = catalog_path
        self.catalog_file = os.path.join(catalog_path, "catalog.json")
        self.assets: Dict[str, DataAsset] = {}
        
        os.makedirs(catalog_path, exist_ok=True)
        self._load_catalog()
    
    def _load_catalog(self):
        """Load existing catalog from disk."""
        if os.path.exists(self.catalog_file):
            with open(self.catalog_file, 'r') as f:
                data = json.load(f)
                for asset_data in data.get("assets", []):
                    asset = DataAsset(**asset_data)
                    self.assets[asset.asset_id] = asset
            logger.info(f"📚 Loaded {len(self.assets)} assets from catalog")
    
    def _save_catalog(self):
        """Save catalog to disk."""
        data = {
            "version": "1.0",
            "updated_at": datetime.now().isoformat(),
            "assets": [asdict(a) for a in self.assets.values()]
        }
        with open(self.catalog_file, 'w') as f:
            json.dump(data, f, indent=2)
    
    def register_asset(self, asset: DataAsset) -> str:
        """Register a new data asset."""
        self.assets[asset.asset_id] = asset
        self._save_catalog()
        logger.info(f"📝 Registered asset: {asset.name} ({asset.asset_id})")
        return asset.asset_id
    
    def get_asset(self, asset_id: str) -> Optional[DataAsset]:
        """Get asset by ID."""
        return self.assets.get(asset_id)
    
    def search(self, 
               query: str = None,
               domain: str = None,
               layer: str = None,
               tags: List[str] = None) -> List[DataAsset]:
        """Search assets by various criteria."""
        results = list(self.assets.values())
        
        if domain:
            results = [a for a in results if a.domain == domain]
        
        if layer:
            results = [a for a in results if a.layer == layer]
        
        if tags:
            results = [a for a in results if any(t in a.tags for t in tags)]
        
        if query:
            query_lower = query.lower()
            results = [a for a in results 
                      if query_lower in a.name.lower() 
                      or query_lower in a.description.lower()]
        
        return results
    
    def list_by_domain(self, domain: str) -> List[DataAsset]:
        """List all assets in a domain."""
        return [a for a in self.assets.values() if a.domain == domain]
    
    def list_by_layer(self, layer: str) -> List[DataAsset]:
        """List all assets in a layer."""
        return [a for a in self.assets.values() if a.layer == layer]
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get catalog statistics."""
        stats = {
            "total_assets": len(self.assets),
            "by_domain": {},
            "by_layer": {},
            "by_type": {}
        }
        
        for asset in self.assets.values():
            stats["by_domain"][asset.domain] = stats["by_domain"].get(asset.domain, 0) + 1
            stats["by_layer"][asset.layer] = stats["by_layer"].get(asset.layer, 0) + 1
            stats["by_type"][asset.asset_type] = stats["by_type"].get(asset.asset_type, 0) + 1
        
        return stats


class FAMEDataLineage:
    """
    Data Lineage Tracker - Track data flow and transformations.
    
    Features:
    - Source-to-target mapping
    - Transformation tracking
    - Impact analysis
    - Visual lineage graph
    """
    
    def __init__(self, lineage_path: str = "data/fabric"):
        """Initialize lineage tracker."""
        self.lineage_path = lineage_path
        self.lineage_file = os.path.join(lineage_path, "lineage.json")
        self.lineages: List[DataLineage] = []
        
        os.makedirs(lineage_path, exist_ok=True)
        self._load_lineage()
    
    def _load_lineage(self):
        """Load existing lineage from disk."""
        if os.path.exists(self.lineage_file):
            with open(self.lineage_file, 'r') as f:
                data = json.load(f)
                for lineage_data in data.get("lineages", []):
                    self.lineages.append(DataLineage(**lineage_data))
    
    def _save_lineage(self):
        """Save lineage to disk."""
        data = {
            "version": "1.0",
            "updated_at": datetime.now().isoformat(),
            "lineages": [asdict(l) for l in self.lineages]
        }
        with open(self.lineage_file, 'w') as f:
            json.dump(data, f, indent=2)
    
    def add_lineage(self, source: str, target: str, transformation: str) -> str:
        """Add a lineage record."""
        lineage = DataLineage(
            lineage_id=f"lin_{len(self.lineages) + 1:04d}",
            source_asset=source,
            target_asset=target,
            transformation=transformation,
            created_at=datetime.now().isoformat()
        )
        self.lineages.append(lineage)
        self._save_lineage()
        return lineage.lineage_id
    
    def get_upstream(self, asset_id: str) -> List[DataLineage]:
        """Get all upstream sources for an asset."""
        return [l for l in self.lineages if l.target_asset == asset_id]
    
    def get_downstream(self, asset_id: str) -> List[DataLineage]:
        """Get all downstream targets for an asset."""
        return [l for l in self.lineages if l.source_asset == asset_id]
    
    def get_full_lineage(self, asset_id: str) -> Dict[str, List[str]]:
        """Get full lineage (upstream and downstream) for an asset."""
        upstream = []
        downstream = []
        
        # Recursive upstream
        def find_upstream(aid):
            for l in self.lineages:
                if l.target_asset == aid:
                    upstream.append(l.source_asset)
                    find_upstream(l.source_asset)
        
        # Recursive downstream
        def find_downstream(aid):
            for l in self.lineages:
                if l.source_asset == aid:
                    downstream.append(l.target_asset)
                    find_downstream(l.target_asset)
        
        find_upstream(asset_id)
        find_downstream(asset_id)
        
        return {"upstream": upstream, "downstream": downstream}


class FAMEDataQuality:
    """
    Data Quality Manager - Define and execute quality rules.
    
    Features:
    - Rule definition
    - Automated checks
    - Quality scoring
    - Alert generation
    """
    
    def __init__(self, quality_path: str = "data/fabric"):
        """Initialize quality manager."""
        self.quality_path = quality_path
        self.rules_file = os.path.join(quality_path, "quality_rules.json")
        self.rules: Dict[str, QualityRule] = {}
        
        os.makedirs(quality_path, exist_ok=True)
        self._load_rules()
        self._init_default_rules()
    
    def _load_rules(self):
        """Load existing rules from disk."""
        if os.path.exists(self.rules_file):
            with open(self.rules_file, 'r') as f:
                data = json.load(f)
                for rule_data in data.get("rules", []):
                    rule = QualityRule(**rule_data)
                    self.rules[rule.rule_id] = rule
    
    def _save_rules(self):
        """Save rules to disk."""
        data = {
            "version": "1.0",
            "updated_at": datetime.now().isoformat(),
            "rules": [asdict(r) for r in self.rules.values()]
        }
        with open(self.rules_file, 'w') as f:
            json.dump(data, f, indent=2)
    
    def _init_default_rules(self):
        """Initialize default quality rules."""
        default_rules = [
            QualityRule(
                rule_id="qr_001",
                name="Stock Price Validity",
                description="Stock price must be greater than 0",
                asset_id="silver_stocks",
                rule_type="validity",
                expression="price > 0",
                threshold=100.0
            ),
            QualityRule(
                rule_id="qr_002",
                name="Transaction Amount Completeness",
                description="Transaction amount cannot be null",
                asset_id="silver_transactions",
                rule_type="completeness",
                expression="amount IS NOT NULL",
                threshold=100.0
            ),
            QualityRule(
                rule_id="qr_003",
                name="Currency Code Validity",
                description="Currency must be 3 characters",
                asset_id="silver_transactions",
                rule_type="validity",
                expression="LENGTH(currency) = 3",
                threshold=100.0
            ),
            QualityRule(
                rule_id="qr_004",
                name="Exchange Rate Validity",
                description="Exchange rate must be positive",
                asset_id="silver_forex",
                rule_type="validity",
                expression="rate > 0",
                threshold=100.0
            ),
            QualityRule(
                rule_id="qr_005",
                name="Company Ticker Uniqueness",
                description="Company tickers must be unique",
                asset_id="silver_financials",
                rule_type="uniqueness",
                expression="COUNT(DISTINCT ticker) = COUNT(*)",
                threshold=100.0
            )
        ]
        
        for rule in default_rules:
            if rule.rule_id not in self.rules:
                self.rules[rule.rule_id] = rule
        
        self._save_rules()
    
    def add_rule(self, rule: QualityRule) -> str:
        """Add a new quality rule."""
        self.rules[rule.rule_id] = rule
        self._save_rules()
        return rule.rule_id
    
    def execute_rules(self, conn) -> Dict[str, Any]:
        """Execute all active quality rules."""
        results = {
            "executed_at": datetime.now().isoformat(),
            "total_rules": len(self.rules),
            "passed": 0,
            "failed": 0,
            "results": []
        }
        
        for rule in self.rules.values():
            if not rule.is_active:
                continue
            
            try:
                # Build quality check query
                sql = f"""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN {rule.expression} THEN 1 ELSE 0 END) as passed
                FROM {rule.asset_id}
                """
                
                result = conn.execute(sql).fetchone()
                total, passed = result[0], result[1]
                
                if total > 0:
                    score = (passed / total) * 100
                else:
                    score = 100.0
                
                is_passed = score >= rule.threshold
                
                results["results"].append({
                    "rule_id": rule.rule_id,
                    "rule_name": rule.name,
                    "asset": rule.asset_id,
                    "score": round(score, 2),
                    "threshold": rule.threshold,
                    "passed": is_passed
                })
                
                if is_passed:
                    results["passed"] += 1
                else:
                    results["failed"] += 1
                    
            except Exception as e:
                results["results"].append({
                    "rule_id": rule.rule_id,
                    "rule_name": rule.name,
                    "error": str(e),
                    "passed": False
                })
                results["failed"] += 1
        
        return results


class FAMEDataFabric:
    """
    Main Data Fabric class - Unified governance layer.
    
    Integrates:
    - Data Catalog
    - Data Lineage
    - Data Quality
    """
    
    def __init__(self, fabric_path: str = "data/fabric"):
        """Initialize Data Fabric components."""
        self.catalog = FAMEDataCatalog(fabric_path)
        self.lineage = FAMEDataLineage(fabric_path)
        self.quality = FAMEDataQuality(fabric_path)
        
        logger.info("🔗 Data Fabric initialized")
    
    def register_elt_assets(self):
        """Register all ELT assets in the catalog."""
        timestamp = datetime.now().isoformat()
        
        # Bronze layer assets
        bronze_assets = [
            DataAsset(
                asset_id="bronze_stocks",
                name="Stock Market Data (Bronze)",
                description="Raw stock quotes from Alpha Vantage API",
                asset_type="file",
                domain="market",
                layer="bronze",
                location="data/bronze/api/stocks_*.json",
                format="json",
                schema={"symbol": "string", "price": "float", "volume": "int"},
                owner="market_team",
                created_at=timestamp,
                updated_at=timestamp,
                tags=["stocks", "api", "realtime"]
            ),
            DataAsset(
                asset_id="bronze_forex",
                name="ECB Exchange Rates (Bronze)",
                description="Raw exchange rates from ECB XML feed",
                asset_type="file",
                domain="forex",
                layer="bronze",
                location="data/bronze/xml/forex_*.json",
                format="json",
                schema={"base_currency": "string", "target_currency": "string", "rate": "float"},
                owner="forex_team",
                created_at=timestamp,
                updated_at=timestamp,
                tags=["forex", "ecb", "xml"]
            ),
            DataAsset(
                asset_id="bronze_financials",
                name="Company Financials (Bronze)",
                description="Raw financial statements from CSV files",
                asset_type="file",
                domain="corporate",
                layer="bronze",
                location="data/bronze/csv/financials_*.json",
                format="json",
                schema={"company_name": "string", "revenue_millions": "float"},
                owner="corporate_team",
                created_at=timestamp,
                updated_at=timestamp,
                tags=["financials", "csv", "quarterly"]
            ),
            DataAsset(
                asset_id="bronze_transactions",
                name="Financial Transactions (Bronze)",
                description="Raw transactions from PostgreSQL database",
                asset_type="file",
                domain="payments",
                layer="bronze",
                location="data/bronze/sql/transactions_*.json",
                format="json",
                schema={"transaction_id": "string", "amount": "float", "currency": "string"},
                owner="payments_team",
                created_at=timestamp,
                updated_at=timestamp,
                tags=["transactions", "sql", "realtime"]
            )
        ]
        
        # Silver layer assets
        silver_assets = [
            DataAsset(
                asset_id="silver_stocks",
                name="Stock Market Data (Silver)",
                description="Cleaned stock data with EUR conversion",
                asset_type="table",
                domain="market",
                layer="silver",
                location="data/warehouse/fame_warehouse.duckdb::silver_stocks",
                format="parquet",
                schema={"symbol": "string", "price_eur": "float", "trend": "string"},
                owner="data_engineering",
                created_at=timestamp,
                updated_at=timestamp,
                tags=["stocks", "cleaned", "eur"]
            ),
            DataAsset(
                asset_id="silver_forex",
                name="Exchange Rates (Silver)",
                description="Cleaned forex rates with inverse calculation",
                asset_type="table",
                domain="forex",
                layer="silver",
                location="data/warehouse/fame_warehouse.duckdb::silver_forex",
                format="parquet",
                schema={"currency_pair": "string", "rate": "float", "inverse_rate": "float"},
                owner="data_engineering",
                created_at=timestamp,
                updated_at=timestamp,
                tags=["forex", "cleaned"]
            ),
            DataAsset(
                asset_id="silver_financials",
                name="Company Financials (Silver)",
                description="Cleaned financial data with calculated ratios",
                asset_type="table",
                domain="corporate",
                layer="silver",
                location="data/warehouse/fame_warehouse.duckdb::silver_financials",
                format="parquet",
                schema={"ticker": "string", "revenue_millions": "float", "calculated_margin": "float"},
                owner="data_engineering",
                created_at=timestamp,
                updated_at=timestamp,
                tags=["financials", "cleaned", "ratios"]
            ),
            DataAsset(
                asset_id="silver_transactions",
                name="Financial Transactions (Silver)",
                description="Cleaned transactions with EUR amounts and risk flags",
                asset_type="table",
                domain="payments",
                layer="silver",
                location="data/warehouse/fame_warehouse.duckdb::silver_transactions",
                format="parquet",
                schema={"transaction_id": "string", "amount_eur": "float", "amount_risk_level": "string"},
                owner="data_engineering",
                created_at=timestamp,
                updated_at=timestamp,
                tags=["transactions", "cleaned", "eur", "risk"]
            )
        ]
        
        # Gold layer assets
        gold_assets = [
            DataAsset(
                asset_id="gold_daily_market",
                name="Daily Market Summary (Gold)",
                description="Aggregated daily market statistics",
                asset_type="table",
                domain="market",
                layer="gold",
                location="data/warehouse/fame_warehouse.duckdb::gold_daily_market",
                format="parquet",
                schema={"market_date": "date", "avg_price_eur": "float", "total_volume": "int"},
                owner="analytics_team",
                created_at=timestamp,
                updated_at=timestamp,
                tags=["aggregated", "daily", "kpi"]
            ),
            DataAsset(
                asset_id="gold_tx_summary",
                name="Transaction Summary (Gold)",
                description="Aggregated transaction statistics by country",
                asset_type="table",
                domain="payments",
                layer="gold",
                location="data/warehouse/fame_warehouse.duckdb::gold_tx_summary",
                format="parquet",
                schema={"tx_date": "date", "sender_country": "string", "total_volume_eur": "float"},
                owner="analytics_team",
                created_at=timestamp,
                updated_at=timestamp,
                tags=["aggregated", "payments", "kpi"]
            )
        ]
        
        # Register all assets
        for asset in bronze_assets + silver_assets + gold_assets:
            self.catalog.register_asset(asset)
        
        # Register lineage
        self.lineage.add_lineage("bronze_stocks", "silver_stocks", "ELT Transform: validation, EUR conversion")
        self.lineage.add_lineage("bronze_forex", "silver_forex", "ELT Transform: inverse rate calculation")
        self.lineage.add_lineage("bronze_financials", "silver_financials", "ELT Transform: ratio calculation")
        self.lineage.add_lineage("bronze_transactions", "silver_transactions", "ELT Transform: EUR conversion, risk flagging")
        self.lineage.add_lineage("silver_stocks", "gold_daily_market", "ELT Aggregate: daily market summary")
        self.lineage.add_lineage("silver_transactions", "gold_tx_summary", "ELT Aggregate: transaction summary")
        
        logger.info(f"📚 Registered {len(self.catalog.assets)} assets with lineage")
    
    def get_summary(self) -> Dict[str, Any]:
        """Get Data Fabric summary."""
        return {
            "catalog": self.catalog.get_statistics(),
            "lineage_count": len(self.lineage.lineages),
            "quality_rules": len(self.quality.rules)
        }


# CLI Test
if __name__ == "__main__":
    print("=" * 60)
    print("🔗 FAME Data Fabric")
    print("=" * 60)
    
    fabric = FAMEDataFabric()
    fabric.register_elt_assets()
    
    print("\n📊 Fabric Summary:")
    summary = fabric.get_summary()
    print(f"   Total Assets: {summary['catalog']['total_assets']}")
    print(f"   By Domain: {summary['catalog']['by_domain']}")
    print(f"   By Layer: {summary['catalog']['by_layer']}")
    print(f"   Lineage Records: {summary['lineage_count']}")
    print(f"   Quality Rules: {summary['quality_rules']}")
    
    print("\n🔍 Search Example (domain='payments'):")
    payments = fabric.catalog.search(domain="payments")
    for asset in payments:
        print(f"   • {asset.name} ({asset.layer})")
