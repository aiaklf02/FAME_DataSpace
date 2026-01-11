# -*- coding: utf-8 -*-
"""
FAME Financial Data Space - Prometheus Metrics Service
======================================================
INNOVATION: Custom metrics collection and exposition for Grafana dashboards

Features:
- Pipeline metrics (extract, load, transform durations)
- Data quality metrics
- Financial KPIs
- System health metrics
- Custom business metrics
"""

from prometheus_client import (
    Counter, Gauge, Histogram, Summary, Info,
    CollectorRegistry, generate_latest,
    start_http_server, CONTENT_TYPE_LATEST
)
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass
from datetime import datetime
from functools import wraps
import threading
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FAMEMetrics:
    """
    INNOVATION: Centralized metrics collection for FAME Data Space
    
    Exposes metrics to Prometheus for Grafana visualization:
    - Pipeline performance
    - Data quality scores
    - Financial indicators
    - System resources
    """
    
    def __init__(self, registry: CollectorRegistry = None):
        self.registry = registry or CollectorRegistry()
        self._init_pipeline_metrics()
        self._init_data_quality_metrics()
        self._init_financial_metrics()
        self._init_system_metrics()
        
        logger.info("✅ FAME Metrics initialized")
    
    # ═══════════════════════════════════════════════════════════════════════════
    # Pipeline Metrics
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _init_pipeline_metrics(self):
        """Initialize EtLT pipeline metrics"""
        
        # Counters
        self.records_extracted = Counter(
            'fame_records_extracted_total',
            'Total records extracted from sources',
            ['source', 'source_type'],
            registry=self.registry
        )
        
        self.records_loaded = Counter(
            'fame_records_loaded_total',
            'Total records loaded to Data Lake',
            ['zone', 'source'],
            registry=self.registry
        )
        
        self.records_transformed = Counter(
            'fame_records_transformed_total',
            'Total records transformed',
            ['stage', 'table'],
            registry=self.registry
        )
        
        self.pipeline_runs = Counter(
            'fame_pipeline_runs_total',
            'Total pipeline executions',
            ['pipeline', 'status'],
            registry=self.registry
        )
        
        # Histograms for latency
        self.extract_duration = Histogram(
            'fame_extract_duration_seconds',
            'Time spent extracting data',
            ['source'],
            buckets=[0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0],
            registry=self.registry
        )
        
        self.load_duration = Histogram(
            'fame_load_duration_seconds',
            'Time spent loading data',
            ['zone'],
            buckets=[0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0],
            registry=self.registry
        )
        
        self.transform_duration = Histogram(
            'fame_transform_duration_seconds',
            'Time spent transforming data',
            ['stage'],
            buckets=[0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0],
            registry=self.registry
        )
        
        # Gauges for current state
        self.pipeline_status = Gauge(
            'fame_pipeline_status',
            'Current pipeline status (0=idle, 1=running, 2=failed)',
            ['pipeline'],
            registry=self.registry
        )
        
        self.last_pipeline_run = Gauge(
            'fame_last_pipeline_run_timestamp',
            'Timestamp of last pipeline run',
            ['pipeline'],
            registry=self.registry
        )
    
    # ═══════════════════════════════════════════════════════════════════════════
    # Data Quality Metrics
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _init_data_quality_metrics(self):
        """Initialize data quality metrics"""
        
        self.data_quality_score = Gauge(
            'fame_data_quality_score',
            'Data quality score (0-100)',
            ['dataset', 'dimension'],
            registry=self.registry
        )
        
        self.null_values = Gauge(
            'fame_null_values_count',
            'Number of null values in dataset',
            ['dataset', 'column'],
            registry=self.registry
        )
        
        self.duplicate_records = Gauge(
            'fame_duplicate_records_count',
            'Number of duplicate records',
            ['dataset'],
            registry=self.registry
        )
        
        self.schema_violations = Counter(
            'fame_schema_violations_total',
            'Total schema validation violations',
            ['dataset', 'rule'],
            registry=self.registry
        )
        
        self.data_freshness = Gauge(
            'fame_data_freshness_seconds',
            'Seconds since last data update',
            ['dataset'],
            registry=self.registry
        )
    
    # ═══════════════════════════════════════════════════════════════════════════
    # Financial Metrics
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _init_financial_metrics(self):
        """Initialize financial KPI metrics"""
        
        # Transaction metrics
        self.transaction_volume = Gauge(
            'fame_transaction_volume_eur',
            'Total transaction volume in EUR',
            ['type', 'period'],
            registry=self.registry
        )
        
        self.transaction_count = Gauge(
            'fame_transaction_count',
            'Number of transactions',
            ['type', 'status'],
            registry=self.registry
        )
        
        self.avg_transaction_value = Gauge(
            'fame_avg_transaction_value_eur',
            'Average transaction value in EUR',
            ['type'],
            registry=self.registry
        )
        
        # Market data metrics
        self.stock_price = Gauge(
            'fame_stock_price_eur',
            'Current stock price in EUR',
            ['symbol', 'exchange'],
            registry=self.registry
        )
        
        self.forex_rate = Gauge(
            'fame_forex_rate',
            'Foreign exchange rate',
            ['base_currency', 'target_currency'],
            registry=self.registry
        )
        
        self.market_volatility = Gauge(
            'fame_market_volatility',
            'Market volatility index',
            ['market'],
            registry=self.registry
        )
        
        # Company metrics
        self.company_count = Gauge(
            'fame_company_count',
            'Number of companies in database',
            ['sector', 'country'],
            registry=self.registry
        )
        
        self.portfolio_value = Gauge(
            'fame_portfolio_value_eur',
            'Total portfolio value in EUR',
            ['portfolio_id'],
            registry=self.registry
        )
    
    # ═══════════════════════════════════════════════════════════════════════════
    # System Metrics
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _init_system_metrics(self):
        """Initialize system health metrics"""
        
        self.service_info = Info(
            'fame_service',
            'FAME Data Space service information',
            registry=self.registry
        )
        
        self.cache_hits = Counter(
            'fame_cache_hits_total',
            'Total cache hits',
            ['cache_type'],
            registry=self.registry
        )
        
        self.cache_misses = Counter(
            'fame_cache_misses_total',
            'Total cache misses',
            ['cache_type'],
            registry=self.registry
        )
        
        self.active_connections = Gauge(
            'fame_active_connections',
            'Number of active connections',
            ['service'],
            registry=self.registry
        )
        
        self.data_lake_storage = Gauge(
            'fame_data_lake_storage_bytes',
            'Data Lake storage usage in bytes',
            ['zone'],
            registry=self.registry
        )
        
        self.warehouse_tables = Gauge(
            'fame_warehouse_tables_count',
            'Number of tables in data warehouse',
            ['schema'],
            registry=self.registry
        )
        
        self.rdf_triples = Gauge(
            'fame_rdf_triples_count',
            'Number of RDF triples in graph',
            ['graph'],
            registry=self.registry
        )
    
    # ═══════════════════════════════════════════════════════════════════════════
    # Decorator for automatic metric collection
    # ═══════════════════════════════════════════════════════════════════════════
    
    def track_pipeline_stage(self, stage: str, source: str = "default"):
        """Decorator to track pipeline stage execution"""
        def decorator(func: Callable):
            @wraps(func)
            def wrapper(*args, **kwargs):
                # Record start
                self.pipeline_status.labels(pipeline=stage).set(1)
                
                # Track duration
                histogram = {
                    'extract': self.extract_duration.labels(source=source),
                    'load': self.load_duration.labels(zone=source),
                    'transform': self.transform_duration.labels(stage=source)
                }.get(stage, self.transform_duration.labels(stage=source))
                
                start_time = time.time()
                try:
                    result = func(*args, **kwargs)
                    self.pipeline_runs.labels(pipeline=stage, status='success').inc()
                    return result
                except Exception as e:
                    self.pipeline_runs.labels(pipeline=stage, status='failed').inc()
                    self.pipeline_status.labels(pipeline=stage).set(2)
                    raise
                finally:
                    duration = time.time() - start_time
                    histogram.observe(duration)
                    self.pipeline_status.labels(pipeline=stage).set(0)
                    self.last_pipeline_run.labels(pipeline=stage).set(time.time())
                    
            return wrapper
        return decorator
    
    # ═══════════════════════════════════════════════════════════════════════════
    # Metric update methods
    # ═══════════════════════════════════════════════════════════════════════════
    
    def update_extraction_metrics(
        self,
        source: str,
        source_type: str,
        record_count: int,
        duration: float
    ):
        """Update extraction metrics after data extraction"""
        self.records_extracted.labels(
            source=source,
            source_type=source_type
        ).inc(record_count)
        self.extract_duration.labels(source=source).observe(duration)
    
    def update_load_metrics(
        self,
        zone: str,
        source: str,
        record_count: int,
        duration: float
    ):
        """Update load metrics after data loading"""
        self.records_loaded.labels(zone=zone, source=source).inc(record_count)
        self.load_duration.labels(zone=zone).observe(duration)
    
    def update_transform_metrics(
        self,
        stage: str,
        table: str,
        record_count: int,
        duration: float
    ):
        """Update transform metrics after transformation"""
        self.records_transformed.labels(stage=stage, table=table).inc(record_count)
        self.transform_duration.labels(stage=stage).observe(duration)
    
    def update_data_quality(
        self,
        dataset: str,
        scores: Dict[str, float],
        null_counts: Dict[str, int] = None,
        duplicate_count: int = 0
    ):
        """Update data quality metrics"""
        for dimension, score in scores.items():
            self.data_quality_score.labels(
                dataset=dataset,
                dimension=dimension
            ).set(score)
        
        if null_counts:
            for column, count in null_counts.items():
                self.null_values.labels(
                    dataset=dataset,
                    column=column
                ).set(count)
        
        self.duplicate_records.labels(dataset=dataset).set(duplicate_count)
    
    def update_financial_kpis(
        self,
        transaction_volume: float = None,
        transaction_count: int = None,
        avg_value: float = None,
        tx_type: str = "all"
    ):
        """Update financial KPI metrics"""
        if transaction_volume is not None:
            self.transaction_volume.labels(
                type=tx_type,
                period="current"
            ).set(transaction_volume)
        
        if transaction_count is not None:
            self.transaction_count.labels(
                type=tx_type,
                status="completed"
            ).set(transaction_count)
        
        if avg_value is not None:
            self.avg_transaction_value.labels(type=tx_type).set(avg_value)
    
    def update_market_data(
        self,
        symbol: str,
        price: float,
        exchange: str = "default"
    ):
        """Update market data metrics"""
        self.stock_price.labels(symbol=symbol, exchange=exchange).set(price)
    
    def update_forex_rate(
        self,
        base: str,
        target: str,
        rate: float
    ):
        """Update forex rate metric"""
        self.forex_rate.labels(
            base_currency=base,
            target_currency=target
        ).set(rate)
    
    def set_service_info(self, info: Dict[str, str]):
        """Set service information"""
        self.service_info.info(info)
    
    # ═══════════════════════════════════════════════════════════════════════════
    # Export methods
    # ═══════════════════════════════════════════════════════════════════════════
    
    def get_metrics(self) -> bytes:
        """Get metrics in Prometheus format"""
        return generate_latest(self.registry)
    
    def start_server(self, port: int = 8000):
        """Start HTTP server for Prometheus scraping"""
        start_http_server(port, registry=self.registry)
        logger.info(f"📊 Metrics server started on port {port}")


# ═══════════════════════════════════════════════════════════════════════════
# FastAPI Integration (optional)
# ═══════════════════════════════════════════════════════════════════════════

def create_metrics_endpoint(metrics: FAMEMetrics):
    """Create FastAPI metrics endpoint"""
    try:
        from fastapi import FastAPI, Response
        from fastapi.responses import PlainTextResponse
        
        app = FastAPI(title="FAME Metrics API")
        
        @app.get("/metrics")
        async def get_metrics():
            return Response(
                content=metrics.get_metrics(),
                media_type=CONTENT_TYPE_LATEST
            )
        
        @app.get("/health")
        async def health():
            return {"status": "healthy", "service": "fame-metrics"}
        
        return app
    except ImportError:
        logger.warning("FastAPI not installed, metrics endpoint not available")
        return None


# ═══════════════════════════════════════════════════════════════════════════
# Global metrics instance
# ═══════════════════════════════════════════════════════════════════════════

_metrics_instance: Optional[FAMEMetrics] = None

def get_metrics() -> FAMEMetrics:
    """Get or create global metrics instance"""
    global _metrics_instance
    if _metrics_instance is None:
        _metrics_instance = FAMEMetrics()
    return _metrics_instance


# ═══════════════════════════════════════════════════════════════════════════
# Usage Example
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    # Initialize metrics
    metrics = FAMEMetrics()
    
    # Set service info
    metrics.set_service_info({
        'version': '1.0.0',
        'environment': 'development',
        'architecture': 'data-lake-fabric-warehouse'
    })
    
    # Simulate pipeline metrics
    print("📊 Simulating pipeline metrics...\n")
    
    # Extract metrics
    metrics.update_extraction_metrics(
        source='api_stocks',
        source_type='json',
        record_count=1500,
        duration=2.5
    )
    
    metrics.update_extraction_metrics(
        source='ecb_forex',
        source_type='xml',
        record_count=180,
        duration=1.2
    )
    
    # Load metrics
    metrics.update_load_metrics(
        zone='bronze',
        source='api_stocks',
        record_count=1500,
        duration=0.8
    )
    
    # Transform metrics
    metrics.update_transform_metrics(
        stage='silver',
        table='silver_stocks',
        record_count=1480,
        duration=3.5
    )
    
    # Financial KPIs
    metrics.update_financial_kpis(
        transaction_volume=1_500_000.00,
        transaction_count=15789,
        avg_value=95.00
    )
    
    # Market data
    metrics.update_market_data('AAPL', 185.50, 'NASDAQ')
    metrics.update_market_data('MSFT', 378.20, 'NASDAQ')
    metrics.update_forex_rate('EUR', 'USD', 1.0850)
    
    # Data quality
    metrics.update_data_quality(
        dataset='transactions',
        scores={
            'completeness': 98.5,
            'accuracy': 99.2,
            'consistency': 97.8,
            'timeliness': 95.0
        },
        null_counts={'amount': 15, 'currency': 0},
        duplicate_count=23
    )
    
    # Print sample metrics
    print("📈 Sample Metrics Output:")
    print("-" * 50)
    output = metrics.get_metrics().decode('utf-8')
    # Print first 50 lines
    for line in output.split('\n')[:50]:
        if line and not line.startswith('#'):
            print(line)
    
    print("\n✅ Metrics ready for Prometheus scraping")
    
    # Optionally start server
    # metrics.start_server(port=8000)
