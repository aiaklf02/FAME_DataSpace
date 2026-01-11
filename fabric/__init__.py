"""
FAME Data Space - Data Fabric Package
======================================
Unified governance layer for the FAME Data Space.

INNOVATION MODULES:
- cache_manager: Redis caching with intelligent invalidation
- alert_system: Real-time alerting with anomaly detection
- metrics_service: Prometheus metrics for Grafana dashboards
"""

from .data_fabric import (
    FAMEDataFabric,
    FAMEDataCatalog,
    FAMEDataLineage,
    FAMEDataQuality,
    DataAsset,
    DataLineage,
    QualityRule
)

from .cache_manager import (
    FAMECacheManager,
    CacheSession,
    CacheStats
)

from .alert_system import (
    FAMEAlertManager,
    Alert,
    AlertRule,
    AlertSeverity,
    AlertStatus,
    AnomalyDetector,
    NotificationChannel,
    SlackChannel,
    WebhookChannel,
    ConsoleChannel
)

from .metrics_service import (
    FAMEMetrics,
    get_metrics
)

__all__ = [
    # Data Fabric Core
    'FAMEDataFabric',
    'FAMEDataCatalog',
    'FAMEDataLineage',
    'FAMEDataQuality',
    'DataAsset',
    'DataLineage',
    'QualityRule',
    
    # Caching (INNOVATION)
    'FAMECacheManager',
    'CacheSession',
    'CacheStats',
    
    # Alerting (INNOVATION)
    'FAMEAlertManager',
    'Alert',
    'AlertRule',
    'AlertSeverity',
    'AlertStatus',
    'AnomalyDetector',
    'NotificationChannel',
    'SlackChannel',
    'WebhookChannel',
    'ConsoleChannel',
    
    # Metrics (INNOVATION)
    'FAMEMetrics',
    'get_metrics'
]
