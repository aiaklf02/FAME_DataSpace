# -*- coding: utf-8 -*-
"""
FAME Financial Data Space - Real-time Alerting System
=====================================================
INNOVATION: Proactive monitoring and alerting for financial data

Features:
- Anomaly detection in financial data
- Threshold-based alerts
- Kafka integration for real-time events
- Multi-channel notifications (Slack, Email, Webhook)
- Alert history and escalation
"""

import json
import logging
import asyncio
from typing import Any, Dict, List, Optional, Callable, Union
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from abc import ABC, abstractmethod
import hashlib
import os
import statistics

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AlertSeverity(Enum):
    """Alert severity levels"""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


class AlertStatus(Enum):
    """Alert lifecycle status"""
    ACTIVE = "active"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"
    SUPPRESSED = "suppressed"


@dataclass
class Alert:
    """Alert data structure"""
    id: str
    name: str
    message: str
    severity: AlertSeverity
    source: str
    timestamp: datetime
    status: AlertStatus = AlertStatus.ACTIVE
    value: Any = None
    threshold: Any = None
    metadata: Dict = field(default_factory=dict)
    acknowledged_by: Optional[str] = None
    resolved_at: Optional[datetime] = None
    
    def to_dict(self) -> Dict:
        return {
            'id': self.id,
            'name': self.name,
            'message': self.message,
            'severity': self.severity.value,
            'source': self.source,
            'timestamp': self.timestamp.isoformat(),
            'status': self.status.value,
            'value': self.value,
            'threshold': self.threshold,
            'metadata': self.metadata
        }


@dataclass
class AlertRule:
    """Alert rule configuration"""
    name: str
    description: str
    condition: Callable[[Any], bool]
    severity: AlertSeverity
    source: str
    cooldown_minutes: int = 5
    enabled: bool = True
    message_template: str = "Alert triggered: {name}"
    metadata: Dict = field(default_factory=dict)
    
    last_triggered: Optional[datetime] = None


# ═══════════════════════════════════════════════════════════════════════════
# Notification Channels
# ═══════════════════════════════════════════════════════════════════════════

class NotificationChannel(ABC):
    """Base class for notification channels"""
    
    @abstractmethod
    async def send(self, alert: Alert) -> bool:
        pass


class WebhookChannel(NotificationChannel):
    """Webhook notification channel"""
    
    def __init__(self, url: str, headers: Dict = None):
        self.url = url
        self.headers = headers or {'Content-Type': 'application/json'}
    
    async def send(self, alert: Alert) -> bool:
        try:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.url,
                    json=alert.to_dict(),
                    headers=self.headers
                ) as response:
                    return response.status == 200
        except Exception as e:
            logger.error(f"Webhook send failed: {e}")
            return False


class SlackChannel(NotificationChannel):
    """Slack notification channel"""
    
    SEVERITY_EMOJI = {
        AlertSeverity.INFO: "ℹ️",
        AlertSeverity.WARNING: "⚠️",
        AlertSeverity.CRITICAL: "🔴",
        AlertSeverity.EMERGENCY: "🚨"
    }
    
    def __init__(self, webhook_url: str, channel: str = None):
        self.webhook_url = webhook_url
        self.channel = channel
    
    async def send(self, alert: Alert) -> bool:
        emoji = self.SEVERITY_EMOJI.get(alert.severity, "📢")
        payload = {
            "text": f"{emoji} *{alert.name}*\n{alert.message}",
            "attachments": [{
                "color": self._severity_color(alert.severity),
                "fields": [
                    {"title": "Source", "value": alert.source, "short": True},
                    {"title": "Severity", "value": alert.severity.value.upper(), "short": True},
                    {"title": "Value", "value": str(alert.value), "short": True},
                    {"title": "Threshold", "value": str(alert.threshold), "short": True}
                ],
                "footer": f"FAME Data Space | {alert.timestamp.strftime('%Y-%m-%d %H:%M:%S')}"
            }]
        }
        
        if self.channel:
            payload["channel"] = self.channel
        
        try:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.post(self.webhook_url, json=payload) as response:
                    return response.status == 200
        except Exception as e:
            logger.error(f"Slack send failed: {e}")
            return False
    
    def _severity_color(self, severity: AlertSeverity) -> str:
        colors = {
            AlertSeverity.INFO: "#36a64f",
            AlertSeverity.WARNING: "#ff9800",
            AlertSeverity.CRITICAL: "#f44336",
            AlertSeverity.EMERGENCY: "#9c27b0"
        }
        return colors.get(severity, "#808080")


class ConsoleChannel(NotificationChannel):
    """Console/log notification channel"""
    
    async def send(self, alert: Alert) -> bool:
        icon = {
            AlertSeverity.INFO: "ℹ️",
            AlertSeverity.WARNING: "⚠️",
            AlertSeverity.CRITICAL: "🔴",
            AlertSeverity.EMERGENCY: "🚨"
        }.get(alert.severity, "📢")
        
        logger.info(f"""
{icon} ALERT: {alert.name}
   Message: {alert.message}
   Severity: {alert.severity.value}
   Source: {alert.source}
   Value: {alert.value} (threshold: {alert.threshold})
   Time: {alert.timestamp}
""")
        return True


# ═══════════════════════════════════════════════════════════════════════════
# Anomaly Detection (INNOVATION)
# ═══════════════════════════════════════════════════════════════════════════

class AnomalyDetector:
    """
    Statistical anomaly detection for financial data
    Uses multiple techniques:
    - Z-score
    - IQR (Interquartile Range)
    - Moving average deviation
    """
    
    def __init__(self, window_size: int = 100):
        self.window_size = window_size
        self.data_windows: Dict[str, List[float]] = {}
    
    def add_datapoint(self, metric: str, value: float):
        """Add a data point to the metric window"""
        if metric not in self.data_windows:
            self.data_windows[metric] = []
        
        self.data_windows[metric].append(value)
        
        # Keep only window_size elements
        if len(self.data_windows[metric]) > self.window_size:
            self.data_windows[metric] = self.data_windows[metric][-self.window_size:]
    
    def detect_zscore_anomaly(
        self,
        metric: str,
        value: float,
        threshold: float = 3.0
    ) -> Optional[Dict]:
        """Detect anomaly using Z-score method"""
        window = self.data_windows.get(metric, [])
        if len(window) < 10:
            return None
        
        mean = statistics.mean(window)
        stdev = statistics.stdev(window) if len(window) > 1 else 0
        
        if stdev == 0:
            return None
        
        z_score = (value - mean) / stdev
        
        if abs(z_score) > threshold:
            return {
                'method': 'z-score',
                'value': value,
                'z_score': z_score,
                'mean': mean,
                'stdev': stdev,
                'threshold': threshold,
                'is_anomaly': True
            }
        return None
    
    def detect_iqr_anomaly(
        self,
        metric: str,
        value: float,
        multiplier: float = 1.5
    ) -> Optional[Dict]:
        """Detect anomaly using IQR method"""
        window = self.data_windows.get(metric, [])
        if len(window) < 10:
            return None
        
        sorted_data = sorted(window)
        q1_idx = len(sorted_data) // 4
        q3_idx = 3 * len(sorted_data) // 4
        
        q1 = sorted_data[q1_idx]
        q3 = sorted_data[q3_idx]
        iqr = q3 - q1
        
        lower_bound = q1 - (multiplier * iqr)
        upper_bound = q3 + (multiplier * iqr)
        
        if value < lower_bound or value > upper_bound:
            return {
                'method': 'iqr',
                'value': value,
                'q1': q1,
                'q3': q3,
                'iqr': iqr,
                'lower_bound': lower_bound,
                'upper_bound': upper_bound,
                'is_anomaly': True
            }
        return None
    
    def detect_all(self, metric: str, value: float) -> List[Dict]:
        """Run all detection methods"""
        self.add_datapoint(metric, value)
        
        anomalies = []
        
        zscore_result = self.detect_zscore_anomaly(metric, value)
        if zscore_result:
            anomalies.append(zscore_result)
        
        iqr_result = self.detect_iqr_anomaly(metric, value)
        if iqr_result:
            anomalies.append(iqr_result)
        
        return anomalies


# ═══════════════════════════════════════════════════════════════════════════
# Alert Manager
# ═══════════════════════════════════════════════════════════════════════════

class FAMEAlertManager:
    """
    INNOVATION: Comprehensive alert management system for FAME Data Space
    
    Features:
    - Rule-based alerting
    - Anomaly detection
    - Multi-channel notifications
    - Alert deduplication
    - Cooldown periods
    - Alert history
    """
    
    def __init__(self):
        self.rules: Dict[str, AlertRule] = {}
        self.channels: List[NotificationChannel] = []
        self.alerts: Dict[str, Alert] = {}
        self.alert_history: List[Alert] = []
        self.anomaly_detector = AnomalyDetector()
        
        # Default console channel
        self.channels.append(ConsoleChannel())
        
        # Initialize default rules
        self._init_default_rules()
        
        logger.info("✅ Alert Manager initialized")
    
    def _init_default_rules(self):
        """Initialize default financial alerting rules"""
        
        # Transaction volume spike
        self.add_rule(AlertRule(
            name="high_transaction_volume",
            description="Transaction volume exceeds threshold",
            condition=lambda v: v > 10000,
            severity=AlertSeverity.WARNING,
            source="transactions",
            message_template="High transaction volume detected: {value} transactions"
        ))
        
        # Large transaction alert
        self.add_rule(AlertRule(
            name="large_transaction",
            description="Single transaction exceeds limit",
            condition=lambda v: v > 100000,  # EUR
            severity=AlertSeverity.CRITICAL,
            source="transactions",
            message_template="Large transaction detected: €{value}"
        ))
        
        # Forex rate volatility
        self.add_rule(AlertRule(
            name="forex_volatility",
            description="Forex rate change exceeds threshold",
            condition=lambda v: abs(v) > 0.05,  # 5% change
            severity=AlertSeverity.WARNING,
            source="forex",
            message_template="High forex volatility detected: {value:.2%} change"
        ))
        
        # Data pipeline failure
        self.add_rule(AlertRule(
            name="pipeline_failure",
            description="EtLT pipeline stage failed",
            condition=lambda v: v.get('status') == 'failed',
            severity=AlertSeverity.CRITICAL,
            source="pipeline",
            message_template="Pipeline {value[stage]} failed: {value[error]}"
        ))
        
        # Data freshness alert
        self.add_rule(AlertRule(
            name="data_staleness",
            description="Data not updated within threshold",
            condition=lambda v: v > 3600,  # seconds
            severity=AlertSeverity.WARNING,
            source="data_quality",
            message_template="Data stale for {value} seconds"
        ))
    
    def _generate_alert_id(self, rule: AlertRule, value: Any) -> str:
        """Generate unique alert ID"""
        data = f"{rule.name}:{rule.source}:{datetime.utcnow().date()}"
        return hashlib.md5(data.encode()).hexdigest()[:12]
    
    def add_rule(self, rule: AlertRule):
        """Add an alert rule"""
        self.rules[rule.name] = rule
        logger.info(f"📋 Rule added: {rule.name}")
    
    def remove_rule(self, name: str):
        """Remove an alert rule"""
        if name in self.rules:
            del self.rules[name]
    
    def add_channel(self, channel: NotificationChannel):
        """Add a notification channel"""
        self.channels.append(channel)
    
    async def evaluate(self, metric: str, value: Any, source: str = None) -> List[Alert]:
        """Evaluate all rules against a metric value"""
        triggered_alerts = []
        
        for rule in self.rules.values():
            if not rule.enabled:
                continue
            
            if source and rule.source != source:
                continue
            
            # Check cooldown
            if rule.last_triggered:
                cooldown_end = rule.last_triggered + timedelta(minutes=rule.cooldown_minutes)
                if datetime.utcnow() < cooldown_end:
                    continue
            
            # Evaluate condition
            try:
                if rule.condition(value):
                    alert = await self._trigger_alert(rule, value)
                    triggered_alerts.append(alert)
            except Exception as e:
                logger.error(f"Rule evaluation error ({rule.name}): {e}")
        
        # Also check for statistical anomalies
        if isinstance(value, (int, float)):
            anomalies = self.anomaly_detector.detect_all(metric, value)
            for anomaly in anomalies:
                alert = await self._trigger_anomaly_alert(metric, anomaly)
                triggered_alerts.append(alert)
        
        return triggered_alerts
    
    async def _trigger_alert(self, rule: AlertRule, value: Any) -> Alert:
        """Trigger an alert from a rule"""
        alert_id = self._generate_alert_id(rule, value)
        
        # Format message
        message = rule.message_template.format(
            name=rule.name,
            value=value,
            **rule.metadata
        )
        
        alert = Alert(
            id=alert_id,
            name=rule.name,
            message=message,
            severity=rule.severity,
            source=rule.source,
            timestamp=datetime.utcnow(),
            value=value,
            threshold=rule.metadata.get('threshold'),
            metadata=rule.metadata
        )
        
        # Update rule
        rule.last_triggered = datetime.utcnow()
        
        # Store alert
        self.alerts[alert_id] = alert
        self.alert_history.append(alert)
        
        # Send notifications
        await self._notify_all(alert)
        
        return alert
    
    async def _trigger_anomaly_alert(self, metric: str, anomaly: Dict) -> Alert:
        """Trigger an alert from anomaly detection"""
        alert_id = hashlib.md5(
            f"anomaly:{metric}:{datetime.utcnow().isoformat()}".encode()
        ).hexdigest()[:12]
        
        alert = Alert(
            id=alert_id,
            name=f"anomaly_{anomaly['method']}",
            message=f"Anomaly detected in {metric} using {anomaly['method']} method",
            severity=AlertSeverity.WARNING,
            source="anomaly_detection",
            timestamp=datetime.utcnow(),
            value=anomaly['value'],
            metadata=anomaly
        )
        
        self.alerts[alert_id] = alert
        self.alert_history.append(alert)
        
        await self._notify_all(alert)
        
        return alert
    
    async def _notify_all(self, alert: Alert):
        """Send alert to all notification channels"""
        for channel in self.channels:
            try:
                await channel.send(alert)
            except Exception as e:
                logger.error(f"Notification failed: {e}")
    
    def acknowledge(self, alert_id: str, user: str) -> bool:
        """Acknowledge an alert"""
        if alert_id in self.alerts:
            self.alerts[alert_id].status = AlertStatus.ACKNOWLEDGED
            self.alerts[alert_id].acknowledged_by = user
            logger.info(f"Alert {alert_id} acknowledged by {user}")
            return True
        return False
    
    def resolve(self, alert_id: str) -> bool:
        """Resolve an alert"""
        if alert_id in self.alerts:
            self.alerts[alert_id].status = AlertStatus.RESOLVED
            self.alerts[alert_id].resolved_at = datetime.utcnow()
            logger.info(f"Alert {alert_id} resolved")
            return True
        return False
    
    def get_active_alerts(self) -> List[Alert]:
        """Get all active alerts"""
        return [a for a in self.alerts.values() if a.status == AlertStatus.ACTIVE]
    
    def get_alert_summary(self) -> Dict:
        """Get alert statistics summary"""
        total = len(self.alert_history)
        by_severity = {}
        by_source = {}
        
        for alert in self.alert_history:
            severity = alert.severity.value
            by_severity[severity] = by_severity.get(severity, 0) + 1
            by_source[alert.source] = by_source.get(alert.source, 0) + 1
        
        return {
            'total_alerts': total,
            'active_alerts': len(self.get_active_alerts()),
            'by_severity': by_severity,
            'by_source': by_source,
            'rules_count': len(self.rules),
            'channels_count': len(self.channels)
        }


# ═══════════════════════════════════════════════════════════════════════════
# Kafka Integration
# ═══════════════════════════════════════════════════════════════════════════

class KafkaAlertConsumer:
    """
    Consume messages from Kafka and trigger alerts
    """
    
    def __init__(
        self,
        alert_manager: FAMEAlertManager,
        bootstrap_servers: str = None,
        topics: List[str] = None
    ):
        self.alert_manager = alert_manager
        self.bootstrap_servers = bootstrap_servers or os.environ.get(
            'KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092'
        )
        self.topics = topics or ['fame.transactions', 'fame.forex', 'fame.pipeline']
        self.consumer = None
    
    async def start(self):
        """Start consuming Kafka messages"""
        try:
            from aiokafka import AIOKafkaConsumer
            
            self.consumer = AIOKafkaConsumer(
                *self.topics,
                bootstrap_servers=self.bootstrap_servers,
                group_id='fame-alert-consumer',
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            
            await self.consumer.start()
            logger.info(f"📡 Kafka consumer started on topics: {self.topics}")
            
            async for message in self.consumer:
                await self._process_message(message)
                
        except Exception as e:
            logger.error(f"Kafka consumer error: {e}")
        finally:
            if self.consumer:
                await self.consumer.stop()
    
    async def _process_message(self, message):
        """Process a Kafka message and evaluate alerts"""
        topic = message.topic
        value = message.value
        
        # Determine metric and source
        if 'fame.transactions' in topic:
            await self.alert_manager.evaluate(
                'transaction_amount',
                value.get('amount', 0),
                source='transactions'
            )
        elif 'fame.forex' in topic:
            await self.alert_manager.evaluate(
                'forex_rate_change',
                value.get('change_percent', 0),
                source='forex'
            )
        elif 'fame.pipeline' in topic:
            await self.alert_manager.evaluate(
                'pipeline_status',
                value,
                source='pipeline'
            )


# ═══════════════════════════════════════════════════════════════════════════
# Usage Example
# ═══════════════════════════════════════════════════════════════════════════

async def main():
    """Example usage of the alert system"""
    
    # Initialize alert manager
    manager = FAMEAlertManager()
    
    # Add Slack channel (if configured)
    slack_webhook = os.environ.get('SLACK_WEBHOOK_URL')
    if slack_webhook:
        manager.add_channel(SlackChannel(slack_webhook, '#fame-alerts'))
    
    # Simulate some metrics
    print("\n📊 Simulating financial metrics...\n")
    
    # Normal transaction
    alerts = await manager.evaluate('transaction_amount', 5000, source='transactions')
    print(f"Normal transaction: {len(alerts)} alerts triggered")
    
    # Large transaction (should trigger alert)
    alerts = await manager.evaluate('transaction_amount', 150000, source='transactions')
    print(f"Large transaction: {len(alerts)} alerts triggered")
    
    # Forex volatility
    alerts = await manager.evaluate('forex_rate_change', 0.08, source='forex')
    print(f"Forex volatility: {len(alerts)} alerts triggered")
    
    # Add some data for anomaly detection
    import random
    for i in range(50):
        value = random.gauss(100, 10)  # Normal distribution
        await manager.evaluate('stock_price', value, source='market')
    
    # Anomaly (should trigger)
    alerts = await manager.evaluate('stock_price', 200, source='market')
    print(f"Stock anomaly: {len(alerts)} alerts triggered")
    
    # Get summary
    print("\n📈 Alert Summary:")
    summary = manager.get_alert_summary()
    for key, value in summary.items():
        print(f"   {key}: {value}")
    
    # Get active alerts
    print(f"\n🔔 Active Alerts: {len(manager.get_active_alerts())}")
    for alert in manager.get_active_alerts():
        print(f"   - [{alert.severity.value.upper()}] {alert.name}: {alert.message}")


if __name__ == "__main__":
    asyncio.run(main())
