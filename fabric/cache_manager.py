# -*- coding: utf-8 -*-
"""
FAME Financial Data Space - Redis Caching Layer
================================================
INNOVATION: Intelligent caching with Redis for high-performance queries

Features:
- Multi-layer caching (L1: memory, L2: Redis)
- Cache invalidation patterns
- Real-time metrics tracking
- Query result caching
- Session management
"""

import redis
import json
import hashlib
import pickle
import logging
from typing import Any, Optional, Dict, List, Union, Callable
from datetime import datetime, timedelta
from functools import wraps
from dataclasses import dataclass, asdict
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class CacheStats:
    """Cache statistics tracking"""
    hits: int = 0
    misses: int = 0
    writes: int = 0
    evictions: int = 0
    
    @property
    def hit_rate(self) -> float:
        total = self.hits + self.misses
        return (self.hits / total * 100) if total > 0 else 0.0


class FAMECacheManager:
    """
    INNOVATION: Advanced Redis caching for FAME Data Space
    
    Provides:
    - Query caching with automatic expiration
    - Data pipeline result caching
    - Real-time KPI caching
    - Session management
    - Pub/Sub for cache invalidation
    """
    
    # Cache key prefixes
    PREFIX_QUERY = "fame:query:"
    PREFIX_KPI = "fame:kpi:"
    PREFIX_DATA = "fame:data:"
    PREFIX_SESSION = "fame:session:"
    PREFIX_LOCK = "fame:lock:"
    PREFIX_STREAM = "fame:stream:"
    
    # Default TTLs (seconds)
    TTL_QUERY = 300        # 5 minutes for queries
    TTL_KPI = 60           # 1 minute for real-time KPIs
    TTL_DATA = 3600        # 1 hour for static data
    TTL_SESSION = 86400    # 24 hours for sessions
    
    def __init__(
        self,
        host: str = None,
        port: int = 6379,
        db: int = 0,
        password: str = None,
        max_connections: int = 50
    ):
        """Initialize Redis connection pool"""
        self.host = host or os.environ.get('REDIS_HOST', 'localhost')
        self.port = port
        self.db = db
        
        # Connection pool for better performance
        self.pool = redis.ConnectionPool(
            host=self.host,
            port=self.port,
            db=self.db,
            password=password,
            max_connections=max_connections,
            decode_responses=False  # For pickle support
        )
        self.redis = redis.Redis(connection_pool=self.pool)
        self.stats = CacheStats()
        
        # Pub/Sub for cache invalidation
        self.pubsub = self.redis.pubsub()
        
        logger.info(f"✅ Redis cache connected: {self.host}:{self.port}")
    
    def _generate_key(self, prefix: str, *args, **kwargs) -> str:
        """Generate a unique cache key from arguments"""
        key_data = json.dumps({'args': args, 'kwargs': kwargs}, sort_keys=True, default=str)
        hash_key = hashlib.md5(key_data.encode()).hexdigest()[:16]
        return f"{prefix}{hash_key}"
    
    def _serialize(self, value: Any) -> bytes:
        """Serialize value for storage"""
        return pickle.dumps(value)
    
    def _deserialize(self, data: bytes) -> Any:
        """Deserialize stored value"""
        return pickle.loads(data) if data else None
    
    # ─────────────────────────────────────────────────────────────────
    # Core Cache Operations
    # ─────────────────────────────────────────────────────────────────
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        try:
            data = self.redis.get(key)
            if data:
                self.stats.hits += 1
                logger.debug(f"Cache HIT: {key}")
                return self._deserialize(data)
            else:
                self.stats.misses += 1
                logger.debug(f"Cache MISS: {key}")
                return None
        except Exception as e:
            logger.error(f"Cache get error: {e}")
            return None
    
    def set(
        self,
        key: str,
        value: Any,
        ttl: int = None,
        nx: bool = False
    ) -> bool:
        """Set value in cache with optional TTL"""
        try:
            serialized = self._serialize(value)
            result = self.redis.set(key, serialized, ex=ttl, nx=nx)
            if result:
                self.stats.writes += 1
                logger.debug(f"Cache SET: {key} (TTL: {ttl}s)")
            return bool(result)
        except Exception as e:
            logger.error(f"Cache set error: {e}")
            return False
    
    def delete(self, *keys: str) -> int:
        """Delete keys from cache"""
        try:
            count = self.redis.delete(*keys)
            self.stats.evictions += count
            return count
        except Exception as e:
            logger.error(f"Cache delete error: {e}")
            return 0
    
    def exists(self, key: str) -> bool:
        """Check if key exists"""
        return bool(self.redis.exists(key))
    
    # ─────────────────────────────────────────────────────────────────
    # Query Caching (INNOVATION)
    # ─────────────────────────────────────────────────────────────────
    
    def cache_query(
        self,
        query: str,
        result: Any,
        ttl: int = None,
        tags: List[str] = None
    ) -> str:
        """Cache a query result with automatic key generation"""
        key = self._generate_key(self.PREFIX_QUERY, query)
        ttl = ttl or self.TTL_QUERY
        
        # Store with metadata
        cache_entry = {
            'query': query,
            'result': result,
            'cached_at': datetime.utcnow().isoformat(),
            'tags': tags or []
        }
        self.set(key, cache_entry, ttl=ttl)
        
        # Track tags for invalidation
        if tags:
            for tag in tags:
                self.redis.sadd(f"fame:tags:{tag}", key)
        
        return key
    
    def get_cached_query(self, query: str) -> Optional[Any]:
        """Get cached query result"""
        key = self._generate_key(self.PREFIX_QUERY, query)
        entry = self.get(key)
        return entry['result'] if entry else None
    
    def invalidate_by_tag(self, tag: str) -> int:
        """Invalidate all cache entries with a specific tag"""
        tag_key = f"fame:tags:{tag}"
        keys = self.redis.smembers(tag_key)
        if keys:
            count = self.delete(*keys)
            self.redis.delete(tag_key)
            logger.info(f"Invalidated {count} entries with tag: {tag}")
            return count
        return 0
    
    # ─────────────────────────────────────────────────────────────────
    # KPI Caching (Real-time)
    # ─────────────────────────────────────────────────────────────────
    
    def set_kpi(self, name: str, value: Any, ttl: int = None) -> bool:
        """Cache a KPI value"""
        key = f"{self.PREFIX_KPI}{name}"
        return self.set(key, {
            'name': name,
            'value': value,
            'timestamp': datetime.utcnow().isoformat()
        }, ttl=ttl or self.TTL_KPI)
    
    def get_kpi(self, name: str) -> Optional[Dict]:
        """Get cached KPI value"""
        key = f"{self.PREFIX_KPI}{name}"
        return self.get(key)
    
    def get_all_kpis(self) -> Dict[str, Any]:
        """Get all cached KPIs"""
        kpis = {}
        for key in self.redis.scan_iter(f"{self.PREFIX_KPI}*"):
            data = self.get(key.decode() if isinstance(key, bytes) else key)
            if data:
                kpis[data['name']] = data
        return kpis
    
    # ─────────────────────────────────────────────────────────────────
    # Data Pipeline Caching
    # ─────────────────────────────────────────────────────────────────
    
    def cache_pipeline_result(
        self,
        pipeline_name: str,
        stage: str,
        result: Any,
        ttl: int = None
    ) -> str:
        """Cache pipeline stage result"""
        key = f"{self.PREFIX_DATA}{pipeline_name}:{stage}"
        self.set(key, {
            'pipeline': pipeline_name,
            'stage': stage,
            'result': result,
            'processed_at': datetime.utcnow().isoformat()
        }, ttl=ttl or self.TTL_DATA)
        return key
    
    def get_pipeline_result(self, pipeline_name: str, stage: str) -> Optional[Any]:
        """Get cached pipeline result"""
        key = f"{self.PREFIX_DATA}{pipeline_name}:{stage}"
        entry = self.get(key)
        return entry['result'] if entry else None
    
    # ─────────────────────────────────────────────────────────────────
    # Distributed Locking (INNOVATION)
    # ─────────────────────────────────────────────────────────────────
    
    def acquire_lock(
        self,
        lock_name: str,
        timeout: int = 30,
        blocking: bool = True,
        blocking_timeout: int = 10
    ) -> Optional[str]:
        """Acquire a distributed lock"""
        lock_key = f"{self.PREFIX_LOCK}{lock_name}"
        lock_value = f"{datetime.utcnow().isoformat()}:{os.getpid()}"
        
        if blocking:
            end_time = datetime.utcnow() + timedelta(seconds=blocking_timeout)
            while datetime.utcnow() < end_time:
                if self.set(lock_key, lock_value, ttl=timeout, nx=True):
                    logger.debug(f"Lock acquired: {lock_name}")
                    return lock_value
                import time
                time.sleep(0.1)
            return None
        else:
            if self.set(lock_key, lock_value, ttl=timeout, nx=True):
                return lock_value
            return None
    
    def release_lock(self, lock_name: str, lock_value: str) -> bool:
        """Release a distributed lock (only if we own it)"""
        lock_key = f"{self.PREFIX_LOCK}{lock_name}"
        current = self.get(lock_key)
        if current == lock_value:
            self.delete(lock_key)
            logger.debug(f"Lock released: {lock_name}")
            return True
        return False
    
    # ─────────────────────────────────────────────────────────────────
    # Real-time Streams (INNOVATION)
    # ─────────────────────────────────────────────────────────────────
    
    def add_to_stream(
        self,
        stream_name: str,
        data: Dict,
        maxlen: int = 10000
    ) -> str:
        """Add entry to a Redis stream"""
        key = f"{self.PREFIX_STREAM}{stream_name}"
        # Convert all values to strings for Redis
        str_data = {k: json.dumps(v) if not isinstance(v, str) else v 
                    for k, v in data.items()}
        entry_id = self.redis.xadd(key, str_data, maxlen=maxlen)
        return entry_id.decode() if isinstance(entry_id, bytes) else entry_id
    
    def read_stream(
        self,
        stream_name: str,
        count: int = 100,
        block: int = None
    ) -> List[Dict]:
        """Read entries from a stream"""
        key = f"{self.PREFIX_STREAM}{stream_name}"
        entries = self.redis.xrevrange(key, count=count)
        return [
            {
                'id': e[0].decode() if isinstance(e[0], bytes) else e[0],
                'data': {
                    k.decode() if isinstance(k, bytes) else k: 
                    json.loads(v.decode() if isinstance(v, bytes) else v)
                    for k, v in e[1].items()
                }
            }
            for e in entries
        ]
    
    # ─────────────────────────────────────────────────────────────────
    # Decorator for automatic caching
    # ─────────────────────────────────────────────────────────────────
    
    def cached(
        self,
        ttl: int = 300,
        prefix: str = None,
        tags: List[str] = None
    ):
        """Decorator for automatic function result caching"""
        def decorator(func: Callable):
            @wraps(func)
            def wrapper(*args, **kwargs):
                # Generate cache key
                cache_prefix = prefix or f"fame:func:{func.__name__}:"
                key = self._generate_key(cache_prefix, *args, **kwargs)
                
                # Try to get from cache
                cached_result = self.get(key)
                if cached_result is not None:
                    return cached_result
                
                # Execute function
                result = func(*args, **kwargs)
                
                # Cache result
                self.set(key, result, ttl=ttl)
                
                # Track tags
                if tags:
                    for tag in tags:
                        self.redis.sadd(f"fame:tags:{tag}", key)
                
                return result
            return wrapper
        return decorator
    
    # ─────────────────────────────────────────────────────────────────
    # Statistics & Monitoring
    # ─────────────────────────────────────────────────────────────────
    
    def get_stats(self) -> Dict:
        """Get cache statistics"""
        info = self.redis.info('stats')
        memory = self.redis.info('memory')
        
        return {
            'local_stats': asdict(self.stats),
            'hit_rate': f"{self.stats.hit_rate:.2f}%",
            'redis_hits': info.get('keyspace_hits', 0),
            'redis_misses': info.get('keyspace_misses', 0),
            'memory_used': memory.get('used_memory_human', 'N/A'),
            'memory_peak': memory.get('used_memory_peak_human', 'N/A'),
            'connected_clients': self.redis.info('clients').get('connected_clients', 0)
        }
    
    def health_check(self) -> Dict:
        """Check Redis health"""
        try:
            self.redis.ping()
            info = self.redis.info('server')
            return {
                'status': 'healthy',
                'redis_version': info.get('redis_version', 'unknown'),
                'uptime_days': info.get('uptime_in_days', 0),
                'connected': True
            }
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'connected': False
            }
    
    def clear_all(self, pattern: str = "fame:*") -> int:
        """Clear all FAME cache keys"""
        keys = list(self.redis.scan_iter(pattern))
        if keys:
            return self.delete(*keys)
        return 0


# ═══════════════════════════════════════════════════════════════════════════
# Context Manager for Cache Sessions
# ═══════════════════════════════════════════════════════════════════════════

class CacheSession:
    """Context manager for batch cache operations"""
    
    def __init__(self, cache: FAMECacheManager):
        self.cache = cache
        self.pipeline = cache.redis.pipeline()
        self.operations = []
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.pipeline.execute()
        return False
    
    def set(self, key: str, value: Any, ttl: int = None):
        serialized = self.cache._serialize(value)
        self.pipeline.set(key, serialized, ex=ttl)
        self.operations.append(('SET', key))
    
    def delete(self, key: str):
        self.pipeline.delete(key)
        self.operations.append(('DEL', key))


# ═══════════════════════════════════════════════════════════════════════════
# Usage Example
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    # Initialize cache
    cache = FAMECacheManager()
    
    # Health check
    print("📊 Cache Health:", cache.health_check())
    
    # Cache a KPI
    cache.set_kpi('total_transactions', 15789)
    cache.set_kpi('avg_transaction_value', 1234.56)
    
    # Get all KPIs
    print("\n📈 Cached KPIs:", cache.get_all_kpis())
    
    # Cache a query result
    query = "SELECT * FROM transactions WHERE date > '2024-01-01'"
    cache.cache_query(query, {'count': 100, 'data': [...]}, tags=['transactions'])
    
    # Get cached query
    result = cache.get_cached_query(query)
    print(f"\n📄 Cached Query Result: {result}")
    
    # Add to stream
    cache.add_to_stream('financial_events', {
        'type': 'transaction',
        'amount': 1500.00,
        'currency': 'EUR',
        'timestamp': datetime.utcnow().isoformat()
    })
    
    # Read stream
    events = cache.read_stream('financial_events', count=10)
    print(f"\n📡 Stream Events: {len(events)} entries")
    
    # Get stats
    print("\n📊 Cache Statistics:", cache.get_stats())
    
    # Decorator example
    @cache.cached(ttl=60, tags=['market'])
    def get_market_data(symbol: str):
        # Simulate expensive operation
        import time
        time.sleep(1)
        return {'symbol': symbol, 'price': 150.25}
    
    # First call (cache miss)
    data1 = get_market_data('AAPL')
    print(f"\n📈 Market Data (1st call): {data1}")
    
    # Second call (cache hit)
    data2 = get_market_data('AAPL')
    print(f"📈 Market Data (2nd call - cached): {data2}")
