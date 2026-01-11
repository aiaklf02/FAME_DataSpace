# -*- coding: utf-8 -*-
"""
FAME Financial Data Space - Apache Superset Configuration
========================================================
Custom configuration for Business Intelligence platform
"""

import os
from datetime import timedelta

# ---------------------------------------------------------
# Superset specific config
# ---------------------------------------------------------
ROW_LIMIT = 5000
SECRET_KEY = os.environ.get('SUPERSET_SECRET_KEY', 'fame_superset_secret_key_2024')

# Flask-WTF flag for CSRF
WTF_CSRF_ENABLED = True
WTF_CSRF_EXEMPT_LIST = []
WTF_CSRF_TIME_LIMIT = 60 * 60 * 24 * 365

# ---------------------------------------------------------
# Database config - Use PostgreSQL for metadata
# ---------------------------------------------------------
SQLALCHEMY_DATABASE_URI = os.environ.get(
    'DATABASE_URL',
    'postgresql://fame_user:fame_password@postgres:5432/fame_transactions'
)

# ---------------------------------------------------------
# Redis for caching (INNOVATION)
# ---------------------------------------------------------
CACHE_CONFIG = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_DEFAULT_TIMEOUT': 300,
    'CACHE_KEY_PREFIX': 'fame_superset_',
    'CACHE_REDIS_HOST': os.environ.get('REDIS_HOST', 'redis'),
    'CACHE_REDIS_PORT': 6379,
    'CACHE_REDIS_DB': 1,
}

DATA_CACHE_CONFIG = CACHE_CONFIG
FILTER_STATE_CACHE_CONFIG = CACHE_CONFIG
EXPLORE_FORM_DATA_CACHE_CONFIG = CACHE_CONFIG

# ---------------------------------------------------------
# Celery config (for async queries)
# ---------------------------------------------------------
class CeleryConfig:
    broker_url = 'redis://redis:6379/0'
    result_backend = 'redis://redis:6379/0'
    task_annotations = {
        'sql_lab.get_sql_results': {
            'rate_limit': '100/s',
        },
    }

CELERY_CONFIG = CeleryConfig

# ---------------------------------------------------------
# Feature flags (INNOVATION)
# ---------------------------------------------------------
FEATURE_FLAGS = {
    'ENABLE_TEMPLATE_PROCESSING': True,
    'DASHBOARD_NATIVE_FILTERS': True,
    'DASHBOARD_CROSS_FILTERS': True,
    'DASHBOARD_NATIVE_FILTERS_SET': True,
    'ALERT_REPORTS': True,
    'ESCAPE_MARKDOWN_HTML': True,
    'DASHBOARD_RBAC': True,
    'ENABLE_EXPLORE_DRAG_AND_DROP': True,
    'ENABLE_FILTER_BOX_MIGRATION': True,
    'DRILL_TO_DETAIL': True,
    'HORIZONTAL_FILTER_BAR': True,
}

# ---------------------------------------------------------
# CORS (for API access)
# ---------------------------------------------------------
ENABLE_CORS = True
CORS_OPTIONS = {
    'supports_credentials': True,
    'allow_headers': ['*'],
    'resources': ['*'],
    'origins': ['http://localhost:3000', 'http://localhost:8501', '*']
}

# ---------------------------------------------------------
# Logging
# ---------------------------------------------------------
ENABLE_TIME_ROTATE = True
TIME_ROTATE_LOG_LEVEL = 'INFO'

# ---------------------------------------------------------
# Custom branding
# ---------------------------------------------------------
APP_NAME = 'FAME Financial Data Space'
APP_ICON = '/static/assets/images/superset-logo-horiz.png'

# ---------------------------------------------------------
# Email (for alerts - INNOVATION)
# ---------------------------------------------------------
SMTP_HOST = 'localhost'
SMTP_STARTTLS = True
SMTP_SSL = False
SMTP_USER = ''
SMTP_PORT = 25
SMTP_PASSWORD = ''
SMTP_MAIL_FROM = 'fame-dataspace@example.com'

# ---------------------------------------------------------
# SQL Lab settings
# ---------------------------------------------------------
SQLLAB_TIMEOUT = 60
SQLLAB_DEFAULT_DBID = None
SQLLAB_ASYNC_TIME_LIMIT_SEC = 60 * 60 * 6

# ---------------------------------------------------------
# Database connections (pre-configured)
# ---------------------------------------------------------
SQLALCHEMY_CUSTOM_PASSWORD_STORE = None
SQLALCHEMY_TRACK_MODIFICATIONS = False

# Allow DuckDB connections
ADDITIONAL_ALLOWED_DATABASES = ['duckdb']

# ---------------------------------------------------------
# Public role access (for demo)
# ---------------------------------------------------------
PUBLIC_ROLE_LIKE_GAMMA = True

# ---------------------------------------------------------
# Thumbnails (async generation)
# ---------------------------------------------------------
FEATURE_FLAGS['THUMBNAILS'] = True
FEATURE_FLAGS['THUMBNAILS_SQLA_LISTENERS'] = True
THUMBNAIL_CACHE_CONFIG = CACHE_CONFIG
