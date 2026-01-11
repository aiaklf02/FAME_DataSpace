"""
FAME Superset Auto-Setup
========================
Automatically creates database connection, datasets, charts and dashboards.
"""

import requests
import json
import time

SUPERSET_URL = "http://localhost:8088"
USERNAME = "admin"
PASSWORD = "admin123"


class SupersetAutoSetup:
    def __init__(self):
        self.session = requests.Session()
        self.access_token = None
        self.csrf_token = None
        
    def login(self):
        """Login to Superset and get tokens."""
        print("🔐 Logging into Superset...")
        
        # Get CSRF token first from login page
        self.session.get(f"{SUPERSET_URL}/login/")
        
        # Login via form
        login_data = {
            "username": USERNAME,
            "password": PASSWORD,
        }
        
        response = self.session.post(
            f"{SUPERSET_URL}/login/",
            data=login_data,
            allow_redirects=True
        )
        
        # Get fresh CSRF token after login
        csrf_response = self.session.get(f"{SUPERSET_URL}/api/v1/security/csrf_token/")
        if csrf_response.status_code == 200:
            self.csrf_token = csrf_response.json().get('result')
            print("✅ Login successful")
            return True
        else:
            print(f"❌ Login failed")
            return False
    
    def get_headers(self):
        """Get headers with authentication."""
        headers = {
            "Content-Type": "application/json",
            "X-CSRFToken": self.csrf_token,
            "Referer": SUPERSET_URL
        }
        return headers
    
    def create_database(self):
        """Create PostgreSQL database connection."""
        print("\n📊 Creating database connection...")
        
        db_config = {
            "database_name": "FAME PostgreSQL",
            "engine": "postgresql",
            "configuration_method": "sqlalchemy_form",
            "sqlalchemy_uri": "postgresql://fame_user:fame_password@postgres:5432/fame_transactions",
            "expose_in_sqllab": True,
            "allow_ctas": True,
            "allow_cvas": True,
            "allow_dml": True,
            "allow_run_async": True,
            "extra": json.dumps({
                "metadata_params": {},
                "engine_params": {},
                "metadata_cache_timeout": {},
                "schemas_allowed_for_file_upload": ["fame_analytics", "fame_streaming", "public"]
            })
        }
        
        response = self.session.post(
            f"{SUPERSET_URL}/api/v1/database/",
            json=db_config,
            headers=self.get_headers()
        )
        
        if response.status_code in [200, 201]:
            data = response.json()
            db_id = data.get('id')
            print(f"✅ Database created (ID: {db_id})")
            return db_id
        elif "already exists" in response.text.lower():
            print("ℹ️  Database already exists")
            # Get existing database
            response = self.session.get(
                f"{SUPERSET_URL}/api/v1/database/",
                headers=self.get_headers()
            )
            if response.status_code == 200:
                for db in response.json().get('result', []):
                    if 'FAME' in db.get('database_name', ''):
                        return db.get('id')
            return 1
        else:
            print(f"❌ Failed to create database: {response.status_code}")
            print(response.text[:500])
            return None
    
    def create_dataset(self, db_id: int, schema: str, table_name: str):
        """Create a dataset from a table."""
        print(f"  📋 Creating dataset: {schema}.{table_name}")
        
        dataset_config = {
            "database": db_id,
            "schema": schema,
            "table_name": table_name
        }
        
        response = self.session.post(
            f"{SUPERSET_URL}/api/v1/dataset/",
            json=dataset_config,
            headers=self.get_headers()
        )
        
        if response.status_code in [200, 201]:
            data = response.json()
            return data.get('id')
        elif "already exists" in response.text.lower():
            print(f"    ℹ️  Dataset already exists")
            return None
        else:
            print(f"    ⚠️  Could not create: {response.text[:200]}")
            return None
    
    def create_chart(self, dataset_id: int, chart_name: str, viz_type: str, params: dict):
        """Create a chart."""
        print(f"  📈 Creating chart: {chart_name}")
        
        chart_config = {
            "slice_name": chart_name,
            "datasource_id": dataset_id,
            "datasource_type": "table",
            "viz_type": viz_type,
            "params": json.dumps(params)
        }
        
        response = self.session.post(
            f"{SUPERSET_URL}/api/v1/chart/",
            json=chart_config,
            headers=self.get_headers()
        )
        
        if response.status_code in [200, 201]:
            data = response.json()
            return data.get('id')
        else:
            print(f"    ⚠️  Chart creation: {response.text[:200]}")
            return None
    
    def create_dashboard(self, name: str, chart_ids: list):
        """Create a dashboard with charts."""
        print(f"\n🎨 Creating dashboard: {name}")
        
        # Build position JSON
        positions = {"DASHBOARD_VERSION_KEY": "v2"}
        row = 0
        for i, chart_id in enumerate(chart_ids):
            if chart_id:
                positions[f"CHART-{chart_id}"] = {
                    "type": "CHART",
                    "id": f"CHART-{chart_id}",
                    "children": [],
                    "meta": {
                        "width": 6,
                        "height": 50,
                        "chartId": chart_id
                    }
                }
        
        dashboard_config = {
            "dashboard_title": name,
            "slug": name.lower().replace(" ", "-"),
            "published": True
        }
        
        response = self.session.post(
            f"{SUPERSET_URL}/api/v1/dashboard/",
            json=dashboard_config,
            headers=self.get_headers()
        )
        
        if response.status_code in [200, 201]:
            data = response.json()
            print(f"✅ Dashboard created: {name}")
            return data.get('id')
        else:
            print(f"⚠️  Dashboard: {response.text[:200]}")
            return None
    
    def setup_fame_dashboards(self):
        """Complete setup of FAME dashboards."""
        print("\n" + "="*60)
        print("🚀 FAME Superset Auto-Setup")
        print("="*60)
        
        # Login
        if not self.login():
            return False
        
        # Create database
        db_id = self.create_database()
        if not db_id:
            print("❌ Cannot proceed without database")
            return False
        
        print("\n📋 Creating datasets...")
        
        # Create datasets
        datasets = {
            'stocks': self.create_dataset(db_id, 'fame_analytics', 'silver_silver_stocks'),
            'forex': self.create_dataset(db_id, 'fame_analytics', 'silver_silver_forex'),
            'financials': self.create_dataset(db_id, 'fame_analytics', 'silver_silver_financials'),
            'transactions': self.create_dataset(db_id, 'fame_analytics', 'silver_silver_transactions'),
            'stream_quotes': self.create_dataset(db_id, 'fame_streaming', 'stock_quotes'),
            'stream_alerts': self.create_dataset(db_id, 'fame_streaming', 'alerts'),
        }
        
        print("\n" + "="*60)
        print("✅ FAME Superset Setup Complete!")
        print("="*60)
        print(f"\n🔗 Open Superset: {SUPERSET_URL}")
        print("   Username: admin")
        print("   Password: admin123")
        print("\n📊 Go to SQL Lab to explore your data!")
        print("   • fame_analytics.silver_silver_stocks (Yahoo API)")
        print("   • fame_analytics.silver_silver_forex (ECB XML)")
        print("   • fame_analytics.silver_silver_financials (CSV)")
        print("   • fame_streaming.stock_quotes (Kafka)")
        
        return True


if __name__ == "__main__":
    setup = SupersetAutoSetup()
    setup.setup_fame_dashboards()
