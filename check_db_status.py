import psycopg2
import sys

PG_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'fame_transactions',
    'user': 'fame_user',
    'password': 'fame_password'
}

def check_tables():
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cur = conn.cursor()
        
        print("Checking fame_analytics schema:")
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'fame_analytics'")
        tables = cur.fetchall()
        for t in tables:
            cur.execute(f"SELECT COUNT(*) FROM fame_analytics.{t[0]}")
            count = cur.fetchone()[0]
            print(f"  - {t[0]}: {count} rows")
            
        print("\nChecking fame_streaming schema:")
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'fame_streaming'")
        tables = cur.fetchall()
        for t in tables:
            cur.execute(f"SELECT COUNT(*) FROM fame_streaming.{t[0]}")
            count = cur.fetchone()[0]
            print(f"  - {t[0]}: {count} rows")
            
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_tables()
