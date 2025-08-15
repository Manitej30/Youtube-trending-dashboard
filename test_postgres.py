import psycopg2

try:
    conn = psycopg2.connect(
        host="host.docker.internal",  # <- use this instead of 127.0.0.1
        port=5432,
        dbname="airflow",
        user="airflow",
        password="airflow"
    )
    print("✅ Connected to Postgres!")
    conn.close()
except Exception as e:
    print("❌ Connection failed:", e)
