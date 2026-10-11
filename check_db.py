import os
import psycopg

def check_database_and_datalake():
    print("======================================================================")
    print("   VERIFICATION DE L'ARCHITECTURE KAPPA (POSTGRESQL + DATA LAKE)")
    print("======================================================================")
    
    # 1. Verification PostgreSQL
    try:
        conn = psycopg.connect(
            host="localhost",
            port=5433,
            dbname="iot_db",
            user="postgres",
            password=os.getenv("POSTGRES_PASSWORD"),
            autocommit=True
        )
        cur = conn.cursor()
        
        cur.execute("SELECT count(*) FROM sensor_metrics;")
        total_metrics = cur.fetchone()[0]
        
        cur.execute("SELECT count(*) FROM sensor_alerts;")
        total_alerts = cur.fetchone()[0]
        
        cur.execute("SELECT count(*) FROM sensor_windowed_metrics;")
        total_windows = cur.fetchone()[0]
        
        print("\n--- STATISTIQUES POSTGRESQL (Serving DB) ---")
        print(f"Total des mesures brutes (sensor_metrics)        : {total_metrics}")
        print(f"Total des alertes (sensor_alerts)                 : {total_alerts}")
        print(f"Total des fenetres 5 min (sensor_windowed_metrics): {total_windows}")
        
        if total_alerts > 0:
            print("\n--- DERNIERES ALERTES DETECTEES (Regles & Machine Learning) ---")
            cur.execute("SELECT timestamp, sensor_id, alert_level, temperature, message FROM sensor_alerts ORDER BY id DESC LIMIT 3;")
            for row in cur.fetchall():
                print(f"[{row[0]}] {row[1]} | Niveau: {row[2]} | Temp: {row[3]} C | Message: {row[4]}")
                
        conn.close()
    except Exception as e:
        print(f"[PostgreSQL] Erreur de connexion: {e}")

    # 2. Verification Data Lake Parquet
    parquet_dir = "./data_lake/raw_parquet"
    print("\n--- STATISTIQUES DATA LAKE (Stockage Parquet) ---")
    if os.path.exists(parquet_dir):
        parquet_files = [f for f in os.listdir(parquet_dir) if f.endswith(".parquet")]
        print(f"Emplacement Data Lake : {os.path.abspath(parquet_dir)}")
        print(f"Fichiers Parquet genere : {len(parquet_files)} fichier(s)")
    else:
        print(f"Emplacement Data Lake : {os.path.abspath(parquet_dir)} (En attente du premier micro-batch)")

    print("======================================================================")

if __name__ == "__main__":
    check_database_and_datalake()
