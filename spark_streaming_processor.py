import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, min as spark_min, max as spark_max,
    stddev, count, when, concat, lit, round as spark_round, udf
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, BooleanType

# Import du module Machine Learning (IsolationForest)
try:
    from ml_detector import check_ml_anomaly
    ML_AVAILABLE = True
except Exception:
    ML_AVAILABLE = False

# Fonction UDF PySpark pour predire l'anomalie avec le modele Machine Learning
@udf(returnType=BooleanType())
def predict_ml_anomaly_udf(temp, humid, press):
    if temp is None or humid is None or press is None:
        return False
    try:
        is_anomaly, _ = check_ml_anomaly(float(temp), float(humid), float(press))
        return bool(is_anomaly)
    except Exception:
        return float(temp) > 40.0

# Configuration PostgreSQL JDBC
DB_URL = "jdbc:postgresql://localhost:5433/iot_db"
DB_PROPERTIES = {
    "user": "postgres",
    "password": os.getenv("POSTGRES_PASSWORD"),
    "driver": "org.postgresql.Driver"
}

# Configuration Data Lake Parquet local
DATA_LAKE_PATH = "./data_lake"

def main():
    print("======================================================================")
    print("  APACHE SPARK STRUCTURED STREAMING & KAPPA ARCHITECTURE ENGINE")
    print("======================================================================")
    print("  - Topic Kafka source : iot-sensor-data")
    print("  - Watermark : 10 minutes")
    print("  - Fenetre d'agregation : 5 minutes (glissement 1 minute)")
    print("  - Detection 2 Niveaux : Regles Metier (40C/60C) + Machine Learning UDF")
    print("  - Stockage Multi-Destination : PostgreSQL (Serving DB) + Parquet (Data Lake)")
    print("======================================================================")
    sys.stdout.flush()

    # Initialisation de la Session Spark
    spark = SparkSession.builder \
        .appName("IoTKappaSparkStreamingEngine") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0") \
        .config("spark.sql.shuffle.partitions", "4") \
        .master("local[*]") \
        .getOrCreate()

    # 1. Schema JSON explicite
    schema = StructType([
        StructField("sensor_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("pressure", DoubleType(), True),
        StructField("status", StringType(), True)
    ])

    # 2. Ingestion du flux Kafka
    try:
        kafka_stream_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "iot-sensor-data") \
            .option("startingOffsets", "latest") \
            .load()
    except Exception as e:
        print(f"[ERREUR KAFKA] Impossible de lire le topic Kafka: {e}", file=sys.stderr)
        return

    # 3. Parsing du JSON et Conversion de types
    parsed_df = kafka_stream_df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json("json_str", schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", col("timestamp").cast(TimestampType()))

    # 4. Data Cleaning (Nettoyage des donnees aberrantes)
    cleaned_df = parsed_df.filter(
        (col("temperature") >= -50.0) & (col("temperature") <= 150.0) &
        (col("humidity") >= 0.0) & (col("humidity") <= 100.0) &
        (col("pressure") > 0.0) & (col("sensor_id").isNotNull())
    ).withColumn("is_ml_anomaly", predict_ml_anomaly_udf(col("temperature"), col("humidity"), col("pressure")))

    # 5. Traitement Batch Multi-Destination (PostgreSQL + Data Lake Parquet)
    def process_micro_batch(batch_df, batch_id):
        if batch_df.count() == 0:
            return
            
        batch_df.cache()
        
        # A. Sauvegarde dans PostgreSQL (Serving DB)
        try:
            metrics_df = batch_df.select(
                col("timestamp"),
                col("sensor_id"),
                col("temperature"),
                col("humidity"),
                col("pressure"),
                col("status")
            )
            metrics_df.write \
                .format("jdbc") \
                .option("url", DB_URL) \
                .option("dbtable", "sensor_metrics") \
                .option("user", DB_PROPERTIES["user"]) \
                .option("password", DB_PROPERTIES["password"]) \
                .option("driver", DB_PROPERTIES["driver"]) \
                .mode("append") \
                .save()
            print(f"[Spark -> Postgres] {metrics_df.count()} mesures ecrites (Batch {batch_id})")
        except Exception as e:
            print(f"[Erreur Postgres] {e}", file=sys.stderr)

        # B. Sauvegarde dans le Data Lake Parquet (/data_lake/raw_parquet)
        try:
            parquet_path = os.path.join(DATA_LAKE_PATH, "raw_parquet")
            metrics_df.write \
                .mode("append") \
                .parquet(parquet_path)
            print(f"[Spark -> Data Lake] Sauvegarde Parquet effectuee dans {parquet_path}")
        except Exception as e:
            print(f"[Erreur Data Lake Parquet] {e}", file=sys.stderr)

        # C. Detection des Alertes (Niveau 1: Regles Metier, Niveau 2: Machine Learning)
        try:
            alerts_df = batch_df.filter(
                (col("temperature") > 40.0) | (col("is_ml_anomaly") == True) | (col("status") == "ERROR")
            )
            if alerts_df.count() > 0:
                formatted_alerts = alerts_df.select(
                    col("timestamp"),
                    col("sensor_id"),
                    col("temperature"),
                    when(col("temperature") > 60.0, lit("CRITICAL"))
                    .when(col("temperature") > 40.0, lit("WARNING"))
                    .when(col("is_ml_anomaly") == True, lit("ML_ANOMALY"))
                    .otherwise(lit("ERROR")).alias("alert_level"),
                    
                    when(col("temperature") > 60.0, concat(lit("CRITIQUE: Surchauffe extreme sur "), col("sensor_id"), lit(" ("), col("temperature"), lit(" C)")))
                    .when(col("temperature") > 40.0, concat(lit("AVERTISSEMENT: Temperature elevated sur "), col("sensor_id"), lit(" ("), col("temperature"), lit(" C)")))
                    .when(col("is_ml_anomaly") == True, concat(lit("ALERTE ML (IsolationForest): Comportement anormal sur "), col("sensor_id"), lit(" ("), col("temperature"), lit(" C)")))
                    .otherwise(lit("ERREUR: Capteur en panne")).alias("message")
                )
                formatted_alerts.write \
                    .format("jdbc") \
                    .option("url", DB_URL) \
                    .option("dbtable", "sensor_alerts") \
                    .option("user", DB_PROPERTIES["user"]) \
                    .option("password", DB_PROPERTIES["password"]) \
                    .option("driver", DB_PROPERTIES["driver"]) \
                    .mode("append") \
                    .save()
                print(f"[Spark ML Alertes] {formatted_alerts.count()} alertes enregistrees dans Postgres (Batch {batch_id})")
        except Exception as e:
            print(f"[Erreur Alertes] {e}", file=sys.stderr)

        batch_df.unpersist()

    raw_query = cleaned_df.writeStream \
        .foreachBatch(process_micro_batch) \
        .start()

    # 6. Agregation Temporelle avec Watermark (5 minutes, glissement 1 minute)
    windowed_df = cleaned_df \
        .withWatermark("timestamp", "10 minutes") \
        .groupBy(
            window(col("timestamp"), "5 minutes", "1 minute"),
            col("sensor_id")
        ) \
        .agg(
            spark_round(avg("temperature"), 2).alias("average_temp"),
            spark_round(spark_min("temperature"), 2).alias("min_temp"),
            spark_round(spark_max("temperature"), 2).alias("max_temp"),
            spark_round(stddev("temperature"), 2).alias("stddev_temp"),
            spark_round(avg("humidity"), 2).alias("average_humid"),
            spark_round(avg("pressure"), 2).alias("average_pressure"),
            count("temperature").alias("reading_count"),
            spark_max("temperature").alias("highest_temp")
        )

    def process_window_micro_batch(batch_df, batch_id):
        if batch_df.count() == 0:
            return
            
        try:
            flattened_df = batch_df.select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("sensor_id"),
                col("average_temp"),
                col("min_temp"),
                col("max_temp"),
                col("stddev_temp"),
                col("average_humid"),
                col("average_pressure"),
                col("reading_count"),
                when(col("highest_temp") > 60.0, lit("CRITICAL"))
                .when(col("highest_temp") > 40.0, lit("Oui"))
                .otherwise(lit("Non")).alias("alerte")
            )
            flattened_df.write \
                .format("jdbc") \
                .option("url", DB_URL) \
                .option("dbtable", "sensor_windowed_metrics") \
                .option("user", DB_PROPERTIES["user"]) \
                .option("password", DB_PROPERTIES["password"]) \
                .option("driver", DB_PROPERTIES["driver"]) \
                .mode("append") \
                .save()
            print(f"[Spark Fenetre 5 min] Agregations calculees et ecrites dans Postgres (Batch {batch_id})")
        except Exception as e:
            print(f"[Erreur Fenetre] {e}", file=sys.stderr)

    window_query = windowed_df.writeStream \
        .outputMode("update") \
        .foreachBatch(process_window_micro_batch) \
        .start()

    print("[SYSTEME ACTIVE] PySpark Structured Streaming en ecoute sur Kafka...")
    sys.stdout.flush()

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\nArret du moteur Spark Streaming.")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
