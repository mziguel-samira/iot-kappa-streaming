# 🏭 Architecture Big Data Kappa — Monitoring IoT & PySpark Streaming ML

## Architecture Globale du Pipeline (Streaming + Data Lake + ML)

```text
                        ┌────────────────────────┐
                        │  Python IoT Simulator  │
                        │  10 Capteurs Usine     │
                        └───────────┬────────────┘
                                    │ (JSON Stream)
                                    ▼
                        ┌────────────────────────┐
                        │      Apache Kafka      │
                        │                        │
                        │  iot-sensor-data       │
                        │  iot-sensor-alerts     │
                        └───────────┬────────────┘
                                    │ (Micro-batch Streaming)
                                    ▼
             ┌──────────────────────────────────────────────┐
             │       PySpark Structured Streaming           │
             │                                              │
             │ • Parsing Schema JSON & Data Cleaning        │
             │ • Watermark (10 min)                         │
             │ • Fenêtrage (5 min, Slide 1 min)             │
             │ • Statistiques (Avg, Min, Max, StdDev)       │
             │ • Détection 2 Niveaux (Règles 40°C/60°C)     │
             │ • Machine Learning (IsolationForest UDF)     │
             └──────────────┬───────────────────────────────┘
                            │
             ┌──────────────┴──────────────┐
             │                             │
             ▼                             ▼
       PostgreSQL                     Data Lake (Parquet)
       Serving DB                     Stockage Long Terme
             │                             │
             ▼                             ▼
          Grafana                       MinIO S3
     Dashboard Temps Réel            Console Port 9001
```

## Stack Technique & Justification des Choix

| Couche | Technologie | Rôle Exact |
| :--- | :--- | :--- |
| **Simulation** | **Python** (`sensor_generator.py`) | Génération continue de 10 capteurs IoT (Température, Humidité, Pression) avec injection d'anomalies. |
| **Messaging** | **Apache Kafka** (Port 9092) | Bus d'événements temps réel avec multi-topics (`iot-sensor-data`, `iot-sensor-alerts`). |
| **Processing** | **PySpark Structured Streaming** | Moteur central de calcul distribué (Watermarking 10 min, Data Cleaning, Fenêtrage 5 min). |
| **Machine Learning** | **Scikit-Learn IsolationForest UDF** | Modèle IA non supervisé prédisant les comportements anormaux des capteurs en direct. |
| **Serving DB** | **PostgreSQL 15** (Port 5433) | Base de données de restitution structurée (`sensor_metrics`, `sensor_alerts`, `sensor_windowed_metrics`). |
| **Data Lake** | **MinIO + Format Parquet** | Stockage distribué des données brutes et transformées en fichiers Parquet pour archivage analytique. |
| **Visualisation** | **Grafana** (Port 3000) | Dashboard temps réel rafraîchi toutes les 5s (Courbes, Statistiques, Alertes 2 Niveaux). |
| **Orchestration** | **Docker Compose** | Orchestration conteneurisée de Zookeeper, Kafka, Postgres, MinIO, Spark Master/Worker et Grafana. |

## Prérequis

Avant de lancer le projet, assure-toi d'avoir installé :

- **Docker Desktop** (démarré et en cours d'exécution)
- **Python 3.9+** avec `pip`
- **Java 8 ou 11** (nécessaire pour PySpark)
- **Windows uniquement** : Hadoop/winutils installé dans `C:\hadoop` (requis pour PySpark sur Windows)

### Installation des dépendances Python

```bash
pip install -r requirements.txt
```

## Configuration

Les identifiants (mots de passe PostgreSQL, Grafana, MinIO) sont définis dans un fichier `.env` à la racine du projet. Un fichier modèle `.env.example` est fourni :

```bash
cp .env.example .env
```

Puis modifie les valeurs dans `.env` selon tes besoins :

```env
POSTGRES_PASSWORD=your_secure_password
GRAFANA_PASSWORD=your_secure_password
MINIO_PASSWORD=your_secure_password
```

⚠️ Le fichier `.env` **ne doit jamais être commité** sur Git (il est déjà listé dans `.gitignore`).

## Déploiement et Démonstration

### 1. Démarrer l'Infrastructure Docker

```bash
start_infrastructure.bat
```

* **Spark Master UI :** `http://localhost:8080`
* **MinIO Console UI :** `http://localhost:9001` (identifiants dans `.env`)
* **Grafana UI :** `http://localhost:3000` (identifiants dans `.env`)

### 2. Lancer le Moteur PySpark Streaming + ML

```bash
run_spark_streaming.bat
```

### 3. Lancer le Simulateur 10 Capteurs IoT

```bash
run_producer.bat
```

### 4. Vérification PostgreSQL + Data Lake Parquet

```bash
python check_db.py
```

## Structure du Dossier

```text
iot-kappa-streaming/
├── docker-compose.yml              # Orchestration (Kafka, Zookeeper, Spark Master/Worker, Postgres, MinIO, Grafana)
├── .env.example                    # Modèle des variables d'environnement (à copier en .env)
├── .gitignore                      # Fichiers exclus du dépôt Git
├── requirements.txt                # Dépendances Python
├── init.sql                        # Script SQL d'initialisation de la base PostgreSQL
├── sensor_generator.py             # Producteur Python (10 Capteurs IoT -> Multi-topics Kafka)
├── ml_detector.py                  # Modèle Machine Learning (IsolationForest)
├── spark_streaming_processor.py    # Moteur PySpark Structured Streaming + ML + Parquet Data Lake
├── check_db.py                     # Vérification de PostgreSQL et du Data Lake Parquet
├── README.md                       # Documentation de l'architecture Kappa
├── grafana/                        # Configuration et dashboard Grafana (Port 3000)
├── start_infrastructure.bat        # Script de démarrage Docker Compose
├── run_producer.bat                # Script de démarrage des Capteurs Python
└── run_spark_streaming.bat         # Script de démarrage du job PySpark Streaming
```

## Auteur

Projet académique Big Data — Architecture Kappa (Streaming + Data Lake + ML).