import time
import random
import json
import sys
from datetime import datetime

# Liste des 10 capteurs IoT de l'usine
SENSORS = [
    {"id": "sensor-01", "base_temp": 22.0, "base_humid": 45.0, "base_pressure": 1013.0}, # Bureau Admin
    {"id": "sensor-02", "base_temp": 35.0, "base_humid": 30.0, "base_pressure": 1010.0}, # Salle Serveurs #1
    {"id": "sensor-03", "base_temp": 15.0, "base_humid": 60.0, "base_pressure": 1015.0}, # Entrepôt Froid
    {"id": "sensor-04", "base_temp": 28.0, "base_humid": 50.0, "base_pressure": 1012.0}, # Atelier Assemblage
    {"id": "sensor-05", "base_temp": 20.0, "base_humid": 55.0, "base_pressure": 1014.0}, # Laboratoire R&D
    {"id": "sensor-06", "base_temp": 38.0, "base_humid": 25.0, "base_pressure": 1008.0}, # Salle Serveurs #2
    {"id": "sensor-07", "base_temp": 42.0, "base_humid": 20.0, "base_pressure": 1005.0}, # Chaudière Industrielle
    {"id": "sensor-08", "base_temp": 18.0, "base_humid": 65.0, "base_pressure": 1016.0}, # Zone Stockage Chimie
    {"id": "sensor-09", "base_temp": 25.0, "base_humid": 40.0, "base_pressure": 1011.0}, # Zone Emballage
    {"id": "sensor-10", "base_temp": 30.0, "base_humid": 35.0, "base_pressure": 1009.0}, # Zone Compressors
]

def generate_sensor_data():
    sensor = random.choice(SENSORS)
    
    # Variations aléatoires simples autour des valeurs de base
    temp_var = random.uniform(-2.5, 2.5)
    humid_var = random.uniform(-4.0, 4.0)
    press_var = random.uniform(-3.0, 3.0)
    
    temperature = round(sensor["base_temp"] + temp_var, 2)
    humidity = round(max(0.0, min(100.0, sensor["base_humid"] + humid_var)), 2)
    pressure = round(sensor["base_pressure"] + press_var, 2)
    
    # Injection ponctuelle d'anomalies (WARNING / CRITICAL / ERROR)
    rand_val = random.random()
    if rand_val < 0.04:
        temperature = round(random.uniform(41.0, 59.0), 2)
        status = "WARNING"
    elif rand_val < 0.06:
        temperature = round(random.uniform(60.0, 85.0), 2)
        status = "CRITICAL"
    elif rand_val < 0.07:
        status = "ERROR"
    else:
        status = "OK"
        
    return {
        "sensor_id": sensor["id"],
        "timestamp": datetime.now().isoformat(),
        "temperature": temperature,
        "humidity": humidity,
        "pressure": pressure,
        "status": status
    }

# Client Kafka
KAFKA_AVAILABLE = False
try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    pass

def main():
    print("Demarrage du simulateur IoT (10 capteurs)...")
    sys.stdout.flush()
    
    producer = None
    if KAFKA_AVAILABLE:
        try:
            producer = KafkaProducer(
                bootstrap_servers=['localhost:9092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks=1
            )
            print("Connecte au broker Kafka sur localhost:9092.", file=sys.stderr)
        except Exception as e:
            print(f"Connexion a Kafka impossible: {e}", file=sys.stderr)

    print("Emission continue vers les topics Kafka 'iot-sensor-data' et 'iot-sensor-alerts'...", file=sys.stderr)
    sys.stderr.flush()
    
    try:
        count = 0
        while True:
            data = generate_sensor_data()
            count += 1
            
            if producer:
                try:
                    # Topic 1 : Toutes les métriques brutes
                    producer.send('iot-sensor-data', data)
                    
                    # Topic 2 : Événements d'alertes dédiés (Eventbus Kafka)
                    if data["status"] in ("WARNING", "CRITICAL", "ERROR"):
                        producer.send('iot-sensor-alerts', data)
                        print(f"[EVENEMENT KAFKA ALERTES] Signal emis sur iot-sensor-alerts pour {data['sensor_id']} ({data['temperature']} C)", file=sys.stderr)
                        
                    print(f"[{count}] {data['sensor_id']} | Temp: {data['temperature']} C | Humid: {data['humidity']}% | Pression: {data['pressure']} hPa | Statut: {data['status']}", file=sys.stderr)
                    sys.stderr.flush()
                except Exception as e:
                    print(f"Erreur d'envoi Kafka: {e}", file=sys.stderr)
            
            print(json.dumps(data))
            sys.stdout.flush()
            time.sleep(0.8)
    except KeyboardInterrupt:
        print("\nSimulateur arrete.")

if __name__ == "__main__":
    main()
