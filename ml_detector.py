import numpy as np
from sklearn.ensemble import IsolationForest

class SensorMLDetector:
    def __init__(self):
        # Entrainement d'un modele IsolationForest sur des donnees normales
        self.model = IsolationForest(contamination=0.05, random_state=42)
        self._train_baseline_model()

    def _train_baseline_model(self):
        # Generation de donnees d'apprentissage normales (temperature, humidite, pression)
        np.random.seed(42)
        n_samples = 1000
        
        temps = np.random.normal(loc=24.0, scale=4.0, size=n_samples)
        humids = np.random.normal(loc=50.0, scale=10.0, size=n_samples)
        pressures = np.random.normal(loc=1013.0, scale=4.0, size=n_samples)
        
        X_train = np.column_stack([temps, humids, pressures])
        self.model.fit(X_train)

    def predict(self, temperature, humidity, pressure):
        """
        Predit si la mesure (temp, humid, pressure) est une anomalie via le modele ML.
        Retourne : (is_anomaly: bool, score: float)
        """
        features = np.array([[temperature, humidity, pressure]])
        prediction = self.model.predict(features)[0]  # -1 pour anomalie, 1 pour normal
        score = float(self.model.score_samples(features)[0])
        
        is_anomaly = (prediction == -1) or (temperature > 40.0)
        return is_anomaly, round(score, 4)

# Instance globale
detector = SensorMLDetector()

def check_ml_anomaly(temperature, humidity, pressure):
    return detector.predict(temperature, humidity, pressure)
