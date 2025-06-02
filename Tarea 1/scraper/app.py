from pymongo import MongoClient
import redis
import requests
import json
import time
import os

# MongoDB configuration
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://storage:27017')
MONGO_DB = 'waze_data'
MONGO_COLLECTION = 'alerts'

# Redis configuration
REDIS_HOST = os.getenv('REDIS_HOST', 'cache')
REDIS_PORT = 6379
REDIS_TTL = 600  # 10 minutos

# URL del API de Waze
url = 'https://www.waze.com/live-map/api/georss?top=-33.318727734058896&bottom=-33.646196973860725&left=-70.77952194213869&right=-70.502254486084&env=row&types=alerts,traffic'

# Inicializar cliente de MongoDB
client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
collection = db[MONGO_COLLECTION]

# Inicializar cliente de Redis
redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def fetch_and_save():
    try:
        cached_data = redis_client.get("waze_alerts")
        if cached_data:
            print("Datos obtenidos de Redis Cache.")
            alerts = json.loads(cached_data)
        else:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                alerts = data.get("alerts", [])
                if alerts:
                    redis_client.setex("waze_alerts", REDIS_TTL, json.dumps(alerts))
                    print("Datos guardados en Redis Cache.")
                else:
                    print("No se encontraron alertas.")
            else:
                print(f"Error al acceder a la API. Código de estado: {response.status_code}")
                return

        if alerts:
            collection.insert_many(alerts)
            print(f"Guardadas {len(alerts)} alertas en MongoDB.")
    except Exception as e:
        print(f"Error durante la solicitud o guardado: {e}")

if __name__ == "__main__":
    while True:
        fetch_and_save()
        time.sleep(600)  # Intervalo de 10 minutos