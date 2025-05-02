from pymongo import MongoClient
import redis
import os
import random
import time
import numpy as np

# Configuración MongoDB
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://storage:27017')
MONGO_DB = 'waze_data'
MONGO_COLLECTION = 'alerts'

# Configuración Redis
REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
REDIS_PORT = 6379

# Inicializar clientes
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[MONGO_DB]
collection = db[MONGO_COLLECTION]

redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def simulate_queries(num_queries=1000):
    # Obtener todos los IDs de alertas desde Mongo
    alert_ids = list(collection.find({}, {'_id': 1}))
    if not alert_ids:
        print("No hay alertas disponibles en MongoDB.")
        return

    ids = [str(doc['_id']) for doc in alert_ids]
    n = len(ids)

    zipf_dist = np.random.zipf(a=2, size=num_queries) % n

    hit, miss = 0, 0
# Simula consulta a caché y MongoDB
    for i in range(num_queries):
        idx = zipf_dist[i]
        alert_id = ids[idx]

        
        cached = redis_client.get(alert_id)
        if cached:
            hit += 1
            print(f"[Cache HIT] Alerta {alert_id}")
        else:
            alert = collection.find_one({'_id': alert_ids[idx]['_id']})
            if alert:
                redis_client.setex(alert_id, 300, str(alert))
                miss += 1
                print(f"[Cache MISS] Alerta {alert_id} consultada y cacheada")

        time.sleep(0.01)  

    print(f"\n➡️ Total Consultas: {num_queries}")
    print(f"✅ Cache HITs: {hit} | ❌ MISSes: {miss} | HIT Ratio: {hit/num_queries:.2f}")

if __name__ == "__main__":
    while True:
        simulate_queries(500)  # Puedes variar el número
        time.sleep(10)  # Cada 5 minutos
