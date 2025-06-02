import requests
import json
import time
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
import redis
import hashlib
import schedule
from dataclasses import dataclass, asdict
import os


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class TrafficIncident:
    """Data class for traffic incidents"""
    incident_id: str
    incident_type: str
    description: str
    latitude: float
    longitude: float
    street: str
    city: str
    comuna: Optional[str]
    severity: int
    timestamp: datetime
    source: str = "waze"
    processed: bool = False

class WazeScraper:
    
    def __init__(self, db_url: str, redis_url: str):
        self.db_url = db_url
        self.redis_client = redis.from_url(redis_url)
        self.session = requests.Session()
        
        self.bounds = {
            'north': -33.2,
            'south': -33.8,
            'east': -70.3,
            'west': -71.0
        }
        
        
        self.incident_types = {
            'ACCIDENT': 'accidente',
            'JAM': 'atasco',
            'WEATHERHAZARD': 'clima',
            'HAZARD': 'peligro',
            'MISC': 'otro',
            'CONSTRUCTION': 'construccion',
            'ALERT': 'alerta'
        }
        
        self.comunas = [
            'Santiago', 'Las Condes', 'Providencia', 'Ñuñoa', 'Maipú',
            'La Florida', 'Puente Alto', 'Peñalolén', 'San Bernardo',
            'Quilicura', 'Huechuraba', 'Renca', 'Independencia',
            'Recoleta', 'Vitacura', 'Lo Barnechea', 'Conchalí',
            'Quinta Normal', 'Estación Central', 'Pedro Aguirre Cerda',
            'San Miguel', 'San Joaquín', 'Macul', 'San Ramón',
            'La Cisterna', 'El Bosque', 'La Granja', 'Lo Espejo',
            'Cerro Navia', 'Pudahuel', 'Lo Prado', 'Cerrillos',
            'Maipú', 'Padre Hurtado', 'Peñaflor', 'Talagante'
        ]
    
    def get_database_connection(self):
        try:
            conn = psycopg2.connect(self.db_url)
            return conn
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            return None
    
    def create_tables(self):
        conn = self.get_database_connection()
        if not conn:
            return False
            
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS traffic_incidents (
                        id SERIAL PRIMARY KEY,
                        incident_id VARCHAR(255) UNIQUE NOT NULL,
                        incident_type VARCHAR(100) NOT NULL,
                        description TEXT,
                        latitude FLOAT NOT NULL,
                        longitude FLOAT NOT NULL,
                        street VARCHAR(255),
                        city VARCHAR(100),
                        comuna VARCHAR(100),
                        severity INTEGER,
                        timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                        source VARCHAR(50) DEFAULT 'waze',
                        processed BOOLEAN DEFAULT FALSE,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    );
                    
                    CREATE INDEX IF NOT EXISTS idx_incident_timestamp 
                    ON traffic_incidents(timestamp);
                    
                    CREATE INDEX IF NOT EXISTS idx_incident_comuna 
                    ON traffic_incidents(comuna);
                    
                    CREATE INDEX IF NOT EXISTS idx_incident_type 
                    ON traffic_incidents(incident_type);
                """)
                conn.commit()
                logger.info("Database tables created successfully")
                return True
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            return False
        finally:
            conn.close()
    
    def generate_incident_id(self, incident_data: Dict) -> str: 
        key_string = f"{incident_data.get('location', {}).get('y', 0)}" \
                    f"{incident_data.get('location', {}).get('x', 0)}" \
                    f"{incident_data.get('type', '')}" \
                    f"{incident_data.get('pubMillis', 0)}"
        return hashlib.md5(key_string.encode()).hexdigest()
    
    def determine_comuna(self, latitude: float, longitude: float) -> Optional[str]:        
        
        if -33.45 <= latitude <= -33.42 and -70.67 <= longitude <= -70.63:
            return 'Santiago'
        
        elif -33.42 <= latitude <= -33.38 and -70.6 <= longitude <= -70.5:
            return 'Las Condes'
        
        elif -33.44 <= latitude <= -33.41 and -70.65 <= longitude <= -70.6:
            return 'Providencia'
        
        elif -33.47 <= latitude <= -33.44 and -70.62 <= longitude <= -70.57:
            return 'Ñuñoa'
        
        else:
            return 'Región Metropolitana'
    
    def fetch_waze_data(self) -> List[Dict]:
        try:
            
            url = "https://www.waze.com/live-map/api/georss"
            
            params = {
                'bottom': self.bounds['south'],
                'top': self.bounds['north'],
                'left': self.bounds['west'],
                'right': self.bounds['east'],
                'env': 'row',
                'types': 'alerts,jams,irregularities'
            }
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json',
                'Referer': 'https://www.waze.com/live-map'
            }
            
            response = self.session.get(url, params=params, headers=headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            incidents = []
            
            
            for alert in data.get('alerts', []):
                incidents.append({
                    'type': alert.get('type', 'MISC'),
                    'subtype': alert.get('subtype', ''),
                    'location': alert.get('location', {}),
                    'street': alert.get('street', ''),
                    'city': alert.get('city', ''),
                    'reportDescription': alert.get('reportDescription', ''),
                    'pubMillis': alert.get('pubMillis', 0),
                    'severity': alert.get('confidence', 0),
                    'source_type': 'alert'
                })
            
            
            for jam in data.get('jams', []):
                for segment in jam.get('segments', []):
                    incidents.append({
                        'type': 'JAM',
                        'subtype': jam.get('speedKMH', 0),
                        'location': {
                            'y': segment.get('from', {}).get('y', 0),
                            'x': segment.get('from', {}).get('x', 0)
                        },
                        'street': jam.get('street', ''),
                        'city': jam.get('city', ''),
                        'reportDescription': f"Traffic jam - Speed: {jam.get('speedKMH', 0)} km/h",
                        'pubMillis': jam.get('pubMillis', int(time.time() * 1000)),
                        'severity': jam.get('level', 0),
                        'source_type': 'jam'
                    })
            
            logger.info(f"Fetched {len(incidents)} incidents from Waze")
            return incidents
            
        except Exception as e:
            logger.error(f"Error fetching Waze data: {e}")
            return []
    
    def process_incident(self, incident_data: Dict) -> Optional[TrafficIncident]:
        try:
            location = incident_data.get('location', {})
            latitude = location.get('y', 0)
            longitude = location.get('x', 0)
            
            
            if not (self.bounds['south'] <= latitude <= self.bounds['north'] and
                    self.bounds['west'] <= longitude <= self.bounds['east']):
                return None
            
            incident_id = self.generate_incident_id(incident_data)
            incident_type = self.incident_types.get(
                incident_data.get('type', 'MISC'), 'otro'
            )
            
            
            comuna = self.determine_comuna(latitude, longitude)
            
            
            pub_millis = incident_data.get('pubMillis', int(time.time() * 1000))
            timestamp = datetime.fromtimestamp(pub_millis / 1000, tz=timezone.utc)
            
            return TrafficIncident(
                incident_id=incident_id,
                incident_type=incident_type,
                description=incident_data.get('reportDescription', ''),
                latitude=latitude,
                longitude=longitude,
                street=incident_data.get('street', ''),
                city=incident_data.get('city', ''),
                comuna=comuna,
                severity=incident_data.get('severity', 0),
                timestamp=timestamp
            )
            
        except Exception as e:
            logger.error(f"Error processing incident: {e}")
            return None
    
    def save_incident(self, incident: TrafficIncident) -> bool:
        
        conn = self.get_database_connection()
        if not conn:
            return False
            
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO traffic_incidents 
                    (incident_id, incident_type, description, latitude, longitude, 
                     street, city, comuna, severity, timestamp, source, processed)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (incident_id) DO NOTHING
                """, (
                    incident.incident_id,
                    incident.incident_type,
                    incident.description,
                    incident.latitude,
                    incident.longitude,
                    incident.street,
                    incident.city,
                    incident.comuna,
                    incident.severity,
                    incident.timestamp,
                    incident.source,
                    incident.processed
                ))
                conn.commit()
                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error saving incident: {e}")
            return False
        finally:
            conn.close()
    
    def cache_incident(self, incident: TrafficIncident):
        try:
            cache_key = f"incident:{incident.incident_id}"
            self.redis_client.setex(
                cache_key, 
                3600,  # 1 hour TTL
                json.dumps(asdict(incident), default=str)
            )
        except Exception as e:
            logger.error(f"Error caching incident: {e}")
    
    def scrape_and_store(self):
        logger.info("Starting Waze scraping cycle")
        
        incidents_data = self.fetch_waze_data()
        if not incidents_data:
            logger.warning("No incidents fetched")
            return
        
        saved_count = 0
        for incident_data in incidents_data:
            incident = self.process_incident(incident_data)
            if incident:
                if self.save_incident(incident):
                    self.cache_incident(incident)
                    saved_count += 1
        
        logger.info(f"Saved {saved_count} new incidents to database")
        
        
        try:
            self.redis_client.setex("scraper:last_run", 3600, datetime.now().isoformat())
            self.redis_client.setex("scraper:incidents_saved", 3600, saved_count)
        except Exception as e:
            logger.error(f"Error updating metrics: {e}")
    
    def export_to_csv(self, output_path: str = "/app/data"):
        conn = self.get_database_connection()
        if not conn:
            return
            
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT incident_id, incident_type, description, latitude, longitude,
                           street, city, comuna, severity, timestamp, source
                    FROM traffic_incidents
                    WHERE processed = FALSE
                    ORDER BY timestamp DESC
                """)
                
                incidents = cursor.fetchall()
                
               
                csv_file = os.path.join(output_path, f"incidents_{int(time.time())}.csv")
                with open(csv_file, 'w', encoding='utf-8') as f:
                    # Header
                    f.write("incident_id,incident_type,description,latitude,longitude,"
                           "street,city,comuna,severity,timestamp,source\n")
                    
                    
                    for incident in incidents:
                        
                        row = []
                        for field in incident:
                            if isinstance(field, str):
                                field = field.replace('"', '""')
                                if ',' in field or '"' in field:
                                    field = f'"{field}"'
                            row.append(str(field))
                        f.write(",".join(row) + "\n")
                
                logger.info(f"Exported {len(incidents)} incidents to {csv_file}")
                
        except Exception as e:
            logger.error(f"Error exporting to CSV: {e}")
        finally:
            conn.close()

def main():
    db_url = os.getenv('DATABASE_URL', 'postgresql://traffic_user:traffic_pass@localhost:5432/traffic_db')
    redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
    
    scraper = WazeScraper(db_url, redis_url)
    
    
    if not scraper.create_tables():
        logger.error("Failed to initialize database")
        return
    
    
    schedule.every(1).minutes.do(scraper.scrape_and_store)
    schedule.every(15).minutes.do(scraper.export_to_csv)
    
    
    scraper.scrape_and_store()
    scraper.export_to_csv()
    
    logger.info("Scraper initialized. Running scheduled tasks...")
    
    
    while True:
        schedule.run_pending()
        time.sleep(10)

if __name__ == "__main__":
    main()