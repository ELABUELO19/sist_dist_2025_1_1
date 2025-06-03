#!/usr/bin/env python3
"""
Waze Traffic Data Scraper
Extracts traffic incidents from Waze live map for Santiago Metropolitan Region
"""

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

# Configure logging
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
    """Scraper for Waze traffic data"""
    
    def __init__(self, db_url: str, redis_url: str):
        self.db_url = db_url
        self.redis_client = redis.from_url(redis_url)
        self.session = requests.Session()
        
        # Santiago Metropolitan Region bounds (actualizado con coordenadas específicas)
        self.bounds = {
            'north': -33.318727734058896,   # Top boundary
            'south': -33.646196973860725,   # Bottom boundary  
            'east': -70.502254486084,       # Right boundary
            'west': -70.77952194213869      # Left boundary
        }
        
        # Mapping of incident types (actualizado para manejar respuesta real de Waze)
        self.incident_types = {
            'ACCIDENT': 'accidente',
            'JAM': 'atasco',
            'WEATHERHAZARD': 'clima',
            'HAZARD': 'peligro',
            'MISC': 'otro',
            'CONSTRUCTION': 'construccion',
            'ALERT': 'alerta',
            'IRREGULARITY': 'irregularidad',
            'ROAD_CLOSED': 'carretera_cerrada',
            'POLICE': 'policia'
        }
        
        # Santiago communes
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
        """Get database connection"""
        try:
            conn = psycopg2.connect(self.db_url)
            return conn
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            return None
    
    def create_tables(self):
        """Create necessary database tables"""
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
        """Generate unique incident ID based on location and time"""
        key_string = f"{incident_data.get('location', {}).get('y', 0)}" \
                    f"{incident_data.get('location', {}).get('x', 0)}" \
                    f"{incident_data.get('type', '')}" \
                    f"{incident_data.get('pubMillis', 0)}"
        return hashlib.md5(key_string.encode()).hexdigest()
    
    def determine_comuna(self, latitude: float, longitude: float) -> Optional[str]:
        """Determine comuna based on coordinates (simplified)"""
        # This is a simplified implementation
        # In a real scenario, you would use a geospatial library or service
        
        # Santiago Centro area
        if -33.45 <= latitude <= -33.42 and -70.67 <= longitude <= -70.63:
            return 'Santiago'
        # Las Condes area
        elif -33.42 <= latitude <= -33.38 and -70.6 <= longitude <= -70.5:
            return 'Las Condes'
        # Providencia area
        elif -33.44 <= latitude <= -33.41 and -70.65 <= longitude <= -70.6:
            return 'Providencia'
        # Ñuñoa area
        elif -33.47 <= latitude <= -33.44 and -70.62 <= longitude <= -70.57:
            return 'Ñuñoa'
        # Default fallback
        else:
            return 'Región Metropolitana'
    
    def fetch_waze_data(self) -> List[Dict]:
        """Fetch traffic data from Waze API"""
        try:
            # Waze live map API endpoint with Santiago Metropolitan Region coordinates
            url = "https://www.waze.com/live-map/api/georss"
            
            params = {
                'top': -33.318727734058896,     # Norte de la RM
                'bottom': -33.646196973860725,  # Sur de la RM
                'left': -70.77952194213869,     # Oeste de la RM
                'right': -70.502254486084,      # Este de la RM
                'env': 'row',
                'types': 'alerts,traffic'       # Usando la URL específica proporcionada
            }
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'es-CL,es;q=0.9,en;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Referer': 'https://www.waze.com/live-map',
                'Origin': 'https://www.waze.com',
                'Connection': 'keep-alive',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-origin',
                'Cache-Control': 'no-cache'
            }
            
            logger.info(f"Fetching Waze data with params: {params}")
            
            response = self.session.get(url, params=params, headers=headers, timeout=30)
            response.raise_for_status()
            
            logger.info(f"Waze API response status: {response.status_code}")
            logger.info(f"Response headers: {dict(response.headers)}")
            
            data = response.json()
            logger.info(f"Waze API response keys: {list(data.keys())}")
            
            incidents = []
            
            # Process alerts (accidentes, peligros, etc.)
            alerts = data.get('alerts', [])
            logger.info(f"Processing {len(alerts)} alerts")
            
            for alert in alerts:
                try:
                    # Extract location data
                    location = alert.get('location', {})
                    latitude = location.get('y', 0)
                    longitude = location.get('x', 0)
                    
                    # Skip if coordinates are invalid
                    if latitude == 0 or longitude == 0:
                        continue
                    
                    incident = {
                        'type': alert.get('type', 'MISC'),
                        'subtype': alert.get('subtype', ''),
                        'location': {
                            'y': latitude,
                            'x': longitude
                        },
                        'street': alert.get('street', ''),
                        'city': alert.get('city', ''),
                        'reportDescription': alert.get('reportDescription', ''),
                        'pubMillis': alert.get('pubMillis', int(time.time() * 1000)),
                        'severity': alert.get('confidence', 0),
                        'source_type': 'alert',
                        'uuid': alert.get('uuid', ''),
                        'country': alert.get('country', 'CL'),
                        'roadType': alert.get('roadType', 0),
                        'magvar': alert.get('magvar', 0),
                        'reliability': alert.get('reliability', 0),
                        'reportRating': alert.get('reportRating', 0)
                    }
                    
                    incidents.append(incident)
                    
                except Exception as e:
                    logger.warning(f"Error processing alert: {e}")
                    continue
            
            # Process traffic/jams
            traffic = data.get('jams', [])
            if not traffic:
                traffic = data.get('traffic', [])  # Alternativamente, puede venir como 'traffic'
            
            logger.info(f"Processing {len(traffic)} traffic/jam incidents")
            
            for jam in traffic:
                try:
                    # Get segments or use main coordinates
                    segments = jam.get('segments', [])
                    
                    if segments:
                        # Process each segment of the traffic jam
                        for segment in segments:
                            from_point = segment.get('from', {})
                            to_point = segment.get('to', {})
                            
                            # Use from point as primary location
                            latitude = from_point.get('y', 0)
                            longitude = from_point.get('x', 0)
                            
                            if latitude == 0 or longitude == 0:
                                continue
                            
                            incident = {
                                'type': 'JAM',
                                'subtype': f"speed_{jam.get('speedKMH', 0)}",
                                'location': {
                                    'y': latitude,
                                    'x': longitude
                                },
                                'street': jam.get('street', ''),
                                'city': jam.get('city', ''),
                                'reportDescription': f"Congestión vehicular - Velocidad: {jam.get('speedKMH', 0)} km/h - Nivel: {jam.get('level', 0)}",
                                'pubMillis': jam.get('pubMillis', int(time.time() * 1000)),
                                'severity': jam.get('level', 0),
                                'source_type': 'jam',
                                'speedKMH': jam.get('speedKMH', 0),
                                'level': jam.get('level', 0),
                                'length': jam.get('length', 0),
                                'delay': jam.get('delay', 0),
                                'roadType': jam.get('roadType', 0),
                                'endNode': to_point
                            }
                            
                            incidents.append(incident)
                    else:
                        # If no segments, use main jam data
                        location = jam.get('location', {})
                        latitude = location.get('y', 0)
                        longitude = location.get('x', 0)
                        
                        if latitude != 0 and longitude != 0:
                            incident = {
                                'type': 'JAM',
                                'subtype': f"speed_{jam.get('speedKMH', 0)}",
                                'location': {
                                    'y': latitude,
                                    'x': longitude
                                },
                                'street': jam.get('street', ''),
                                'city': jam.get('city', ''),
                                'reportDescription': f"Congestión vehicular - Velocidad: {jam.get('speedKMH', 0)} km/h",
                                'pubMillis': jam.get('pubMillis', int(time.time() * 1000)),
                                'severity': jam.get('level', 0),
                                'source_type': 'jam',
                                'speedKMH': jam.get('speedKMH', 0),
                                'level': jam.get('level', 0)
                            }
                            
                            incidents.append(incident)
                            
                except Exception as e:
                    logger.warning(f"Error processing jam: {e}")
                    continue
            
            # Process irregularities if present
            irregularities = data.get('irregularities', [])
            logger.info(f"Processing {len(irregularities)} irregularities")
            
            for irregularity in irregularities:
                try:
                    location = irregularity.get('location', {})
                    latitude = location.get('y', 0)
                    longitude = location.get('x', 0)
                    
                    if latitude == 0 or longitude == 0:
                        continue
                    
                    incident = {
                        'type': 'IRREGULARITY',
                        'subtype': irregularity.get('type', ''),
                        'location': {
                            'y': latitude,
                            'x': longitude
                        },
                        'street': irregularity.get('street', ''),
                        'city': irregularity.get('city', ''),
                        'reportDescription': irregularity.get('description', 'Irregularidad en el tráfico'),
                        'pubMillis': irregularity.get('pubMillis', int(time.time() * 1000)),
                        'severity': irregularity.get('severity', 0),
                        'source_type': 'irregularity'
                    }
                    
                    incidents.append(incident)
                    
                except Exception as e:
                    logger.warning(f"Error processing irregularity: {e}")
                    continue
            
            logger.info(f"Successfully fetched {len(incidents)} total incidents from Waze")
            logger.info(f"Breakdown - Alerts: {len(alerts)}, Traffic: {len(traffic)}, Irregularities: {len(irregularities)}")
            
            return incidents
            
        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP error fetching Waze data: {e}")
            return []
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error fetching Waze data: {e}")
            return []
    
    def process_incident(self, incident_data: Dict) -> Optional[TrafficIncident]:
        """Process raw incident data into TrafficIncident object"""
        try:
            location = incident_data.get('location', {})
            latitude = location.get('y', 0)
            longitude = location.get('x', 0)
            
            # Skip incidents outside our bounds
            if not (self.bounds['south'] <= latitude <= self.bounds['north'] and
                    self.bounds['west'] <= longitude <= self.bounds['east']):
                return None
            
            incident_id = self.generate_incident_id(incident_data)
            incident_type = self.incident_types.get(
                incident_data.get('type', 'MISC'), 'otro'
            )
            
            # Determine comuna
            comuna = self.determine_comuna(latitude, longitude)
            
            # Create timestamp
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
        """Save incident to database"""
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
        """Cache incident in Redis"""
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
        """Main scraping and storage method"""
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
        
        # Update metrics in Redis
        try:
            self.redis_client.setex("scraper:last_run", 3600, datetime.now().isoformat())
            self.redis_client.setex("scraper:incidents_saved", 3600, saved_count)
        except Exception as e:
            logger.error(f"Error updating metrics: {e}")
    
    def export_to_csv(self, output_path: str = "/app/data"):
        """Export incidents to CSV for Pig processing"""
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
                
                # Write to CSV
                csv_file = os.path.join(output_path, f"incidents_{int(time.time())}.csv")
                with open(csv_file, 'w', encoding='utf-8') as f:
                    # Header
                    f.write("incident_id,incident_type,description,latitude,longitude,"
                           "street,city,comuna,severity,timestamp,source\n")
                    
                    # Data
                    for incident in incidents:
                        # Escape commas and quotes in strings
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
    """Main function"""
    db_url = os.getenv('DATABASE_URL', 'postgresql://traffic_user:traffic_pass@localhost:5432/traffic_db')
    redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
    
    scraper = WazeScraper(db_url, redis_url)
    
    # Initialize database
    if not scraper.create_tables():
        logger.error("Failed to initialize database")
        return
    
    # Schedule scraping every 5 minutes
    schedule.every(5).minutes.do(scraper.scrape_and_store)
    schedule.every(30).minutes.do(scraper.export_to_csv)
    
    # Run initial scrape
    scraper.scrape_and_store()
    scraper.export_to_csv()
    
    logger.info("Scraper initialized. Running scheduled tasks...")
    
    # Keep running
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()