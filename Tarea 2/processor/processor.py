import os
import json
import time
import logging
import subprocess
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import psycopg2
from psycopg2.extras import RealDictCursor
import redis
import pandas as pd
from dataclasses import dataclass
import schedule


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class ProcessingMetrics:
    """Metrics for processing performance"""
    total_records: int
    filtered_records: int
    processing_time: float
    errors_count: int
    timestamp: datetime

class TrafficDataProcessor:
    """Main processor for traffic data"""
    
    def __init__(self, db_url: str, redis_url: str):
        self.db_url = db_url
        self.redis_client = redis.from_url(redis_url)
        self.data_path = "/app/data"
        self.output_path = "/app/output"
        
        os.makedirs(self.data_path, exist_ok=True)
        os.makedirs(self.output_path, exist_ok=True)
        
        self.duplicate_distance_threshold = 0.001  # ~100 meters
        self.duplicate_time_threshold = 300  # 5 minutes
        
    def get_database_connection(self):
        try:
            conn = psycopg2.connect(self.db_url)
            return conn
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            return None
    
    def filter_and_clean_data(self) -> Tuple[int, int]:
        """Filter and clean raw incident data"""
        conn = self.get_database_connection()
        if not conn:
            return 0, 0
            
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Get unprocessed incidents
                cursor.execute("""
                    SELECT * FROM traffic_incidents 
                    WHERE processed = FALSE 
                    ORDER BY timestamp DESC
                """)
                
                raw_incidents = cursor.fetchall()
                logger.info(f"Found {len(raw_incidents)} unprocessed incidents")
                
                if not raw_incidents:
                    return 0, 0
                
                filtered_incidents = []
                
                for incident in raw_incidents:
                    if self.is_valid_incident(incident):
                        filtered_incidents.append(incident)
                
                logger.info(f"Filtered to {len(filtered_incidents)} valid incidents")
                
                deduplicated_incidents = self.remove_duplicates(filtered_incidents)
                logger.info(f"After deduplication: {len(deduplicated_incidents)} incidents")
                
                homogenized_incidents = self.homogenize_incidents(deduplicated_incidents)
                
                incident_ids = [inc['incident_id'] for inc in homogenized_incidents]
                if incident_ids:
                    cursor.execute("""
                        UPDATE traffic_incidents 
                        SET processed = TRUE 
                        WHERE incident_id = ANY(%s)
                    """, (incident_ids,))
                    conn.commit()
                
                return len(raw_incidents), len(homogenized_incidents)
                
        except Exception as e:
            logger.error(f"Error in filtering and cleaning: {e}")
            return 0, 0
        finally:
            conn.close()
    
    def is_valid_incident(self, incident: Dict) -> bool:
        required_fields = ['incident_id', 'incident_type', 'latitude', 'longitude', 'timestamp']
        for field in required_fields:
            if not incident.get(field):
                return False
        
        lat, lon = incident['latitude'], incident['longitude']
        if not (-33.8 <= lat <= -33.2 and -71.0 <= lon <= -70.3):
            return False
        
        description = incident.get('description', '').strip()
        if len(description) < 3:
            return False

        try:
            incident_time = incident['timestamp']
            if isinstance(incident_time, str):
                incident_time = datetime.fromisoformat(incident_time.replace('Z', '+00:00'))
            
            time_diff = datetime.now().replace(tzinfo=incident_time.tzinfo) - incident_time
            if time_diff.days > 7:
                return False
        except:
            return False
        
        return True
    
    def remove_duplicates(self, incidents: List[Dict]) -> List[Dict]:
        if not incidents:
            return incidents
        
        sorted_incidents = sorted(incidents, key=lambda x: x['timestamp'])
        deduplicated = []
        
        for incident in sorted_incidents:
            is_duplicate = False
            
            for existing in deduplicated:
                if self.are_incidents_duplicate(incident, existing):
                    is_duplicate = True
                    break
            
            if not is_duplicate:
                deduplicated.append(incident)
        
        return deduplicated
    
    def are_incidents_duplicate(self, inc1: Dict, inc2: Dict) -> bool:
        if inc1['incident_type'] != inc2['incident_type']:
            return False
        
        lat_diff = abs(inc1['latitude'] - inc2['latitude'])
        lon_diff = abs(inc1['longitude'] - inc2['longitude'])
        
        if lat_diff > self.duplicate_distance_threshold or lon_diff > self.duplicate_distance_threshold:
            return False
        
        try:
            time1 = inc1['timestamp']
            time2 = inc2['timestamp']
            
            if isinstance(time1, str):
                time1 = datetime.fromisoformat(time1.replace('Z', '+00:00'))
            if isinstance(time2, str):
                time2 = datetime.fromisoformat(time2.replace('Z', '+00:00'))
            
            time_diff = abs((time1 - time2).total_seconds())
            if time_diff > self.duplicate_time_threshold:
                return False
        except:
            return False
        
        return True
    
    def homogenize_incidents(self, incidents: List[Dict]) -> List[Dict]:
        homogenized = []
        
        type_mapping = {
            'accidente': 'ACCIDENTE',
            'atasco': 'ATASCO', 
            'construccion': 'CONSTRUCCION',
            'peligro': 'PELIGRO',
            'clima': 'CLIMA',
            'alerta': 'ALERTA',
            'otro': 'OTRO'
        }
        
        comuna_mapping = {
            'santiago': 'Santiago',
            'las condes': 'Las Condes',
            'providencia': 'Providencia',
            'ñuñoa': 'Ñuñoa',
            'maipú': 'Maipú',
            'maipu': 'Maipú',
            'la florida': 'La Florida',
            'puente alto': 'Puente Alto',
            'peñalolén': 'Peñalolén',
            'penalolen': 'Peñalolén'
        }
        
        for incident in incidents:
            try:
                incident_type = incident.get('incident_type', '').lower()
                standardized_type = type_mapping.get(incident_type, 'OTRO')
                
                comuna = incident.get('comuna', '').lower().strip()
                standardized_comuna = comuna_mapping.get(comuna, incident.get('comuna', ''))
                
                description = incident.get('description', '').strip()
                if not description:
                    description = f"Incidente de tipo {standardized_type}"
                
                severity = incident.get('severity', 0)
                if severity > 10:
                    severity = min(severity // 10, 10)
                
                homogenized_incident = {
                    'incident_id': incident['incident_id'],
                    'incident_type': standardized_type,
                    'description': description,
                    'latitude': round(incident['latitude'], 6),
                    'longitude': round(incident['longitude'], 6),
                    'street': incident.get('street', '').strip(),
                    'city': incident.get('city', '').strip(),
                    'comuna': standardized_comuna,
                    'severity': severity,
                    'timestamp': incident['timestamp'],
                    'source': incident.get('source', 'waze')
                }
                
                homogenized.append(homogenized_incident)
                
            except Exception as e:
                logger.error(f"Error homogenizing incident {incident.get('incident_id')}: {e}")
                continue
        
        return homogenized
    
    def export_for_pig_processing(self) -> str:
        conn = self.get_database_connection()
        if not conn:
            return ""
            
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT incident_id, incident_type, description, latitude, longitude,
                           street, city, comuna, severity, timestamp, source
                    FROM traffic_incidents
                    WHERE processed = TRUE
                    ORDER BY timestamp DESC
                    LIMIT 10000
                """)
                
                incidents = cursor.fetchall()
                
                if not incidents:
                    logger.warning("No processed incidents found for export")
                    return ""

                timestamp = int(time.time())
                csv_filename = f"processed_incidents_{timestamp}.csv"
                csv_path = os.path.join(self.data_path, csv_filename)
                
                df = pd.DataFrame(incidents)
                df = df.fillna('')
                df['description'] = df['description'].str.replace('"', '""')
                df['street'] = df['street'].str.replace('"', '""')
                
                df.to_csv(csv_path, index=False, encoding='utf-8')
                
                logger.info(f"Exported {len(incidents)} incidents to {csv_path}")
                return csv_path
                
        except Exception as e:
            logger.error(f"Error exporting data: {e}")
            return ""
        finally:
            conn.close()
    
    def run_pig_analysis(self, csv_path: str) -> bool:
        try:
            if not csv_path or not os.path.exists(csv_path):
                logger.error("CSV file not found for Pig processing")
                return False
            
            hdfs_input_path = "/data/"
            hdfs_cmd = f"hdfs dfs -put -f {csv_path} {hdfs_input_path}"
            
            result = subprocess.run(hdfs_cmd, shell=True, capture_output=True, text=True)
            if result.returncode != 0:
                logger.error(f"Failed to copy data to HDFS: {result.stderr}")
                return False
            
            pig_script_path = "/pig_scripts/traffic_analysis.pig"
            pig_cmd = f"pig -f {pig_script_path}"
            
            logger.info("Starting Apache Pig analysis...")
            start_time = time.time()
            
            result = subprocess.run(pig_cmd, shell=True, capture_output=True, text=True)
            
            processing_time = time.time() - start_time
            
            if result.returncode == 0:
                logger.info(f"Pig analysis completed successfully in {processing_time:.2f} seconds")
                logger.info(f"Pig output: {result.stdout}")
                return True
            else:
                logger.error(f"Pig analysis failed: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"Error running Pig analysis: {e}")
            return False
    
    def cache_analysis_results(self):
        try:
            output_files = {
                'comuna_counts': '/output/incidents_by_comuna/part-r-00000',
                'type_counts': '/output/incidents_by_type/part-r-00000',
                'hourly_counts': '/output/incidents_by_hour/part-r-00000',
                'hotspots': '/output/traffic_hotspots/part-r-00000'
            }
            
            for cache_key, file_path in output_files.items():
                try:
                    if os.path.exists(file_path):
                        with open(file_path, 'r') as f:
                            content = f.read()
                            self.redis_client.setex(
                                f"analysis:{cache_key}", 
                                3600,  # 1 hour TTL
                                content
                            )
                        logger.info(f"Cached {cache_key} analysis results")
                except Exception as e:
                    logger.error(f"Error caching {cache_key}: {e}")
            
            self.redis_client.setex(
                "analysis:last_update", 
                3600, 
                datetime.now().isoformat()
            )
            
        except Exception as e:
            logger.error(f"Error caching results: {e}")
    
    def generate_processing_report(self, metrics: ProcessingMetrics) -> Dict:
        try:
            cache_stats = {}
            cache_keys = [
                'analysis:comuna_counts',
                'analysis:type_counts', 
                'analysis:hourly_counts',
                'analysis:hotspots'
            ]
            
            for key in cache_keys:
                try:
                    ttl = self.redis_client.ttl(key)
                    cache_stats[key] = {
                        'exists': self.redis_client.exists(key),
                        'ttl': ttl
                    }
                except:
                    cache_stats[key] = {'exists': False, 'ttl': -1}
            
            conn = self.get_database_connection()
            db_stats = {}
            
            if conn:
                try:
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT COUNT(*) FROM traffic_incidents")
                        total_incidents = cursor.fetchone()[0]
                        
                        cursor.execute("SELECT COUNT(*) FROM traffic_incidents WHERE processed = TRUE")
                        processed_incidents = cursor.fetchone()[0]
                        
                        cursor.execute("""
                            SELECT incident_type, COUNT(*) 
                            FROM traffic_incidents 
                            WHERE processed = TRUE 
                            GROUP BY incident_type 
                            ORDER BY COUNT(*) DESC 
                            LIMIT 5
                        """)
                        top_types = cursor.fetchall()
                        
                        db_stats = {
                            'total_incidents': total_incidents,
                            'processed_incidents': processed_incidents,
                            'processing_rate': (processed_incidents / total_incidents * 100) if total_incidents > 0 else 0,
                            'top_incident_types': top_types
                        }
                except Exception as e:
                    logger.error(f"Error getting DB stats: {e}")
                finally:
                    conn.close()
            
            report = {
                'processing_metrics': {
                    'total_records': metrics.total_records,
                    'filtered_records': metrics.filtered_records,
                    'processing_time': metrics.processing_time,
                    'errors_count': metrics.errors_count,
                    'timestamp': metrics.timestamp.isoformat(),
                    'filter_rate': (metrics.filtered_records / metrics.total_records * 100) if metrics.total_records > 0 else 0
                },
                'cache_statistics': cache_stats,
                'database_statistics': db_stats,
                'system_performance': {
                    'records_per_second': metrics.filtered_records / metrics.processing_time if metrics.processing_time > 0 else 0
                }
            }
            
            return report
            
        except Exception as e:
            logger.error(f"Error generating report: {e}")
            return {}
    
    def process_data_pipeline(self):
        """Main data processing pipeline"""
        logger.info("Starting data processing pipeline")
        start_time = time.time()
        
        try:
            logger.info("Step 1: Filtering and cleaning data")
            total_records, filtered_records = self.filter_and_clean_data()
            
            if filtered_records == 0:
                logger.info("No data to process")
                return
            
            logger.info("Step 2: Exporting data for Apache Pig")
            csv_path = self.export_for_pig_processing()
            
            if not csv_path:
                logger.error("Failed to export data for Pig processing")
                return
            
            logger.info("Step 3: Running Apache Pig analysis")
            pig_success = self.run_pig_analysis(csv_path)
            
            if not pig_success:
                logger.error("Pig analysis failed")
                return
            
            logger.info("Step 4: Caching analysis results")
            self.cache_analysis_results()
            
            processing_time = time.time() - start_time
            metrics = ProcessingMetrics(
                total_records=total_records,
                filtered_records=filtered_records,
                processing_time=processing_time,
                errors_count=0, 
                timestamp=datetime.now()
            )
            
            report = self.generate_processing_report(metrics)
            
            self.redis_client.setex(
                "processing:last_report",
                3600,
                json.dumps(report, default=str)
            )
            
            logger.info(f"Processing pipeline completed successfully in {processing_time:.2f} seconds")
            logger.info(f"Processed {filtered_records} incidents from {total_records} total records")
            
        except Exception as e:
            logger.error(f"Error in processing pipeline: {e}")

def main():
    db_url = os.getenv('DATABASE_URL', 'postgresql://traffic_user:traffic_pass@postgres:5432/traffic_db')
    redis_url = os.getenv('REDIS_URL', 'redis://redis:6379')
    
    processor = TrafficDataProcessor(db_url, redis_url)
    
    schedule.every(30).minutes.do(processor.process_data_pipeline)
    
    processor.process_data_pipeline()
    
    logger.info("Data processor initialized. Running scheduled tasks...")
    
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()