#!/usr/bin/env python3
"""
API Service for Traffic Analysis System
Provides REST endpoints for accessing processed traffic data and analytics
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor
import redis
import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Configuration
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://traffic_user:traffic_pass@postgres:5432/traffic_db')
REDIS_URL = os.getenv('REDIS_URL', 'redis://redis:6379')

# Initialize Redis connection
redis_client = redis.from_url(REDIS_URL)

def get_db_connection():
    """Get database connection"""
    try:
        return psycopg2.connect(DATABASE_URL)
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        return None

def cache_response(key: str, data: dict, ttl: int = 300):
    """Cache response in Redis"""
    try:
        redis_client.setex(key, ttl, json.dumps(data, default=str))
    except Exception as e:
        logger.error(f"Cache error: {e}")

def get_cached_response(key: str) -> Optional[dict]:
    """Get cached response from Redis"""
    try:
        cached = redis_client.get(key)
        if cached:
            return json.loads(cached)
    except Exception as e:
        logger.error(f"Cache retrieval error: {e}")
    return None

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
        # Check database
        conn = get_db_connection()
        if conn:
            conn.close()
            db_status = "healthy"
        else:
            db_status = "unhealthy"
        
        # Check Redis
        try:
            redis_client.ping()
            cache_status = "healthy"
        except:
            cache_status = "unhealthy"
        
        return jsonify({
            "status": "healthy" if db_status == "healthy" and cache_status == "healthy" else "unhealthy",
            "database": db_status,
            "cache": cache_status,
            "timestamp": datetime.now().isoformat()
        })
    
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/incidents', methods=['GET'])
def get_incidents():
    """Get traffic incidents with optional filtering"""
    try:
        # Check cache first
        cache_key = f"incidents:{request.query_string.decode()}"
        cached_response = get_cached_response(cache_key)
        if cached_response:
            return jsonify(cached_response)
        
        # Parse query parameters
        comuna = request.args.get('comuna')
        incident_type = request.args.get('type')
        limit = min(int(request.args.get('limit', 100)), 1000)
        offset = int(request.args.get('offset', 0))
        hours = int(request.args.get('hours', 24))  # Last N hours
        
        # Build query
        conditions = ["timestamp > %s"]
        params = [datetime.now() - timedelta(hours=hours)]
        
        if comuna:
            conditions.append("comuna = %s")
            params.append(comuna)
        
        if incident_type:
            conditions.append("incident_type = %s")
            params.append(incident_type)
        
        where_clause = " AND ".join(conditions)
        
        conn = get_db_connection()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500
        
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = f"""
                    SELECT incident_id, incident_type, description, latitude, longitude,
                           street, city, comuna, severity, timestamp, source
                    FROM traffic_incidents
                    WHERE {where_clause}
                    ORDER BY timestamp DESC
                    LIMIT %s OFFSET %s
                """
                
                cursor.execute(query, params + [limit, offset])
                incidents = cursor.fetchall()
                
                # Get total count
                count_query = f"SELECT COUNT(*) FROM traffic_incidents WHERE {where_clause}"
                cursor.execute(count_query, params)
                total_count = cursor.fetchone()['count']
                
                response = {
                    "incidents": [dict(incident) for incident in incidents],
                    "total": total_count,
                    "limit": limit,
                    "offset": offset,
                    "timestamp": datetime.now().isoformat()
                }
                
                # Cache response
                cache_response(cache_key, response, 300)  # 5 minutes
                
                return jsonify(response)
        
        finally:
            conn.close()
    
    except Exception as e:
        logger.error(f"Error in get_incidents: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/analytics/comuna', methods=['GET'])
def get_comuna_analytics():
    """Get analytics by comuna"""
    try:
        cache_key = "analytics:comuna"
        cached_response = get_cached_response(cache_key)
        if cached_response:
            return jsonify(cached_response)
        
        conn = get_db_connection()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500
        
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT 
                        comuna,
                        COUNT(*) as total_incidents,
                        AVG(severity) as avg_severity,
                        COUNT(CASE WHEN severity >= 7 THEN 1 END) as critical_incidents,
                        COUNT(CASE WHEN incident_type = 'ACCIDENTE' THEN 1 END) as accidents,
                        COUNT(CASE WHEN incident_type = 'ATASCO' THEN 1 END) as traffic_jams,
                        COUNT(CASE WHEN incident_type = 'CONSTRUCCION' THEN 1 END) as construction,
                        MAX(timestamp) as last_incident
                    FROM traffic_incidents
                    WHERE timestamp > NOW() - INTERVAL '7 days'
                    GROUP BY comuna
                    ORDER BY total_incidents DESC
                """)
                
                results = cursor.fetchall()
                
                response = {
                    "comuna_analytics": [dict(row) for row in results],
                    "generated_at": datetime.now().isoformat()
                }
                
                cache_response(cache_key, response, 600)  # 10 minutes
                return jsonify(response)
        
        finally:
            conn.close()
    
    except Exception as e:
        logger.error(f"Error in get_comuna_analytics: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/analytics/temporal', methods=['GET'])
def get_temporal_analytics():
    """Get temporal analytics (hourly patterns)"""
    try:
        cache_key = "analytics:temporal"
        cached_response = get_cached_response(cache_key)
        if cached_response:
            return jsonify(cached_response)
        
        conn = get_db_connection()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500
        
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT 
                        EXTRACT(hour FROM timestamp) as hour,
                        COUNT(*) as incident_count,
                        AVG(severity) as avg_severity,
                        COUNT(CASE WHEN severity >= 7 THEN 1 END) as critical_count
                    FROM traffic_incidents
                    WHERE timestamp > NOW() - INTERVAL '7 days'
                    GROUP BY EXTRACT(hour FROM timestamp)
                    ORDER BY hour
                """)
                
                hourly_data = cursor.fetchall()
                
                cursor.execute("""
                    SELECT 
                        EXTRACT(dow FROM timestamp) as day_of_week,
                        COUNT(*) as incident_count,
                        AVG(severity) as avg_severity
                    FROM traffic_incidents
                    WHERE timestamp > NOW() - INTERVAL '7 days'
                    GROUP BY EXTRACT(dow FROM timestamp)
                    ORDER BY day_of_week
                """)
                
                daily_data = cursor.fetchall()
                
                response = {
                    "hourly_patterns": [dict(row) for row in hourly_data],
                    "daily_patterns": [dict(row) for row in daily_data],
                    "generated_at": datetime.now().isoformat()
                }
                
                cache_response(cache_key, response, 600)
                return jsonify(response)
        
        finally:
            conn.close()
    
    except Exception as e:
        logger.error(f"Error in get_temporal_analytics: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/analytics/hotspots', methods=['GET'])
def get_hotspots():
    """Get traffic hotspots"""
    try:
        cache_key = "analytics:hotspots"
        cached_response = get_cached_response(cache_key)
        if cached_response:
            return jsonify(cached_response)
        
        conn = get_db_connection()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500
        
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT 
                        ROUND(latitude::numeric, 3) as latitude,
                        ROUND(longitude::numeric, 3) as longitude,
                        COUNT(*) as incident_count,
                        AVG(severity) as avg_severity,
                        MAX(severity) as max_severity,
                        array_agg(DISTINCT incident_type) as incident_types,
                        array_agg(DISTINCT comuna) as comunas,
                        MAX(timestamp) as last_incident
                    FROM traffic_incidents
                    WHERE timestamp > NOW() - INTERVAL '7 days'
                    GROUP BY ROUND(latitude::numeric, 3), ROUND(longitude::numeric, 3)
                    HAVING COUNT(*) >= 3
                    ORDER BY incident_count DESC
                    LIMIT 50
                """)
                
                hotspots = cursor.fetchall()
                
                response = {
                    "hotspots": [dict(row) for row in hotspots],
                    "generated_at": datetime.now().isoformat()
                }
                
                cache_response(cache_key, response, 600)
                return jsonify(response)
        
        finally:
            conn.close()
    
    except Exception as e:
        logger.error(f"Error in get_hotspots: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/analytics/types', methods=['GET'])
def get_type_analytics():
    """Get incident type analytics"""
    try:
        cache_key = "analytics:types"
        cached_response = get_cached_response(cache_key)
        if cached_response:
            return jsonify(cached_response)
        
        conn = get_db_connection()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500
        
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT 
                        incident_type,
                        COUNT(*) as frequency,
                        AVG(severity) as avg_severity,
                        COUNT(CASE WHEN severity >= 7 THEN 1 END) as critical_count,
                        COUNT(DISTINCT comuna) as affected_comunas,
                        MAX(timestamp) as last_occurrence
                    FROM traffic_incidents
                    WHERE timestamp > NOW() - INTERVAL '7 days'
                    GROUP BY incident_type
                    ORDER BY frequency DESC
                """)
                
                results = cursor.fetchall()
                
                response = {
                    "type_analytics": [dict(row) for row in results],
                    "generated_at": datetime.now().isoformat()
                }
                
                cache_response(cache_key, response, 600)
                return jsonify(response)
        
        finally:
            conn.close()
    
    except Exception as e:
        logger.error(f"Error in get_type_analytics: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/analytics/summary', methods=['GET'])
def get_summary_analytics():
    """Get summary analytics dashboard"""
    try:
        cache_key = "analytics:summary"
        cached_response = get_cached_response(cache_key)
        if cached_response:
            return jsonify(cached_response)
        
        conn = get_db_connection()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500
        
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Overall statistics
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_incidents,
                        COUNT(CASE WHEN processed = TRUE THEN 1 END) as processed_incidents,
                        AVG(severity) as avg_severity,
                        COUNT(CASE WHEN severity >= 7 THEN 1 END) as critical_incidents,
                        COUNT(DISTINCT comuna) as affected_comunas,
                        COUNT(DISTINCT incident_type) as incident_types,
                        MAX(timestamp) as last_updated
                    FROM traffic_incidents
                    WHERE timestamp > NOW() - INTERVAL '24 hours'
                """)
                
                overall_stats = cursor.fetchone()
                
                # Top comunas by incidents (last 24h)
                cursor.execute("""
                    SELECT comuna, COUNT(*) as incident_count
                    FROM traffic_incidents
                    WHERE timestamp > NOW() - INTERVAL '24 hours'
                    GROUP BY comuna
                    ORDER BY incident_count DESC
                    LIMIT 5
                """)
                
                top_comunas = cursor.fetchall()
                
                # Recent critical incidents
                cursor.execute("""
                    SELECT incident_type, comuna, severity, timestamp, description
                    FROM traffic_incidents
                    WHERE severity >= 7 AND timestamp > NOW() - INTERVAL '6 hours'
                    ORDER BY timestamp DESC
                    LIMIT 10
                """)
                
                critical_incidents = cursor.fetchall()
                
                # System performance metrics
                try:
                    processing_report = redis_client.get("processing:last_report")
                    if processing_report:
                        processing_metrics = json.loads(processing_report)
                    else:
                        processing_metrics = {}
                except:
                    processing_metrics = {}
                
                response = {
                    "overall_statistics": dict(overall_stats),
                    "top_comunas_24h": [dict(row) for row in top_comunas],
                    "recent_critical_incidents": [dict(row) for row in critical_incidents],
                    "system_performance": processing_metrics,
                    "cache_status": {
                        "total_keys": len(redis_client.keys("*")),
                        "analysis_keys": len(redis_client.keys("analysis:*"))
                    },
                    "generated_at": datetime.now().isoformat()
                }
                
                cache_response(cache_key, response, 300)  # 5 minutes
                return jsonify(response)
        
        finally:
            conn.close()
    
    except Exception as e:
        logger.error(f"Error in get_summary_analytics: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/pig/results', methods=['GET'])
def get_pig_results():
    """Get results from Apache Pig analysis"""
    try:
        cache_key = "pig:results"
        cached_response = get_cached_response(cache_key)
        if cached_response:
            return jsonify(cached_response)
        
        results = {}
        
        # Try to get cached Pig results from Redis
        pig_keys = [
            "analysis:comuna_counts",
            "analysis:type_counts", 
            "analysis:hourly_counts",
            "analysis:hotspots"
        ]
        
        for key in pig_keys:
            try:
                data = redis_client.get(key)
                if data:
                    # Parse CSV-like data
                    lines = data.decode().strip().split('\n')
                    parsed_data = []
                    for line in lines:
                        if line:
                            parsed_data.append(line.split(','))
                    results[key.split(':')[1]] = parsed_data
            except Exception as e:
                logger.error(f"Error parsing {key}: {e}")
                results[key.split(':')[1]] = []
        
        response = {
            "pig_analysis_results": results,
            "generated_at": datetime.now().isoformat(),
            "note": "Results from Apache Pig distributed processing"
        }
        
        cache_response(cache_key, response, 900)  # 15 minutes
        return jsonify(response)
    
    except Exception as e:
        logger.error(f"Error in get_pig_results: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/system/metrics', methods=['GET'])
def get_system_metrics():
    """Get system performance metrics"""
    try:
        cache_key = "metrics:system"
        cached_response = get_cached_response(cache_key)
        if cached_response:
            return jsonify(cached_response)
        
        # Database metrics
        conn = get_db_connection()
        db_metrics = {}
        
        if conn:
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    # Table sizes
                    cursor.execute("""
                        SELECT 
                            schemaname,
                            tablename,
                            pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
                        FROM pg_tables 
                        WHERE schemaname = 'public'
                    """)
                    table_sizes = cursor.fetchall()
                    
                    # Database size
                    cursor.execute("SELECT pg_size_pretty(pg_database_size('traffic_db')) as db_size")
                    db_size = cursor.fetchone()
                    
                    db_metrics = {
                        "table_sizes": [dict(row) for row in table_sizes],
                        "database_size": dict(db_size)
                    }
            finally:
                conn.close()
        
        # Redis metrics
        redis_metrics = {}
        try:
            info = redis_client.info()
            redis_metrics = {
                "used_memory": info.get('used_memory_human'),
                "connected_clients": info.get('connected_clients'),
                "total_commands_processed": info.get('total_commands_processed'),
                "keyspace_hits": info.get('keyspace_hits', 0),
                "keyspace_misses": info.get('keyspace_misses', 0)
            }
            
            # Calculate hit rate
            hits = redis_metrics['keyspace_hits']
            misses = redis_metrics['keyspace_misses']
            if hits + misses > 0:
                redis_metrics['hit_rate'] = hits / (hits + misses) * 100
            else:
                redis_metrics['hit_rate'] = 0
        except:
            redis_metrics = {"error": "Redis metrics unavailable"}
        
        response = {
            "database_metrics": db_metrics,
            "cache_metrics": redis_metrics,
            "system_uptime": datetime.now().isoformat(),
            "generated_at": datetime.now().isoformat()
        }
        
        cache_response(cache_key, response, 300)
        return jsonify(response)
    
    except Exception as e:
        logger.error(f"Error in get_system_metrics: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/trigger/scraping', methods=['POST'])
def trigger_scraping():
    """Trigger manual scraping (for testing)"""
    try:
        # This would typically trigger the scraper service
        # For now, we'll just return a success message
        
        # You could implement this by:
        # 1. Sending a message to a queue
        # 2. Making an HTTP call to the scraper service
        # 3. Setting a flag in Redis that the scraper checks
        
        redis_client.setex("trigger:scraping", 300, "requested")
        
        return jsonify({
            "message": "Scraping triggered successfully",
            "timestamp": datetime.now().isoformat()
        })
    
    except Exception as e:
        logger.error(f"Error triggering scraping: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/trigger/processing', methods=['POST'])
def trigger_processing():
    """Trigger manual data processing"""
    try:
        redis_client.setex("trigger:processing", 300, "requested")
        
        return jsonify({
            "message": "Data processing triggered successfully",
            "timestamp": datetime.now().isoformat()
        })
    
    except Exception as e:
        logger.error(f"Error triggering processing: {e}")
        return jsonify({"error": str(e)}), 500

@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({"error": "Internal server error"}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)