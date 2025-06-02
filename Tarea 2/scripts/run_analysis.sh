# run_analysis.sh - Script para ejecutar análisis completo

echo "=== Ejecutando Análisis Completo de Tráfico ==="

# Verificar que el sistema esté ejecutándose
if ! docker-compose ps | grep -q "Up"; then
    echo "Error: El sistema no está ejecutándose. Ejecutar ./start_system.sh primero"
    exit 1
fi

# 1. Ejecutar scraping manual
echo "Paso 1: Ejecutando scraping de datos..."
docker exec scraper python -c "
import sys
sys.path.append('/app')
from scraper import WazeScraper
import os

scraper = WazeScraper(
    os.getenv('DATABASE_URL'),
    os.getenv('REDIS_URL')
)
scraper.scrape_and_store()
scraper.export_to_csv()
print('Scraping completado')
"

# 2. Ejecutar procesamiento
echo "Paso 2: Ejecutando procesamiento de datos..."
docker exec processor python -c "
import sys
sys.path.append('/app')
from processor import TrafficDataProcessor
import os

processor = TrafficDataProcessor(
    os.getenv('DATABASE_URL'),
    os.getenv('REDIS_URL')
)
processor.process_data_pipeline()
print('Procesamiento completado')
"

# 3. Verificar datos disponibles
echo "Paso 3: Verificando datos disponibles..."
INCIDENT_COUNT=$(docker exec postgres psql -U traffic_user -d traffic_db -t -c "SELECT COUNT(*) FROM traffic_incidents WHERE processed = TRUE;" | tr -d ' ')
echo "Incidentes procesados disponibles: $INCIDENT_COUNT"

if [ "$INCIDENT_COUNT" -eq "0" ]; then
    echo "Warning: No hay datos procesados disponibles. Insertando datos de ejemplo..."
    
    # Insertar datos de ejemplo para demostración
    docker exec postgres psql -U traffic_user -d traffic_db -c "
    INSERT INTO traffic_incidents (incident_id, incident_type, description, latitude, longitude, street, city, comuna, severity, timestamp, processed) VALUES
    ('demo_001', 'ACCIDENTE', 'Accidente en Providencia', -33.4372, -70.6506, 'Av. Providencia', 'Santiago', 'Providencia', 7, NOW() - INTERVAL '1 hour', TRUE),
    ('demo_002', 'ATASCO', 'Congestión en Las Condes', -33.4025, -70.5693, 'Av. Las Condes', 'Santiago', 'Las Condes', 5, NOW() - INTERVAL '2 hours', TRUE),
    ('demo_003', 'CONSTRUCCION', 'Obras en Santiago Centro', -33.4488, -70.6693, 'Av. Libertador', 'Santiago', 'Santiago', 3, NOW() - INTERVAL '3 hours', TRUE),
    ('demo_004', 'ACCIDENTE', 'Choque en Ñuñoa', -33.4569, -70.6063, 'Av. Irarrázaval', 'Santiago', 'Ñuñoa', 8, NOW() - INTERVAL '30 minutes', TRUE),
    ('demo_005', 'ATASCO', 'Tráfico lento Maipú', -33.5111, -70.7581, 'Av. Pajaritos', 'Santiago', 'Maipú', 4, NOW() - INTERVAL '45 minutes', TRUE),
    ('demo_006', 'PELIGRO', 'Peligro en La Florida', -33.5183, -70.5928, 'Av. Vicuña Mackenna', 'Santiago', 'La Florida', 6, NOW() - INTERVAL '1.5 hours', TRUE),
    ('demo_007', 'ATASCO', 'Congestión Puente Alto', -33.6103, -70.5756, 'Av. Concha y Toro', 'Santiago', 'Puente Alto', 5, NOW() - INTERVAL '2.5 hours', TRUE),
    ('demo_008', 'ACCIDENTE', 'Accidente San Miguel', -33.4969, -70.6519, 'Gran Avenida', 'Santiago', 'San Miguel', 9, NOW() - INTERVAL '20 minutes', TRUE),
    ('demo_009', 'CONSTRUCCION', 'Obras Quilicura', -33.3606, -70.7394, 'Av. Matta Sur', 'Santiago', 'Quilicura', 2, NOW() - INTERVAL '4 hours', TRUE),
    ('demo_010', 'CLIMA', 'Lluvia afecta visibilidad', -33.4264, -70.6111, 'Av. Manuel Montt', 'Santiago', 'Providencia', 4, NOW() - INTERVAL '10 minutes', TRUE)
    ON CONFLICT (incident_id) DO NOTHING;
    "
    
    echo "Datos de ejemplo insertados"
fi

# 4. Exportar datos para Pig
echo "Paso 4: Exportando datos para Apache Pig..."
docker exec processor python -c "
import sys
sys.path.append('/app')
from processor import TrafficDataProcessor
import os

processor = TrafficDataProcessor(
    os.getenv('DATABASE_URL'),
    os.getenv('REDIS_URL')
)
csv_path = processor.export_for_pig_processing()
print(f'Datos exportados a: {csv_path}')
"

# 5. Copiar datos a HDFS
echo "Paso 5: Copiando datos a HDFS..."
docker exec namenode hdfs dfs -rm -r /data/* 2>/dev/null || true
docker exec namenode hdfs dfs -put /data/*.csv /data/ 2>/dev/null || echo "No hay archivos CSV para copiar"

# Verificar archivos en HDFS
echo "Archivos en HDFS:"
docker exec namenode hdfs dfs -ls /data/

# 6. Ejecutar análisis con Apache Pig
echo "Paso 6: Ejecutando análisis con Apache Pig..."
docker exec pig pig -f /pig_scripts/traffic_analysis.pig

# 7. Verificar resultados
echo "Paso 7: Verificando resultados del análisis..."

echo "Archivos de salida generados:"
docker exec namenode hdfs dfs -ls /output/ 2>/dev/null || echo "No se encontraron archivos de salida"

# Mostrar algunos resultados si existen
echo ""
echo "=== RESULTADOS DEL ANÁLISIS ==="

echo "1. Incidentes por Comuna:"
docker exec namenode hdfs dfs -cat /output/incidents_by_comuna/part-r-00000 2>/dev/null | head -10 || echo "No disponible"

echo ""
echo "2. Incidentes por Tipo:"
docker exec namenode hdfs dfs -cat /output/incidents_by_type/part-r-00000 2>/dev/null | head -10 || echo "No disponible"

echo ""
echo "3. Análisis Temporal (por hora):"
docker exec namenode hdfs dfs -cat /output/incidents_by_hour/part-r-00000 2>/dev/null | head -10 || echo "No disponible"

echo ""
echo "4. Hotspots de Tráfico:"
docker exec namenode hdfs dfs -cat /output/traffic_hotspots/part-r-00000 2>/dev/null | head -5 || echo "No disponible"

# 8. Copiar resultados a directorio local
echo "Paso 8: Copiando resultados a directorio local..."
mkdir -p output/results
docker exec namenode hdfs dfs -get /output/* /output/ 2>/dev/null || echo "Error copiando resultados"

# 9. Actualizar caché
echo "Paso 9: Actualizando caché Redis..."
docker exec processor python -c "
import sys
sys.path.append('/app')
from processor import TrafficDataProcessor
import os

processor = TrafficDataProcessor(
    os.getenv('DATABASE_URL'),
    os.getenv('REDIS_URL')
)
processor.cache_analysis_results()
print('Caché actualizado')
"

# 10. Mostrar estadísticas finales
echo ""
echo "=== ESTADÍSTICAS FINALES ==="

# Estadísticas de base de datos
echo "Base de datos:"
docker exec postgres psql -U traffic_user -d traffic_db -c "
SELECT 
    COUNT(*) as total_incidents,
    COUNT(CASE WHEN processed = TRUE THEN 1 END) as processed_incidents,
    COUNT(DISTINCT comuna) as comunas_affected,
    COUNT(DISTINCT incident_type) as incident_types
FROM traffic_incidents;
"

# Estadísticas de caché
echo ""
echo "Caché Redis:"
docker exec redis redis-cli KEYS "analysis:*" | wc -l | xargs echo "Claves de análisis en caché:"
docker exec redis redis-cli KEYS "analysis:*"

echo ""
echo "=== ANÁLISIS COMPLETADO ==="
echo ""
echo "Para acceder a los resultados:"
echo "- Archivos HDFS: docker exec namenode hdfs dfs -ls /output/"
echo "- Resultados locales: ls -la output/"
echo "- Caché Redis: docker exec redis redis-cli KEYS 'analysis:*'"
echo "- Base de datos: docker exec postgres psql -U traffic_user -d traffic_db"
echo ""
echo "Interfaces web:"
echo "- Hadoop NameNode: http://localhost:9870"
echo "- Yarn ResourceManager: http://localhost:8088"
