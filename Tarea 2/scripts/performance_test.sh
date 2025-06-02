# performance_test.sh - Script para probar rendimiento

echo "=== Test de Rendimiento del Sistema ==="

# Función para medir tiempo
measure_time() {
    local start_time=$(date +%s.%N)
    eval "$1"
    local end_time=$(date +%s.%N)
    local duration=$(echo "$end_time - $start_time" | bc)
    echo "Tiempo transcurrido: ${duration} segundos"
}

# Test 1: Inserción masiva de datos
echo "Test 1: Inserción masiva de datos (1000 registros)..."
measure_time "docker exec postgres psql -U traffic_user -d traffic_db -c \"
WITH RECURSIVE generate_data AS (
    SELECT 
        1 as n,
        'perf_test_' || 1 as incident_id,
        (ARRAY['ACCIDENTE', 'ATASCO', 'CONSTRUCCION', 'PELIGRO'])[1 + (random() * 3)::int] as incident_type,
        'Performance test incident ' || 1 as description,
        -33.4 + (random() * 0.6 - 0.3) as latitude,
        -70.6 + (random() * 0.4 - 0.2) as longitude,
        'Test Street ' || 1 as street,
        'Santiago' as city,
        (ARRAY['Santiago', 'Providencia', 'Las Condes', 'Ñuñoa', 'Maipú'])[1 + (random() * 4)::int] as comuna,
        (random() * 10)::int as severity,
        NOW() - (random() * INTERVAL '24 hours') as timestamp
    UNION ALL
    SELECT 
        n + 1,
        'perf_test_' || (n + 1),
        (ARRAY['ACCIDENTE', 'ATASCO', 'CONSTRUCCION', 'PELIGRO'])[(n % 4) + 1],
        'Performance test incident ' || (n + 1),
        -33.4 + (random() * 0.6 - 0.3),
        -70.6 + (random() * 0.4 - 0.2),
        'Test Street ' || (n + 1),
        'Santiago',
        (ARRAY['Santiago', 'Providencia', 'Las Condes', 'Ñuñoa', 'Maipú'])[(n % 5) + 1],
        (random() * 10)::int,
        NOW() - (random() * INTERVAL '24 hours')
    FROM generate_data
    WHERE n < 1000
)
INSERT INTO traffic_incidents (incident_id, incident_type, description, latitude, longitude, street, city, comuna, severity, timestamp)
SELECT incident_id, incident_type, description, latitude, longitude, street, city, comuna, severity, timestamp
FROM generate_data
ON CONFLICT (incident_id) DO NOTHING;
\""

# Test 2: Procesamiento de datos
echo ""
echo "Test 2: Procesamiento de datos..."
measure_time "docker exec processor python -c \"
import sys
sys.path.append('/app')
from processor import TrafficDataProcessor
import os

processor = TrafficDataProcessor(
    os.getenv('DATABASE_URL'),
    os.getenv('REDIS_URL')
)
total, filtered = processor.filter_and_clean_data()
print(f'Procesados: {filtered} de {total} registros')
\""

# Test 3: Exportación CSV
echo ""
echo "Test 3: Exportación a CSV..."
measure_time "docker exec processor python -c \"
import sys
sys.path.append('/app')
from processor import TrafficDataProcessor
import os

processor = TrafficDataProcessor(
    os.getenv('DATABASE_URL'),
    os.getenv('REDIS_URL')
)
csv_path = processor.export_for_pig_processing()
print(f'Exportado a: {csv_path}')
\""

# Test 4: Consultas de base de datos
echo ""
echo "Test 4: Consultas complejas en base de datos..."
measure_time "docker exec postgres psql -U traffic_user -d traffic_db -c \"
SELECT 
    comuna,
    incident_type,
    COUNT(*) as count,
    AVG(severity) as avg_severity,
    EXTRACT(hour FROM timestamp) as hour
FROM traffic_incidents 
WHERE timestamp > NOW() - INTERVAL '24 hours'
GROUP BY comuna, incident_type, EXTRACT(hour FROM timestamp)
ORDER BY count DESC
LIMIT 50;
\""

# Test 5: Operaciones de caché
echo ""
echo "Test 5: Operaciones de caché Redis..."
measure_time "
for i in {1..1000}; do
    docker exec redis redis-cli SET test_key_\$i \"test_value_\$i\" > /dev/null
done
echo 'Escritura de 1000 claves completada'

for i in {1..1000}; do
    docker exec redis redis-cli GET test_key_\$i > /dev/null
done
echo 'Lectura de 1000 claves completada'

docker exec redis redis-cli DEL \$(docker exec redis redis-cli KEYS 'test_key_*' | tr '\n' ' ') > /dev/null
echo 'Limpieza completada'
"

# Estadísticas finales
echo ""
echo "=== Estadísticas de Rendimiento ==="
echo "Total de incidentes en BD:"
docker exec postgres psql -U traffic_user -d traffic_db -t -c "SELECT COUNT(*) FROM traffic_incidents;" | tr -d ' '

echo "Incidentes procesados:"
docker exec postgres psql -U traffic_user -d traffic_db -t -c "SELECT COUNT(*) FROM traffic_incidents WHERE processed = TRUE;" | tr -d ' '

echo "Uso de memoria Redis:"
docker exec redis redis-cli info memory | grep used_memory_human

echo "Espacio usado en HDFS:"
docker exec namenode hdfs dfs -df -h /

echo ""
echo "Test de rendimiento completado"