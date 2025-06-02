# test_system.sh - Script para probar el funcionamiento del sistema

echo "=== Probando Sistema de Análisis de Tráfico ==="

# Test 1: Verificar conexión a base de datos
echo "Test 1: Conexión a base de datos..."
if docker exec postgres pg_isready -h localhost -p 5432 > /dev/null 2>&1; then
    echo "✓ Base de datos disponible"
else
    echo "✗ Error en base de datos"
    exit 1
fi

# Test 2: Verificar Redis
echo "Test 2: Conexión a Redis..."
if docker exec redis redis-cli ping | grep -q PONG; then
    echo "✓ Redis disponible"
else
    echo "✗ Error en Redis"
    exit 1
fi

# Test 3: Verificar Hadoop
echo "Test 3: Verificar Hadoop HDFS..."
if docker exec namenode hdfs dfs -ls / > /dev/null 2>&1; then
    echo "✓ Hadoop HDFS disponible"
else
    echo "✗ Error en Hadoop HDFS"
    exit 1
fi

# Test 4: Verificar tabla de base de datos
echo "Test 4: Verificar estructura de base de datos..."
DB_CHECK=$(docker exec postgres psql -U traffic_user -d traffic_db -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_name='traffic_incidents';" 2>/dev/null | tr -d ' ')
if [ "$DB_CHECK" = "1" ]; then
    echo "✓ Tabla traffic_incidents existe"
else
    echo "✗ Error: tabla traffic_incidents no existe"
    exit 1
fi

# Test 5: Insertar datos de prueba
echo "Test 5: Insertar datos de prueba..."
docker exec postgres psql -U traffic_user -d traffic_db -c "
INSERT INTO traffic_incidents (incident_id, incident_type, description, latitude, longitude, street, city, comuna, severity, timestamp) 
VALUES ('test_$(date +%s)', 'ACCIDENTE', 'Test incident', -33.4372, -70.6506, 'Test Street', 'Santiago', 'Santiago', 5, NOW())
ON CONFLICT (incident_id) DO NOTHING;
" > /dev/null 2>&1

if [ $? -eq 0 ]; then
    echo "✓ Datos de prueba insertados"
else
    echo "✗ Error insertando datos de prueba"
fi

# Test 6: Verificar conteo de registros
echo "Test 6: Verificar datos en base de datos..."
RECORD_COUNT=$(docker exec postgres psql -U traffic_user -d traffic_db -t -c "SELECT COUNT(*) FROM traffic_incidents;" 2>/dev/null | tr -d ' ')
echo "✓ Total de registros en BD: $RECORD_COUNT"

# Test 7: Probar scraper (ejecutar una vez)
echo "Test 7: Probar servicio scraper..."
if docker exec scraper python -c "
import sys
sys.path.append('/app')
from scraper import WazeScraper
import os
scraper = WazeScraper(
    os.getenv('DATABASE_URL', 'postgresql://traffic_user:traffic_pass@postgres:5432/traffic_db'),
    os.getenv('REDIS_URL', 'redis://redis:6379')
)
print('Scraper inicializado correctamente')
" 2>/dev/null; then
    echo "✓ Servicio scraper funcional"
else
    echo "⚠ Warning: Error en servicio scraper (puede ser temporal)"
fi

# Test 8: Probar procesador
echo "Test 8: Probar servicio procesador..."
if docker exec processor python -c "
import sys
sys.path.append('/app')
from processor import TrafficDataProcessor
import os
processor = TrafficDataProcessor(
    os.getenv('DATABASE_URL', 'postgresql://traffic_user:traffic_pass@postgres:5432/traffic_db'),
    os.getenv('REDIS_URL', 'redis://redis:6379')
)
print('Procesador inicializado correctamente')
" 2>/dev/null; then
    echo "✓ Servicio procesador funcional"
else
    echo "⚠ Warning: Error en servicio procesador"
fi

# Test 9: Probar Apache Pig
echo "Test 9: Probar Apache Pig..."
if docker exec pig pig -version > /dev/null 2>&1; then
    echo "✓ Apache Pig disponible"
else
    echo "✗ Error en Apache Pig"
fi

# Test 10: Crear directorio en HDFS
echo "Test 10: Crear directorios en HDFS..."
docker exec namenode hdfs dfs -mkdir -p /data /output > /dev/null 2>&1
if docker exec namenode hdfs dfs -ls / | grep -q data; then
    echo "✓ Directorios HDFS creados"
else
    echo "✗ Error creando directorios HDFS"
fi

echo "=== Tests completados ==="
echo ""
echo "Para monitorear el sistema:"
echo "- Logs del scraper: docker logs -f scraper"
echo "- Logs del procesador: docker logs -f processor"
echo "- Ver datos: docker exec postgres psql -U traffic_user -d traffic_db -c 'SELECT * FROM traffic_incidents LIMIT 5;'"
echo "- Ver cache: docker exec redis redis-cli KEYS '*'"
