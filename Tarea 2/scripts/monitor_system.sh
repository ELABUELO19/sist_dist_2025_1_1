# monitor_system.sh - Script para monitorear el sistema

echo "=== Monitor del Sistema de Análisis de Tráfico ==="

while true; do
    clear
    echo "=== Estado del Sistema - $(date) ==="
    echo ""
    
    # Estado de contenedores
    echo "Estado de Contenedores:"
    docker-compose ps
    echo ""
    
    # Estadísticas de base de datos
    echo "Estadísticas de Base de Datos:"
    docker exec postgres psql -U traffic_user -d traffic_db -c "
    SELECT 
        'Total Incidentes' as metric, COUNT(*)::text as value FROM traffic_incidents
    UNION ALL
    SELECT 
        'Incidentes Procesados' as metric, COUNT(*)::text as value FROM traffic_incidents WHERE processed = TRUE
    UNION ALL
    SELECT 
        'Último Incidente' as metric, MAX(timestamp)::text as value FROM traffic_incidents
    UNION ALL
    SELECT 
        'Comunas Activas' as metric, COUNT(DISTINCT comuna)::text as value FROM traffic_incidents WHERE processed = TRUE;
    " 2>/dev/null || echo "Error accediendo a la base de datos"
    echo ""
    
    # Estadísticas de HDFS
    echo "Estadísticas de HDFS:"
    docker exec namenode hdfs dfs -df -h / 2>/dev/null || echo "Error accediendo a HDFS"
    echo ""
    
    # Archivos en proceso
    echo "Archivos de Datos:"
    echo "- CSV locales: $(ls data/*.csv 2>/dev/null | wc -l) archivos"
    echo "- Archivos HDFS /data: $(docker exec namenode hdfs dfs -ls /data/ 2>/dev/null | grep -c "^-" || echo "0") archivos"
    echo "- Resultados HDFS /output: $(docker exec namenode hdfs dfs -ls /output/ 2>/dev/null | grep -c "^d" || echo "0") directorios"
    echo ""
    
    # Estado de caché
    echo "Estado de Caché Redis:"
    docker exec redis redis-cli info memory 2>/dev/null | grep used_memory_human || echo "Error accediendo a Redis"
    echo "Claves de análisis: $(docker exec redis redis-cli KEYS 'analysis:*' 2>/dev/null | wc -l) claves"
    echo ""
    
    # Uso de recursos
    echo "Uso de Recursos:"
    echo "Memoria total usada por contenedores:"
    docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}" 2>/dev/null | head -8
    echo ""
    
    echo "Presiona Ctrl+C para salir, Enter para actualizar..."
    read -t 10
done
