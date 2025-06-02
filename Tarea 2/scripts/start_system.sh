#!/bin/bash
# start_system.sh - Script para inicializar el sistema completo

echo "=== Iniciando Sistema de Análisis de Tráfico ==="

# Verificar que Docker esté ejecutándose
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker no está ejecutándose"
    exit 1
fi

# Crear directorios necesarios
echo "Creando directorios..."
mkdir -p data output pig_scripts pig/hadoop-conf

# Copiar configuraciones de Hadoop
echo "Copiando configuraciones de Hadoop..."
cat > pig/hadoop-conf/core-site.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://namenode:9000</value>
    </property>
</configuration>
EOF

cat > pig/hadoop-conf/hdfs-site.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>dfs.webhdfs.enabled</name>
        <value>true</value>
    </property>
    <property>
        <name>dfs.permissions.enabled</name>
        <value>false</value>
    </property>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
</configuration>
EOF

# Iniciar servicios
echo "Iniciando servicios Docker..."
docker-compose down -v
docker-compose up -d

# Esperar a que los servicios estén listos
echo "Esperando a que los servicios estén listos..."
sleep 30

# Verificar servicios
echo "Verificando servicios..."
docker-compose ps

# Esperar a que Hadoop esté listo
echo "Esperando a que Hadoop esté listo..."
while ! docker exec namenode hdfs dfsadmin -report > /dev/null 2>&1; do
    echo "Esperando Hadoop..."
    sleep 10
done

echo "=== Sistema iniciado correctamente ==="
echo "Hadoop NameNode: http://localhost:9870"
echo "Yarn ResourceManager: http://localhost:8088"
echo "Base de datos PostgreSQL: localhost:5432"
echo "Redis: localhost:6379"

# Ejecutar test inicial
echo "Ejecutando test inicial..."
./test_system.sh