# cleanup.sh - Script para limpiar el sistema

echo "=== Limpieza del Sistema ==="

read -p "¿Estás seguro de que quieres limpiar el sistema? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    exit 1
fi

echo "Deteniendo contenedores..."
docker-compose down

echo "Eliminando volúmenes..."
docker-compose down -v

echo "Eliminando imágenes del proyecto..."
docker rmi $(docker images | grep -E "(scraper|processor|pig)" | awk '{print $3}') 2>/dev/null || echo "No hay imágenes del proyecto para eliminar"

echo "Limpiando archivos locales..."
rm -rf data/*.csv
rm -rf output/*
rm -rf pig/hadoop-conf/*

echo "Limpieza de Docker general..."
docker system prune -f

echo "Sistema limpiado completamente"
