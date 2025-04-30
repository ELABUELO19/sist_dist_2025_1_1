
# Plataforma de Análisis de Tráfico - Sistemas Distribuidos 2025-1

Este proyecto implementa una arquitectura distribuida para el monitoreo de eventos de tráfico en la Región Metropolitana, basada en datos colaborativos extraídos desde Waze.

## Video explicacion y procedimiento

- [Video explicacion](https://youtu.be/T7a9we1Xb00)

## Tecnologías Utilizadas

- [Python 3](https://www.python.org/)
- [MongoDB](https://www.mongodb.com/)
- [Docker](https://www.docker.com/)
- [Docker Compose](https://docs.docker.com/compose/)

## Estructura del Proyecto

- `scraper/`: extracción de eventos desde Waze.
- `generator/`: simulación de consultas a los eventos.
- `docker-compose.yml`: orquestación de servicios.

## Instalación y Ejecución

1. Clonar el repositorio:
```bash
git clone https://github.com/Shadower-Sama/sist_dist_2025_1_1.git
cd sist_dist_2025_1_1
```

2. Ejecutar el sistema:
```bash
docker-compose up --build
```

3. Para detener:
```bash
docker-compose down
```

## Configuración

Puedes modificar los siguientes parámetros:
- `CACHE_POLICY` y `CACHE_CAPACITY` en `cache_service/config.py`.
- Tipo de distribución en `traffic_generator/generator.py`.

## Autores
 Ignacio Gutierres y Matias Herrera.

