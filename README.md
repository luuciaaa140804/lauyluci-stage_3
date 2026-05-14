# Stage 3 – Cluster Architecture

Motor de búsqueda distribuido sobre un clúster de contenedores Docker.

## Arquitectura

```
                    ┌──────────────────────────────────────────────┐
                    │            Docker Network (search_net)        │
                    │                                              │
  Cliente HTTP ────►│  [Nginx :80]  ←── Load Balancer (least_conn) │
                    │       ↓ ↓ ↓                                  │
                    │  [search1] [search2] [search3]               │
                    │       ↑       ↑       ↑                      │
                    │    Hazelcast MultiMap (índice en memoria)     │
                    │       ↑                                      │
                    │  [indexer1]  [indexer2]  ←── consumen broker │
                    │       ↑                                      │
                    │  [ActiveMQ :61616]  ←── Message Broker       │
                    │       ↑                                      │
                    │  [crawler1] [crawler2]  ←── publican eventos │
                    │                                              │
                    │  Volúmenes compartidos:                      │
                    │    datalake_data  → /datalake                │
                    │    datamarts_data → /datamarts               │
                    └──────────────────────────────────────────────┘
```

## Requisitos

- Docker Desktop (Windows/Mac) o Docker Engine (Linux)
- Al menos 4 GB de RAM disponibles para Docker

## Arrancar el clúster

```bash
# Construir imágenes y arrancar todos los contenedores
docker compose up -d --build

# Ver logs en tiempo real (todos los servicios)
docker compose logs -f

# Ver logs de un servicio concreto
docker compose logs -f indexer1
```

El primer arranque tarda ~3-5 minutos mientras Maven descarga dependencias.

## Comprobar que todo funciona

```bash
# Estado del clúster (a través del Control Module)
curl http://localhost:7000/cluster/status

# Estado del Load Balancer (responde uno de los 3 search nodes)
curl http://localhost:80/status

# Consola web de ActiveMQ
# http://localhost:8161  (usuario: admin, contraseña: admin)
```

## Realizar búsquedas

Las búsquedas van a través del Load Balancer (puerto 80).
El campo `served_by` indica qué nodo respondió:

```bash
# Búsqueda simple
curl "http://localhost:80/search?q=love"

# Con filtro de autor
curl "http://localhost:80/search?q=love&author=Austen"

# Con filtro de idioma
curl "http://localhost:80/search?q=baleine&language=fr"

# Combinado
curl "http://localhost:80/search?q=whale&author=Melville"
```

En PowerShell:
```powershell
Invoke-RestMethod -Uri "http://localhost:80/search?q=love&author=Austen"
Invoke-RestMethod -Uri "http://localhost:80/search?q=whale"
```

## Benchmarking

### Test de balanceo de carga (ver qué nodo responde)

```bash
for i in {1..9}; do
  curl -s "http://localhost:80/search?q=love" | python3 -c "import sys,json; print(json.load(sys.stdin)['served_by'])"
done
```

En PowerShell:
```powershell
1..9 | ForEach-Object {
  (Invoke-RestMethod -Uri "http://localhost:80/search?q=love").served_by
}
```

### Test de fault tolerance (caída de un nodo)

```powershell
# 1. Hacer búsquedas y ver qué nodos responden
1..5 | ForEach-Object { (Invoke-RestMethod "http://localhost:80/search?q=love").served_by }

# 2. Derribar search2
docker stop search2

# 3. Volver a buscar -> solo responden search1 y search3
1..5 | ForEach-Object { (Invoke-RestMethod "http://localhost:80/search?q=love").served_by }

# 4. Recuperar el nodo
docker start search2

# 5. Después de ~30s, search2 vuelve al pool automáticamente
1..5 | ForEach-Object { (Invoke-RestMethod "http://localhost:80/search?q=love").served_by }
```

### Test de escalado horizontal (añadir un 4º nodo de búsqueda)

```powershell
# Añadir search4 al clúster
docker compose up -d --scale search-service=4

# Recargar configuración de Nginx (si es necesario)
docker exec load_balancer nginx -s reload
```

### Test de latencia con múltiples queries concurrentes

```powershell
# Medir latencia de 10 búsquedas
$times = 1..10 | ForEach-Object {
  $sw = [Diagnostics.Stopwatch]::StartNew()
  Invoke-RestMethod "http://localhost:80/search?q=love" | Out-Null
  $sw.ElapsedMilliseconds
}
$avg = ($times | Measure-Object -Average).Average
Write-Host "Latencia media: $avg ms"
```

## Parar el clúster

```bash
# Parar todos los contenedores (mantiene los volúmenes)
docker compose down

# Parar Y borrar volúmenes (limpieza total)
docker compose down -v
```

## Componentes y puertos

| Contenedor | Puerto externo | Descripción |
|---|---|---|
| nginx | 80 | Load Balancer |
| control | 7000 | API de administración |
| activemq | 61616 / 8161 | Message Broker / Consola web |
| search1/2/3 | — (interno 8080) | Search Services |
| indexer1/2 | — (interno) | Indexing Services |
| crawler1/2 | — (interno) | Crawler Services |

## YouTube Demo

[Enlace al vídeo de demostración]
