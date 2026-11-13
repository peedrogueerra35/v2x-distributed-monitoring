# RSU D2 - Persona B (compatible con main PR #15)

En este repositorio existen **dos** archivos Compose:

- **Local**: `services/rsu/compose.yml`; dos proyectos (`v2x-rsu1`, `v2x-rsu2`), puertos 1883/8090 y 1884/8091.
- **AWS**: `infra/deploy/compose.rsu.yml`; un proyecto `v2x` por EC2, puertos 1883/8090, imagen `${RSU_IMAGE}` desde ECR. Lo lanza `infra/deploy/deploy.sh` de Persona A.

Para ambos casos los usuarios MQTT **son `sim` y `rsu`**, no `vehicle` ni `rsu-app`. `MQTT_VEHICLE_PASSWORD` y `MQTT_APP_PASSWORD` son secretos, jamás versionarlos.

## 1. Pruebas unitarias sin AWS

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install pytest jsonschema -r simulator/requirements.txt -r services/rsu/requirements.txt
pytest -q
python infra/scripts/offline_evidence.py
```

## 2. Dos RSU locales con Docker

```bash
cp services/rsu/.env.example services/rsu/.env
# Editar contraseñas y METRICS_TOKEN; no mantener CHANGE_ME_
./infra/scripts/local_rsus.sh up
curl -i http://127.0.0.1:8090/health
curl -i http://127.0.0.1:8091/health
set -a; source services/rsu/.env; set +a
python infra/scripts/smoke_edge.py
```

**PASS:** ambas respuestas `/health` son HTTP 200 y `smoke_edge.py` recibe una alerta local `collision_risk` de cada RSU. Sin credenciales AWS, la salida SQLite puede acumular eventos: el estado saludable solo comprueba Mosquitto, no SQS.

## 3. Simulación MQTT completa

```bash
python -m simulator --transport mqtt --duration 120 \
  --mqtt-rsu-1-host 127.0.0.1 --mqtt-rsu-1-port 1883 \
  --mqtt-rsu-2-host 127.0.0.1 --mqtt-rsu-2-port 1884 \
  --scenario simulator/scenarios/accident.yaml
```

Se generan `runs/<id>/vehicle_alerts.jsonl`, `summary.json`, `groundtruth.jsonl`, etc. Ejecutar en **tiempo real**: `--fast` es solo para simulaciones offline, no para usar conjuntamente con MQTT y las ventanas por tiempo real.

## 4. Métricas y E1

```bash
curl -sS -H "X-Metrics-Token: $METRICS_TOKEN" http://127.0.0.1:8090/metrics
python infra/scripts/edge_metrics.py runs/<run_id> \
  --rsu1 http://127.0.0.1:8090 --rsu2 http://127.0.0.1:8091 --token "$METRICS_TOKEN"
# Con simulador MQTT activo, iniciar en otra terminal:
python experiments/experiment_e1.py --mode local --container v2x-rsu1-rsu-1 \
  --rsu-url http://127.0.0.1:8090 --token "$METRICS_TOKEN" \
  --repetitions 5 --outage-s 20
```

E1 suspende **el emisor SQS de la aplicación**, no corta físicamente la red. Para SSM sobre EC2 usar `--mode ssm --instance-id i-... --rsu-url http://IP:8090`. En EC2 el proyecto Compose es `v2x` y el servicio `rsu`.

Para bajar el entorno local:

```bash
./infra/scripts/local_rsus.sh down
```

## 5. Detalles técnicos

- Suscripción MQTT persistente QoS1; validación de esquema y tema MQTT, deduplicación `event_id`, ACK solo después del commit de SQLite.
- Ventanas *tumbling* event-time de 5 s por segmento, 2 s de tolerancia; `vehicle_count`, `mean_speed_mps`, `sample_count`, ocupación aproximada. Las posiciones no se reenvían individualmente a SQS.
- Alertas locales TTC menor que 2 s entre vehículos del mismo carril; enfriamiento de 15 s. Es riesgo de seguimiento, **no** detección general de intersecciones perpendiculares.
- Eventos críticos se encolan de inmediato con prioridad mayor que `density`.
- Buffer SQLite WAL con backoff exponencial a SQS y borrado solo tras `SendMessage` satisfactorio. Con reintentos puede haber duplicados, deduplicados por el cloud mediante `event_id`.
- SNS firmado: URL regional restringida, comprobación de firma con certificado X.509, validación del esquema `v2x-alert.schema.json`, deduplicación de `alert_id` y publicación MQTT.
- `/metrics` y `/audit` requieren token; si `METRICS_TOKEN` no está configurado, ambos responden 403.

Para el detalle completo: `docs/INFORME_D2_PERSONA_B_ACTUALIZADO.md` y `docs/CONTRATO_PERSONAS_A_B_D2.md`.
