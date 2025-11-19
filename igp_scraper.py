# igp_scraper.py
import os
import json
import uuid
from datetime import datetime

import boto3
import requests

# Endpoint ArcGIS con todos los sismos reportados (CENSIS / IGP)
ARCGIS_URL = (
    "https://ide.igp.gob.pe/arcgis/rest/services/"
    "monitoreocensis/SismosReportados/MapServer/0/query"
)

# Nombre de la tabla DynamoDB (se lee de variable de entorno o usa un default)
DYNAMO_TABLE_NAME = os.environ.get("DYNAMO_TABLE_NAME", "IGP_Sismos")


def _fetch_last_sismos(limit: int = 10):
    """
    Consulta el servicio ArcGIS y retorna los últimos `limit` sismos
    (features completos).
    """
    params = {
        "f": "json",
        "where": "1=1",
        "outFields": "*",
        # Ordenar por fechaevento descendente para que los primeros sean los más recientes
        "orderByFields": "fechaevento DESC",
        "resultRecordCount": limit,
        "returnGeometry": "false",
    }

    resp = requests.get(ARCGIS_URL, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    features = data.get("features", [])
    return features


def _ms_to_iso(ms):
    """
    Convierte un timestamp en milisegundos (formato ArcGIS de fecha) a ISO8601.
    Si algo falla, lo devuelve como string crudo.
    """
    if ms is None:
        return ""
    try:
        # ArcGIS date = milisegundos desde Unix epoch
        return datetime.utcfromtimestamp(ms / 1000.0).isoformat()
    except Exception:
        return str(ms)


def _normalize_feature(feature: dict) -> dict:
    """
    Toma un feature de ArcGIS y lo transforma en un dict "plano"
    listo para DynamoDB.
    """
    attrs = feature.get("attributes", {}) or {}

    fechaevento_ms = attrs.get("fechaevento") or attrs.get("fecha")

    item = {
        "id": str(uuid.uuid4()),  # PK en DynamoDB

        # Campos clave (strings para simplificar en DynamoDB)
        "codigo": str(attrs.get("code", "")),
        "fechaEvento": _ms_to_iso(fechaevento_ms),
        "horaLocal": attrs.get("hora", "") or "",
        "referencia": attrs.get("ref", "") or "",

        "magnitud": str(attrs.get("magnitud", "")),
        "intensidad": attrs.get("int_", "") or "",
        "profundidadKm": str(attrs.get("prof", "")),
        "profundidadCategoria": attrs.get("profundidad", "") or "",  # Superficial/Intermedio/Profundo
        "departamento": attrs.get("departamento", "") or "",
        "sentido": attrs.get("sentido", "") or "",
        "ultimoFlag": attrs.get("ultimo", "") or "",
    }

    return item


def _save_to_dynamo(items):
    """
    Borra el contenido previo de la tabla y escribe los nuevos items.
    """
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(DYNAMO_TABLE_NAME)

    # 1) Limpiar la tabla (como en tu ejemplo de bomberos)
    scan = table.scan()
    with table.batch_writer() as batch:
        for old in scan.get("Items", []):
            if "id" in old:
                batch.delete_item(Key={"id": old["id"]})

        # 2) Insertar nuevos
        for item in items:
            batch.put_item(Item=item)


def lambda_handler(event, context):
    try:
        features = _fetch_last_sismos(limit=10)
        items = [_normalize_feature(f) for f in features]

        _save_to_dynamo(items)

        return {
            "statusCode": 200,
            "body": json.dumps(items, ensure_ascii=False),
            "headers": {
                "Content-Type": "application/json; charset=utf-8",
                "Access-Control-Allow-Origin": "*",
            },
        }

    except Exception as e:
        # En caso de error, devolvemos un 500 para que el API lo vea clarito
        return {
            "statusCode": 500,
            "body": json.dumps(
                {"error": str(e)}, ensure_ascii=False
            ),
            "headers": {
                "Content-Type": "application/json; charset=utf-8",
                "Access-Control-Allow-Origin": "*",
            },
        }
