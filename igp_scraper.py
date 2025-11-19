import os
import json
import uuid
from datetime import datetime

import boto3
import requests

# Endpoint ArcGIS del IGP (CENSIS - Sismos reportados)
ARCGIS_URL = (
    "https://ide.igp.gob.pe/arcgis/rest/services/"
    "monitoreocensis/SismosReportados/MapServer/0/query"
)

# Nombre de la tabla DynamoDB (puedes usar el env var o el valor por defecto)
DYNAMO_TABLE_NAME = os.environ.get("DYNAMO_TABLE_NAME", "TablaWebScrapping")


def _fetch_last_sismos(limit: int = 10):
    """
    Llama al servicio ArcGIS y obtiene los últimos `limit` sismos.
    """
    params = {
        "f": "json",
        "where": "1=1",
        "outFields": "*",
        "orderByFields": "fechaevento DESC",
        "resultRecordCount": limit,
        "returnGeometry": "false",
    }

    resp = requests.get(ARCGIS_URL, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    return data.get("features", [])


def _ms_to_iso(ms):
    """
    Convierte fecha en milisegundos (formato ArcGIS) a ISO8601.
    """
    if ms is None:
        return ""
    try:
        return datetime.utcfromtimestamp(ms / 1000.0).isoformat()
    except Exception:
        return str(ms)


def _normalize_feature(feature: dict) -> dict:
    """
    Normaliza un feature de ArcGIS a un dict plano listo para DynamoDB.
    """
    attrs = feature.get("attributes", {}) or {}
    fechaevento_ms = attrs.get("fechaevento") or attrs.get("fecha")

    item = {
        "id": str(uuid.uuid4()),  # PK en DynamoDB

        "codigo": str(attrs.get("code", "")),
        "fechaEvento": _ms_to_iso(fechaevento_ms),
        "horaLocal": attrs.get("hora", "") or "",
        "referencia": attrs.get("ref", "") or "",

        "magnitud": str(attrs.get("magnitud", "")),
        "intensidad": attrs.get("int_", "") or "",
        "profundidadKm": str(attrs.get("prof", "")),
        "profundidadCategoria": attrs.get("profundidad", "") or "",
        "departamento": attrs.get("departamento", "") or "",
        "sentido": attrs.get("sentido", "") or "",
        "ultimoFlag": attrs.get("ultimo", "") or "",
    }

    return item


def _save_to_dynamo(items):
    """
    Limpia la tabla y guarda los nuevos ítems.
    """
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(DYNAMO_TABLE_NAME)

    # 1) Borrar contenido previo (como en tu ejemplo de bomberos)
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

        # 👈 IMPORTANTE: body debe ser SIEMPRE un string (JSON serializado)
        return {
            "statusCode": 200,
            "body": json.dumps(items, ensure_ascii=False),
            "headers": {
                "Content-Type": "application/json; charset=utf-8",
                "Access-Control-Allow-Origin": "*",
            },
        }

    except Exception as e:
        # Si algo falla, devolvemos un 500 controlado
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)}, ensure_ascii=False),
            "headers": {
                "Content-Type": "application/json; charset=utf-8",
                "Access-Control-Allow-Origin": "*",
            },
        }
