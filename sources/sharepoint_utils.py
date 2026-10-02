"""
sources/sharepoint_utils.py
Descarga de archivos desde SharePoint Online via Microsoft Graph API.

Autentica con client credentials (app registrada en Entra ID), descarga el
archivo a un temporal y delega la lectura en los lectores que ya existen
(excel_utils o csv_utils segun la extension). Asi hereda la deteccion de
formato: .xlsx, .xls binario, .xls SpreadsheetML, .csv y .txt.
"""

import logging
import os
import tempfile
from pathlib import Path
from typing import Iterator, Optional

import requests
from dotenv import load_dotenv
from msal import ConfidentialClientApplication

from sources.csv_utils import read_csv_chunks
from sources.excel_utils import read_excel_chunks, DEFAULT_CHUNK_SIZE

load_dotenv()

logger = logging.getLogger(__name__)

GRAPH_URL = "https://graph.microsoft.com/v1.0"
SCOPES    = ["https://graph.microsoft.com/.default"]

# Extensiones que se leen como texto delimitado en vez de Excel
CSV_EXTENSIONS = {".csv", ".txt", ".tsv", ".tab"}

# Cache a nivel de proceso: evita reautenticar y re-resolver los ids
# de sitio en cada tabla del run.
_token_cache = None
_ids_cache   = {}


def _get_credentials() -> dict:
    """
    Lee las credenciales de SharePoint desde variables de entorno.

    Variables esperadas en .env:
        SHAREPOINT_TENANT_ID
        SHAREPOINT_CLIENT_ID
        SHAREPOINT_CLIENT_SECRET
        SHAREPOINT_HOST         (ej: miempresa.sharepoint.com)
        SHAREPOINT_SITE_PATH    (ej: /sites/PowerBI)
        SHAREPOINT_DRIVE_NAME   (opcional - si no se define usa la primera
                                 biblioteca de documentos del sitio)
    """
    creds = {
        "tenant_id":     os.environ.get("SHAREPOINT_TENANT_ID"),
        "client_id":     os.environ.get("SHAREPOINT_CLIENT_ID"),
        "client_secret": os.environ.get("SHAREPOINT_CLIENT_SECRET"),
        "host":          os.environ.get("SHAREPOINT_HOST"),
        "site_path":     os.environ.get("SHAREPOINT_SITE_PATH"),
        "drive_name":    os.environ.get("SHAREPOINT_DRIVE_NAME"),
    }

    faltantes = [
        k for k in ("tenant_id", "client_id", "client_secret", "host", "site_path")
        if not creds[k]
    ]
    if faltantes:
        nombres = ", ".join(f"SHAREPOINT_{k.upper()}" for k in faltantes)
        raise EnvironmentError(
            f"Faltan credenciales de SharePoint en .env: {nombres}"
        )

    return creds


def get_token() -> str:
    """
    Obtiene un token de aplicacion para Microsoft Graph.
    Se cachea a nivel de proceso - MSAL renueva solo cuando expira.
    """
    global _token_cache
    if _token_cache:
        return _token_cache

    creds = _get_credentials()
    app = ConfidentialClientApplication(
        creds["client_id"],
        client_credential=creds["client_secret"],
        authority=f"https://login.microsoftonline.com/{creds['tenant_id']}",
    )

    result = app.acquire_token_for_client(SCOPES)
    if "access_token" not in result:
        raise EnvironmentError(
            f"No se pudo autenticar contra Microsoft Graph: "
            f"{result.get('error_description', result)}"
        )

    _token_cache = result["access_token"]
    logger.debug("Token de Microsoft Graph obtenido.")
    return _token_cache


def get_site_and_drive_id(site_path: Optional[str] = None) -> tuple:
    """
    Resuelve el site_id y drive_id del sitio de SharePoint.
    Se cachea por site_path para no repetir las llamadas en cada tabla.

    Args:
        site_path: Ruta del sitio (ej: /sites/PowerBI).
                   Si no se pasa, usa SHAREPOINT_SITE_PATH del .env.
    """
    creds     = _get_credentials()
    site_path = site_path or creds["site_path"]

    if site_path in _ids_cache:
        return _ids_cache[site_path]

    token   = get_token()
    headers = {"Authorization": f"Bearer {token}"}

    # 1. Resolver el sitio
    site_url  = f"{GRAPH_URL}/sites/{creds['host']}:{site_path}"
    site_resp = requests.get(site_url, headers=headers, timeout=60)
    if site_resp.status_code != 200:
        raise ValueError(
            f"No se pudo resolver el sitio de SharePoint '{site_path}' "
            f"en '{creds['host']}' (HTTP {site_resp.status_code}): {site_resp.text[:300]}"
        )
    site_id = site_resp.json()["id"]

    # 2. Resolver la biblioteca de documentos
    drives_url  = f"{GRAPH_URL}/sites/{site_id}/drives"
    drives_resp = requests.get(drives_url, headers=headers, timeout=60)
    drives_resp.raise_for_status()
    drives = drives_resp.json().get("value", [])

    if not drives:
        raise ValueError(f"El sitio '{site_path}' no tiene bibliotecas de documentos.")

    drive_name = creds["drive_name"]
    if drive_name:
        elegido = next((d for d in drives if d.get("name") == drive_name), None)
        if not elegido:
            disponibles = [d.get("name") for d in drives]
            raise ValueError(
                f"No se encontro la biblioteca '{drive_name}' en '{site_path}'. "
                f"Disponibles: {disponibles}"
            )
    else:
        elegido = drives[0]
        logger.debug("Usando la primera biblioteca del sitio: '%s'.", elegido.get("name"))

    ids = (site_id, elegido["id"])
    _ids_cache[site_path] = ids
    return ids


def download_file(remote_path: str, site_path: Optional[str] = None) -> bytes:
    """
    Descarga un archivo de SharePoint y devuelve su contenido en bytes.

    Args:
        remote_path: Ruta del archivo dentro de la biblioteca,
                     ej: 'Planes/Planes_ARG.xlsx'
        site_path:   Sitio de SharePoint. Si no se pasa usa el del .env.
    """
    site_id, drive_id = get_site_and_drive_id(site_path)
    headers = {"Authorization": f"Bearer {get_token()}"}

    remote_path = remote_path.strip("/")
    url = f"{GRAPH_URL}/sites/{site_id}/drives/{drive_id}/root:/{remote_path}:/content"

    logger.debug("Descargando de SharePoint: %s", remote_path)
    resp = requests.get(url, headers=headers, timeout=300)

    if resp.status_code == 404:
        raise FileNotFoundError(
            f"No se encontro el archivo '{remote_path}' en SharePoint. "
            f"Verifica la ruta en config/tables.json (es relativa a la "
            f"biblioteca de documentos, sin incluir su nombre)."
        )
    if resp.status_code != 200:
        raise ValueError(
            f"Error descargando '{remote_path}' de SharePoint "
            f"(HTTP {resp.status_code}): {resp.text[:300]}"
        )

    logger.debug("Descargados %.1f KB de '%s'.", len(resp.content) / 1024, remote_path)
    return resp.content


def read_sharepoint_chunks(
    remote_path: str,
    sheet_name: Optional[str] = None,
    columns: Optional[list] = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    all_string: bool = False,
    delimiter: str = ";",
    encoding: str = "utf-8",
    site_path: Optional[str] = None,
) -> Iterator[list[dict]]:
    """
    Descarga un archivo de SharePoint y devuelve sus filas por chunks.

    El archivo se baja a un temporal y la lectura se delega en los lectores
    existentes segun la extension, para reusar toda la deteccion de formato:
        .csv .txt .tsv .tab  -> csv_utils
        el resto            -> excel_utils (.xlsx, .xls binario, SpreadsheetML)

    Args:
        remote_path: Ruta del archivo en la biblioteca, ej 'Planes/Planes_ARG.xlsx'.
        sheet_name:  Hoja de Excel a leer. Ignorado para CSV.
        columns:     Columnas a extraer. None = todas.
        chunk_size:  Filas por chunk.
        all_string:  Si True, fuerza todo a string.
        delimiter:   Separador, solo para CSV.
        encoding:    Encoding, solo para CSV.
        site_path:   Sitio de SharePoint. Si no se pasa usa el del .env.

    Yields:
        Lista de dicts con las filas del chunk actual.
    """
    contenido = download_file(remote_path, site_path)
    extension = Path(remote_path).suffix.lower()

    # Escribir a un temporal para poder reusar los lectores existentes
    fd, temp_path = tempfile.mkstemp(suffix=extension or ".xlsx")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(contenido)

        if extension in CSV_EXTENSIONS:
            yield from read_csv_chunks(
                temp_path,
                delimiter=delimiter,
                encoding=encoding,
                columns=columns,
                chunk_size=chunk_size,
                all_string=all_string,
            )
        else:
            yield from read_excel_chunks(
                temp_path,
                sheet_name or "Sheet1",
                columns=columns,
                chunk_size=chunk_size,
                all_string=all_string,
            )
    finally:
        try:
            os.unlink(temp_path)
        except OSError:
            logger.debug("No se pudo borrar el temporal '%s'.", temp_path)
