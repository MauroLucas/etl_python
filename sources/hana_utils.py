"""
sources/hana_utils.py
Conexion a SAP HANA y lectura de datos por chunks.
Soporta multiples instancias de HANA (produccion, desarrollo, etc.).
Credenciales leidas desde variables de entorno o .env.
"""

import logging
import os
from typing import Iterator, Optional

from dotenv import load_dotenv
from hdbcli import dbapi

from sources.db_writer import validate_no_duplicate_columns

load_dotenv()

logger = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE = 10_000

# Prefijo de variables de entorno por source_name.
# Agregar nuevas instancias de HANA aqui agregando una entrada
# y las variables correspondientes en .env
HANA_ENV_PREFIX = {
    "hana":    "HANA",
    "hanadev": "HANADEV",
}


def _get_credentials(source_name: str) -> dict:
    """
    Lee las credenciales de SAP HANA desde variables de entorno.

    source_name   prefijo de variables en .env
    -----------   ----------------------------
    hana       ->  HANA_HOST, HANA_PORT, HANA_DATABASE, HANA_USERNAME, HANA_PASSWORD
    hanadev    ->  HANADEV_HOST, HANADEV_PORT, HANADEV_DATABASE, HANADEV_USERNAME, HANADEV_PASSWORD
    """
    prefix = HANA_ENV_PREFIX.get(source_name)
    if not prefix:
        raise EnvironmentError(
            f"Origen '{source_name}' no registrado en HANA_ENV_PREFIX. "
            f"Agrega una entrada en sources/hana_utils.py."
        )

    host     = os.environ.get(f"{prefix}_HOST")
    port     = os.environ.get(f"{prefix}_PORT")
    database = os.environ.get(f"{prefix}_DATABASE")
    username = os.environ.get(f"{prefix}_USERNAME")
    password = os.environ.get(f"{prefix}_PASSWORD")

    if not host or not port or not database:
        raise EnvironmentError(
            f"Faltan credenciales para el origen SAP HANA '{source_name}'. "
            f"Revisa .env: {prefix}_HOST, {prefix}_PORT, {prefix}_DATABASE, "
            f"{prefix}_USERNAME, {prefix}_PASSWORD"
        )

    return {
        "host": host, "port": port, "database": database,
        "username": username, "password": password,
    }


def get_hana_connection(source_name: str = "hana") -> dbapi.Connection:
    """
    Crea y devuelve una conexion a la instancia de SAP HANA indicada.

    Args:
        source_name: Nombre del origen segun HANA_ENV_PREFIX (hana, hanadev, ...).
    """
    creds = _get_credentials(source_name)

    logger.debug(
        "Conectando a SAP HANA '%s': %s:%s / %s",
        source_name, creds["host"], creds["port"], creds["database"],
    )

    return dbapi.connect(
        address=creds["host"],
        port=int(creds["port"]),
        user=creds["username"],
        password=creds["password"],
        databaseName=creds["database"],
        encrypt=False,
    )


def read_hana_chunks(
    query: str,
    params: Optional[dict] = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    source_name: str = "hana",
) -> Iterator[list[dict]]:
    """
    Ejecuta una query en SAP HANA y devuelve los resultados por chunks
    como listas de diccionarios.

    Args:
        query:       Query SQL a ejecutar.
        params:      Parametros de la query (usa ? como placeholder en hdbcli).
        chunk_size:  Filas por chunk - viene de la config de cada tabla en el JSON.
        source_name: Instancia de HANA a usar (hana, hanadev, ...).

    Yields:
        Lista de dicts con las filas del chunk actual.
    """
    params = params or {}

    # hdbcli usa ? como placeholder - convertimos :nombre -> ? en orden
    param_values = []
    if params:
        for key, value in params.items():
            placeholder = f":{key}"
            if placeholder in query:
                query = query.replace(placeholder, "?")
                param_values.append(value)

    conn = get_hana_connection(source_name)
    try:
        cursor = conn.cursor()
        cursor.execute(query, param_values if param_values else None)
        columns = [col[0] for col in cursor.description]
        validate_no_duplicate_columns(columns, f"SAP HANA ({source_name})")

        while True:
            rows = cursor.fetchmany(chunk_size)
            if not rows:
                break
            yield [dict(zip(columns, row)) for row in rows]

        cursor.close()
    finally:
        conn.close()


def get_hana_column_lengths(
    schema: str,
    table: str,
    source_name: str = "hana",
) -> dict:
    """
    Consulta SYS.TABLE_COLUMNS en HANA para obtener el largo maximo
    de cada columna de tipo string.

    Args:
        schema:      Schema de HANA.
        table:       Nombre de la tabla.
        source_name: Instancia de HANA a consultar (hana, hanadev, ...).

    Returns:
        Diccionario {column_name: max_length} solo para columnas de tipo string.
        Columnas sin largo definido (numericas, fechas) no se incluyen.
    """
    query = """
        SELECT COLUMN_NAME, LENGTH
        FROM SYS.TABLE_COLUMNS
        WHERE SCHEMA_NAME = ? AND TABLE_NAME = ?
        AND DATA_TYPE_NAME IN ('NVARCHAR', 'VARCHAR', 'CHAR', 'NCHAR', 'ALPHANUM', 'SHORTTEXT')
    """
    conn = get_hana_connection(source_name)
    try:
        cursor = conn.cursor()
        cursor.execute(query, [schema, table])
        rows = cursor.fetchall()

        if not rows:
            logger.warning(
                "La consulta al schema de HANA para '%s.%s' no devolvio columnas string. "
                "Verificar que el schema y tabla existen y tienen columnas de tipo string.",
                schema, table,
            )
            return {}

        result = {row[0]: row[1] for row in rows}
        logger.debug(
            "Schema HANA '%s.%s': %d columnas string encontradas.",
            schema, table, len(result),
        )
        return result

    except Exception as exc:
        logger.warning(
            "No se pudo consultar largos de columnas desde HANA para '%s.%s': %s. "
            "Causa probable: permisos insuficientes sobre SYS.TABLE_COLUMNS "
            "o nombre de schema/tabla incorrecto.",
            schema, table, exc,
        )
        return {}
    finally:
        conn.close()
