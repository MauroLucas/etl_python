"""
sources/sqlserver_utils.py
Conexion a SQL Server origen y lectura de datos por chunks.
Usa SQLAlchemy + pyodbc, igual que el destino.
"""

import logging
import os
from typing import Iterator, Optional

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

load_dotenv()

logger = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE = 10_000


import pyodbc

from sources.db_writer import detect_odbc_driver, validate_no_duplicate_columns

# Prefijo de variables de entorno por source_name
# Agregar nuevos SQL Server origen aqui agregando una entrada al diccionario
# y las variables correspondientes en .env
SQLSERVER_ENV_PREFIX = {
    "sqlserver":  "SQLSERVER",
    "sqlserver2": "SQLSERVER2",
    "sqlserver3": "SQLSERVER3",
}


def _get_credentials(prefix: str) -> dict:
    """
    Lee las credenciales de SQL Server desde variables de entorno.

    Variables esperadas (ejemplo para prefix="SQLSERVER"):
        SQLSERVER_HOST
        SQLSERVER_PORT        (opcional, default 1433)
        SQLSERVER_DATABASE
        SQLSERVER_USERNAME
        SQLSERVER_PASSWORD    (soporta cualquier caracter especial con comillas en .env)
        SQLSERVER_DRIVER      (opcional, se autodetecta si no se define)
    """
    host     = os.environ.get(f"{prefix}_HOST")
    port     = os.environ.get(f"{prefix}_PORT", "1433")
    database = os.environ.get(f"{prefix}_DATABASE")
    username = os.environ.get(f"{prefix}_USERNAME")
    password = os.environ.get(f"{prefix}_PASSWORD", "")
    driver   = os.environ.get(f"{prefix}_DRIVER") or detect_odbc_driver()

    if not host or not database:
        raise EnvironmentError(
            f"Faltan credenciales para el origen SQL Server '{prefix}'. "
            f"Revisa .env: {prefix}_HOST, {prefix}_DATABASE, "
            f"{prefix}_USERNAME, {prefix}_PASSWORD"
        )

    return {
        "host": host, "port": port, "database": database,
        "username": username, "password": password, "driver": driver,
    }


def get_sqlserver_source_engine(source_name: str = "sqlserver") -> Engine:
    """
    Crea y devuelve un engine SQLAlchemy para SQL Server origen.
    Usa pyodbc directo como creator para evitar problemas de encodeo
    de caracteres especiales (n, #, @, etc.) en la password.

    source_name   prefijo de variables en .env
    -----------   ----------------------------
    sqlserver  ->  SQLSERVER_HOST, SQLSERVER_DATABASE, SQLSERVER_USERNAME, SQLSERVER_PASSWORD
    sqlserver2 ->  SQLSERVER2_HOST, SQLSERVER2_DATABASE, SQLSERVER2_USERNAME, SQLSERVER2_PASSWORD
    sqlserver3 ->  SQLSERVER3_HOST, SQLSERVER3_DATABASE, SQLSERVER3_USERNAME, SQLSERVER3_PASSWORD
    """
    prefix = SQLSERVER_ENV_PREFIX.get(source_name)
    if not prefix:
        raise EnvironmentError(
            f"Origen '{source_name}' no registrado en SQLSERVER_ENV_PREFIX. "
            f"Agrega una entrada en sqlserver_utils.py."
        )

    creds = _get_credentials(prefix)

    # Usar pyodbc directo como creator para evitar encodeo de caracteres especiales
    # La password se pasa tal cual sin URL encoding
    def _creator():
        conn_str = (
            f"DRIVER={{{creds['driver']}}};"
            f"SERVER={creds['host']},{creds['port']};"
            f"DATABASE={creds['database']};"
            f"UID={creds['username']};"
            f"PWD={creds['password']};"
            f"TrustServerCertificate=yes;"
            f"Encrypt=yes;"
        )
        return pyodbc.connect(conn_str, autocommit=False)

    return create_engine("mssql+pyodbc://", creator=_creator, fast_executemany=True)


def read_sqlserver_chunks(
    engine: Engine,
    query: str,
    params: Optional[dict] = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> Iterator[list[dict]]:
    """
    Ejecuta una query en SQL Server origen y devuelve los resultados
    por chunks como listas de diccionarios.

    Args:
        engine:     Engine SQLAlchemy del origen.
        query:      Query SQL a ejecutar (puede tener :parametros nombrados).
        params:     Diccionario con valores de parametros.
        chunk_size: Filas por chunk.

    Yields:
        Lista de dicts con las filas del chunk actual.
    """
    params = params or {}

    with engine.connect() as conn:
        result  = conn.execute(text(query), params if params else {})
        columns = list(result.keys())
        validate_no_duplicate_columns(columns, "SQL Server origen")

        while True:
            rows = result.fetchmany(chunk_size)
            if not rows:
                break
            yield [dict(zip(columns, row)) for row in rows]


def get_sqlserver_column_lengths(
    engine: Engine,
    schema: str,
    table: str,
) -> dict:
    """
    Consulta INFORMATION_SCHEMA.COLUMNS para obtener el largo maximo
    de cada columna de tipo string.

    Returns:
        Diccionario {column_name: max_length} solo para columnas string.
        Columnas con CHARACTER_MAXIMUM_LENGTH = -1 son NVARCHAR(MAX).
    """
    query = text("""
        SELECT COLUMN_NAME, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = :schema
          AND TABLE_NAME   = :table
          AND DATA_TYPE IN ('nvarchar', 'varchar', 'char', 'nchar')
    """)

    try:
        with engine.connect() as conn:
            rows = conn.execute(query, {"schema": schema, "table": table}).fetchall()

        if not rows:
            logger.warning(
                "La consulta al schema de SQL Server para '%s.%s' no devolvio columnas string.",
                schema, table,
            )
            return {}

        result = {}
        for col_name, max_len in rows:
            # -1 significa NVARCHAR(MAX) en SQL Server
            result[col_name] = None if max_len == -1 else max_len

        logger.debug(
            "Schema SQL Server '%s.%s': %d columnas string encontradas.",
            schema, table, len(result),
        )
        return result

    except Exception as exc:
        logger.warning(
            "No se pudo consultar largos de columnas desde SQL Server para '%s.%s': %s.",
            schema, table, exc,
        )
        return {}
