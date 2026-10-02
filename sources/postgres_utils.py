"""
sources/postgres_utils.py
Conexion a PostgreSQL y lectura de datos por chunks.
"""

import logging
import os
from typing import Iterator, Optional

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

from sources.db_writer import validate_no_duplicate_columns

load_dotenv()

logger = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE = 10_000


def get_postgres_connection() -> psycopg2.extensions.connection:
    """
    Crea y devuelve una conexion a PostgreSQL leyendo credenciales
    desde variables de entorno o .env.
    """
    host     = os.environ.get("POSTGRES_HOST")
    port     = os.environ.get("POSTGRES_PORT", "5432")
    database = os.environ.get("POSTGRES_DATABASE")
    username = os.environ.get("POSTGRES_USERNAME")
    password = os.environ.get("POSTGRES_PASSWORD")

    if not host or not database:
        raise EnvironmentError(
            "No se encontraron credenciales para PostgreSQL. "
            "Revisa .env: POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DATABASE, "
            "POSTGRES_USERNAME, POSTGRES_PASSWORD"
        )

    logger.debug("Conectando a PostgreSQL: %s:%s/%s", host, port, database)

    return psycopg2.connect(
        host=host,
        port=int(port),
        dbname=database,
        user=username,
        password=password,
    )


def read_postgres_chunks(
    query: str,
    params: Optional[dict] = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> Iterator[list[dict]]:
    """
    Ejecuta una query en PostgreSQL y devuelve los resultados por chunks
    como listas de diccionarios.

    Args:
        query:      Query SQL a ejecutar. Usa %(nombre)s como placeholder.
        params:     Diccionario con valores de parametros.
        chunk_size: Filas por chunk - viene de la config de cada tabla en el JSON.

    Yields:
        Lista de dicts con las filas del chunk actual.
    """
    # PostgreSQL usa %(nombre)s como placeholder - convertimos :nombre -> %(nombre)s
    params = params or {}
    if params:
        for key in params:
            query = query.replace(f":{key}", f"%({key})s")

    conn = get_postgres_connection()
    try:
        # RealDictCursor devuelve filas como dicts directamente
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(query, params if params else None)
        validate_no_duplicate_columns([d[0] for d in cursor.description], "PostgreSQL")

        while True:
            rows = cursor.fetchmany(chunk_size)
            if not rows:
                break
            yield [dict(row) for row in rows]

        cursor.close()
    finally:
        conn.close()


def get_postgres_column_lengths(schema: str, table: str) -> dict:
    """
    Consulta information_schema en PostgreSQL para obtener el largo maximo
    de cada columna de tipo string.

    Returns:
        Diccionario {column_name: max_length} solo para columnas de tipo string.
    """
    query = """
        SELECT column_name, character_maximum_length
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name   = %s
          AND data_type IN ('character varying', 'character', 'text', 'varchar', 'char')
    """
    conn = get_postgres_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(query, [schema, table])
        result = {}
        for col_name, max_len in cursor.fetchall():
            # text en PostgreSQL no tiene largo definido - usar None para NVARCHAR(MAX)
            result[col_name] = max_len if max_len else None
        return result
    except Exception as exc:
        logger.warning(
            "No se pudo consultar largos de columnas para '%s.%s': %s. "
            "Se usara NVARCHAR(MAX) para todas las columnas string.",
            schema, table, exc,
        )
        return {}
    finally:
        conn.close()
