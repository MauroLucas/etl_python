"""
sources/oracle_utils.py
Conexion a Oracle y lectura de datos por chunks.
Requiere Oracle Instant Client para Oracle 12 (modo thick).
"""

import logging
import os
from typing import Iterator, Optional

import oracledb
from dotenv import load_dotenv

from sources.db_writer import validate_no_duplicate_columns

load_dotenv()

logger = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE = 10_000

# Inicializacion del modo thick - se ejecuta una sola vez al importar el modulo
_thick_initialized = False


def _init_thick_mode() -> None:
    """
    Inicializa oracledb en modo thick usando el Oracle Instant Client.
    Requerido para Oracle 12 que usa protocolo de autenticacion antiguo.
    Solo se inicializa una vez por proceso.
    """
    global _thick_initialized
    if _thick_initialized:
        return

    client_dir = os.environ.get("ORACLE_CLIENT_DIR")
    if not client_dir:
        raise EnvironmentError(
            "No se encontro ORACLE_CLIENT_DIR en .env. "
            "Ejemplo: ORACLE_CLIENT_DIR=C:\\oracle\\instantclient_21_15"
        )

    oracledb.init_oracle_client(lib_dir=client_dir)
    _thick_initialized = True
    logger.debug("Oracle Instant Client inicializado desde: %s", client_dir)


def get_oracle_connection() -> oracledb.Connection:
    """
    Crea y devuelve una conexion a Oracle leyendo credenciales
    desde variables de entorno o .env.
    """
    _init_thick_mode()

    host     = os.environ.get("ORACLE_HOST")
    port     = os.environ.get("ORACLE_PORT", "1521")
    database = os.environ.get("ORACLE_DATABASE")  # service name o SID
    username = os.environ.get("ORACLE_USERNAME")
    password = os.environ.get("ORACLE_PASSWORD")

    if not host or not database:
        raise EnvironmentError(
            "No se encontraron credenciales para Oracle. "
            "Revisa .env: ORACLE_HOST, ORACLE_PORT, ORACLE_DATABASE, "
            "ORACLE_USERNAME, ORACLE_PASSWORD, ORACLE_CLIENT_DIR"
        )

    dsn = f"{host}:{port}/{database}"
    logger.debug("Conectando a Oracle: %s", dsn)

    return oracledb.connect(
        user=username,
        password=password,
        dsn=dsn,
    )


def read_oracle_chunks(
    query: str,
    params: Optional[dict] = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> Iterator[list[dict]]:
    """
    Ejecuta una query en Oracle y devuelve los resultados por chunks
    como listas de diccionarios.

    Oracle usa :nombre como placeholder (igual que HANA).

    Args:
        query:      Query SQL a ejecutar.
        params:     Diccionario con valores de parametros.
        chunk_size: Filas por chunk - viene de la config de cada tabla en el JSON.

    Yields:
        Lista de dicts con las filas del chunk actual.
    """
    params = params or {}

    conn = get_oracle_connection()
    try:
        cursor = conn.cursor()

        # oracledb acepta :nombre directamente como placeholder
        cursor.execute(query, params if params else None)
        columns = [col[0] for col in cursor.description]
        validate_no_duplicate_columns(columns, "Oracle")

        while True:
            rows = cursor.fetchmany(chunk_size)
            if not rows:
                break
            yield [dict(zip(columns, row)) for row in rows]

        cursor.close()
    finally:
        conn.close()


def get_oracle_column_lengths(schema: str, table: str) -> dict:
    """
    Consulta ALL_TAB_COLUMNS en Oracle para obtener el largo maximo
    de cada columna de tipo string.

    Returns:
        Diccionario {column_name: max_length} solo para columnas de tipo string.
    """
    # Usar format() en lugar de placeholders para evitar ORA-01745
    # en queries sobre vistas del sistema de Oracle
    query = """
        SELECT COLUMN_NAME, DATA_LENGTH
        FROM ALL_TAB_COLUMNS
        WHERE OWNER      = '{schema}'
          AND TABLE_NAME = '{table}'
          AND DATA_TYPE IN ('VARCHAR2', 'NVARCHAR2', 'CHAR', 'NCHAR', 'VARCHAR')
    """.format(schema=schema.upper(), table=table.upper())
    try:
        conn = get_oracle_connection()
        cursor = conn.cursor()
        cursor.execute(query)
        result = {row[0]: row[1] for row in cursor.fetchall()}
        cursor.close()
        conn.close()
        return result
    except Exception as exc:
        logger.warning(
            "No se pudo consultar largos de columnas para '%s.%s': %s. "
            "Se usara NVARCHAR(MAX) para todas las columnas string.",
            schema, table, exc,
        )
        return {}
