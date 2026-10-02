"""
sources/db_writer.py
Escritura de datos hacia SQL Server usando SQLAlchemy + pyodbc.
Maneja creacion automatica de tablas, TRUNCATE e INSERT por chunks.

Estrategia de tipos para strings:

  CON nvarchar_max_length en el JSON:
    -> Todas las columnas string usan ese tamanio fijo (NVARCHAR(n))
    -> No se consulta schema origen ni se infiere desde el chunk
    -> Recomendado para tablas con datos inconsistentes con el schema

  SIN nvarchar_max_length:
    1. Schema del origen (HANA/Oracle/Postgres) -> largo exacto definido
    2. Inferencia desde el primer chunk -> redondea a 255/500/1000/2000/4000/MAX
    3. Columna completamente nula -> NVARCHAR(MAX)
"""

import logging
import os
from datetime import date, datetime

import pyodbc
from typing import Any, Iterator, Optional

from dotenv import load_dotenv
from sqlalchemy import (
    Unicode, UnicodeText,
    BigInteger, Column, Date, DateTime, Float,
    MetaData, String, Table, Text,
    create_engine, text,
)
from sqlalchemy.engine import Engine

load_dotenv()

logger = logging.getLogger(__name__)

# Maximo ColumnSize que SQL Server acepta para SQL_WVARCHAR al bindear
# un parametro. Por encima de este valor hay que pasar 0, que es como
# el driver representa NVARCHAR(MAX). Cualquier otro numero mayor se
# rechaza con HY104 Invalid precision value.
MAX_NVARCHAR_BIND = 4000
NVARCHAR_MAX_BIND = 0


def detect_odbc_driver() -> str:
    """
    Detecta el driver ODBC de SQL Server instalado, prefiriendo el mas nuevo.
    Evita tener que configurar el driver manualmente en cada maquina.

    Orden de preferencia:
      1. ODBC Driver 18 for SQL Server
      2. ODBC Driver 17 for SQL Server
      3. SQL Server Native Client 11.0
      4. SQL Server (driver legacy de Windows)
    """
    installed = pyodbc.drivers()
    for candidate in (
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "SQL Server Native Client 11.0",
        "SQL Server",
    ):
        if candidate in installed:
            logger.debug("Driver ODBC detectado: %s", candidate)
            return candidate

    raise EnvironmentError(
        f"No se encontro ningun driver ODBC de SQL Server instalado. "
        f"Drivers disponibles: {installed}. "
        f"Descargar desde: https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server"
    )


def get_destination_engine() -> Engine:
    """
    Crea y devuelve un engine SQLAlchemy para SQL Server destino.

    Usa pyodbc directo como creator para evitar problemas de encodeo
    de caracteres especiales (#, n, @, %) en la password.

    Variables esperadas en .env:
        DESTINATION_HOST
        DESTINATION_PORT       (opcional, default 1433)
        DESTINATION_DATABASE
        DESTINATION_USERNAME
        DESTINATION_PASSWORD   (usar comillas dobles si contiene #)
        DESTINATION_DRIVER     (opcional, se autodetecta si no se define)

    Compatibilidad: si no estan definidas las variables individuales,
    intenta usar DESTINATION_CONNECTION_STRING como antes.
    """
    host     = os.environ.get("DESTINATION_HOST")
    port     = os.environ.get("DESTINATION_PORT", "1433")
    database = os.environ.get("DESTINATION_DATABASE")
    username = os.environ.get("DESTINATION_USERNAME")
    password = os.environ.get("DESTINATION_PASSWORD", "")
    driver   = os.environ.get("DESTINATION_DRIVER") or detect_odbc_driver()

    if host and database:
        # Modo recomendado: credenciales separadas via pyodbc creator
        def _creator():
            conn_str = (
                f"DRIVER={{{driver}}};"
                f"SERVER={host},{port};"
                f"DATABASE={database};"
                f"UID={username};"
                f"PWD={password};"
                f"TrustServerCertificate=yes;"
                f"Encrypt=yes;"
            )
            return pyodbc.connect(conn_str, autocommit=False)

        return create_engine("mssql+pyodbc://", creator=_creator)

    # Fallback: connection string completa (legacy)
    conn_str = os.environ.get("DESTINATION_CONNECTION_STRING")
    if conn_str:
        return create_engine(conn_str)

    raise EnvironmentError(
        "No se encontraron credenciales para el destino. "
        "Revisa .env: DESTINATION_HOST, DESTINATION_DATABASE, "
        "DESTINATION_USERNAME, DESTINATION_PASSWORD"
    )


def _string_type(length: Optional[int]):
    """
    Devuelve el tipo SQLAlchemy para una columna string.
    Siempre usa NVARCHAR (Unicode) para compatibilidad total con SAP y datos internacionales:
      None o <= 0  -> NVARCHAR(MAX)
      <= 4000      -> NVARCHAR(length)
      > 4000       -> NVARCHAR(MAX)
    """
    if not length or length <= 0:
        return UnicodeText()    # NVARCHAR(MAX)
    if length <= 4000:
        return Unicode(length)  # NVARCHAR(length)
    return UnicodeText()        # NVARCHAR(MAX)


def _infer_column_types(
    chunk: list[dict],
    col_lengths: Optional[dict] = None,
    nvarchar_max_length: Optional[int] = None,
) -> dict:
    """
    Infiere los tipos de todas las columnas con esta prioridad para strings:

    1. col_lengths del schema origen -> largo exacto del sistema origen
    2. Inferencia desde el chunk    -> largo maximo medido en las filas
       capado por nvarchar_max_length si esta definido
    3. Columna completamente nula   -> nvarchar_max_length o NVARCHAR(MAX)

    Args:
        chunk:               Primer chunk de datos.
        col_lengths:         {col_name: max_length} del schema origen.
        nvarchar_max_length: Tope maximo para strings inferidos desde el chunk.
                             None = sin tope (puede llegar a NVARCHAR(MAX)).
    """
    if not chunk:
        return {}

    columns   = list(chunk[0].keys())
    col_types = {}

    for col in columns:
        values   = [row.get(col) for row in chunk]
        non_null = [v for v in values if v is not None]

        if not non_null:
            # Columna completamente nula - usar fallback
            col_types[col] = _string_type(nvarchar_max_length)
            continue

        sample = non_null[0]

        if isinstance(sample, bool):
            col_types[col] = UnicodeText()  # bool como NVARCHAR
        elif isinstance(sample, int):
            col_types[col] = BigInteger()
        elif isinstance(sample, float):
            col_types[col] = Float()
        elif isinstance(sample, datetime):
            col_types[col] = DateTime()
        elif isinstance(sample, date):
            col_types[col] = Date()
        else:
            # String - dos modos segun si nvarchar_max_length esta definido:
            #
            # CON nvarchar_max_length: todas las columnas string usan ese tamanio fijo.
            #   No se consulta el schema origen ni se infiere desde el chunk.
            #   Garantiza consistencia y evita truncamientos inesperados.
            #
            # SIN nvarchar_max_length: intenta inferir en este orden:
            #   1. Largo del schema origen (HANA, Oracle, Postgres)
            #   2. Largo maximo medido en el primer chunk
            #   3. NVARCHAR(MAX) como ultimo recurso

            if nvarchar_max_length:
                # Tamanio fijo para todas las columnas string
                col_types[col] = _string_type(nvarchar_max_length)
            elif col_lengths and col in col_lengths and col_lengths[col]:
                # Schema origen disponible
                col_types[col] = _string_type(col_lengths[col])
            else:
                # Inferir desde el chunk
                measured = max(
                    (len(str(v)) for v in non_null),
                    default=0,
                )
                if measured <= 255:
                    length = 255
                elif measured <= 500:
                    length = 500
                elif measured <= 1000:
                    length = 1000
                elif measured <= 2000:
                    length = 2000
                elif measured <= 4000:
                    length = 4000
                else:
                    length = None  # NVARCHAR(MAX)
                col_types[col] = _string_type(length)

    return col_types


def _ensure_table(
    engine: Engine,
    schema: str,
    table_name: str,
    chunk: list[dict],
    col_lengths: Optional[dict] = None,
    nvarchar_max_length: Optional[int] = None,
) -> None:
    """
    Crea la tabla en destino si no existe.
    Si ya existe, agrega columnas nuevas con ALTER TABLE.
    """
    col_types = _infer_column_types(chunk, col_lengths, nvarchar_max_length)

    with engine.connect() as conn:
        exists = engine.dialect.has_table(conn, table_name, schema=schema)

    if not exists:
        metadata = MetaData()
        columns  = [
            Column(col_name, col_type, nullable=True)
            for col_name, col_type in col_types.items()
        ]
        table = Table(table_name, metadata, *columns, schema=schema)
        metadata.create_all(engine)
        logger.info("Tabla '%s.%s' creada con %d columnas.", schema, table_name, len(columns))
    else:
        with engine.connect() as conn:
            existing = {
                row[0] for row in conn.execute(
                    text(
                        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                        "WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table"
                    ),
                    {"schema": schema, "table": table_name},
                )
            }

        new_columns = {k: v for k, v in col_types.items() if k not in existing}
        if new_columns:
            with engine.begin() as conn:
                for col_name, col_type in new_columns.items():
                    type_str = col_type.compile(dialect=engine.dialect)
                    conn.execute(
                        text(
                            f"ALTER TABLE [{schema}].[{table_name}] "
                            f"ADD [{col_name}] {type_str} NULL"
                        )
                    )
                    logger.info(
                        "Columna '%s' (%s) agregada a '%s.%s'.",
                        col_name, type_str, schema, table_name,
                    )


def _next_width_tier(measured: int) -> Optional[int]:
    """
    Devuelve el siguiente tamanio estandar que cubre el largo medido.
    Redondear a niveles evita hacer un ALTER por cada byte de crecimiento.
    None significa NVARCHAR(MAX).
    """
    for tier in (255, 500, 1000, 2000, 4000):
        if measured <= tier:
            return tier
    return None  # NVARCHAR(MAX)


def get_string_column_widths(engine: Engine, schema: str, table_name: str) -> dict:
    """
    Devuelve el ancho actual de las columnas string de la tabla destino.

    {col_name: largo}  donde None significa NVARCHAR(MAX) (sin limite).
    Solo incluye columnas de tipo caracter - las numericas y fechas se omiten.
    """
    query = text(
        "SELECT COLUMN_NAME, CHARACTER_MAXIMUM_LENGTH "
        "FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table "
        "AND DATA_TYPE IN ('nvarchar','varchar','char','nchar')"
    )
    try:
        with engine.connect() as conn:
            rows = conn.execute(query, {"schema": schema, "table": table_name}).fetchall()
        # -1 en SQL Server significa MAX
        return {c: (None if length == -1 else length) for c, length in rows}
    except Exception as exc:
        logger.warning(
            "No se pudo consultar el ancho de columnas de '%s.%s': %s",
            schema, table_name, exc,
        )
        return {}


def widen_columns_if_needed(
    engine: Engine,
    schema: str,
    table_name: str,
    chunk: list[dict],
    current_widths: dict,
) -> None:
    """
    Ensancha con ALTER TABLE las columnas string que no alcanzan para el chunk.

    Evita el error 'String or binary data would be truncated' cuando un chunk
    trae valores mas largos que los del chunk que creo la tabla.
    Ensanchar nunca pierde datos, asi que es seguro hacerlo automaticamente.

    Muta current_widths con los nuevos anchos para no repetir el ALTER.
    """
    for col, current in list(current_widths.items()):
        if current is None:
            continue  # ya es NVARCHAR(MAX), no hay nada que ensanchar

        values = [row.get(col) for row in chunk]
        measured = max(
            (len(str(v)) for v in values if v is not None),
            default=0,
        )
        if measured <= current:
            continue

        new_width = _next_width_tier(measured)
        type_str  = "NVARCHAR(MAX)" if new_width is None else f"NVARCHAR({new_width})"

        try:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        f"ALTER TABLE [{schema}].[{table_name}] "
                        f"ALTER COLUMN [{col}] {type_str} NULL"
                    )
                )
            current_widths[col] = new_width
            logger.info(
                "Columna '%s.%s.%s' ensanchada de NVARCHAR(%s) a %s "
                "(el dato requeria %d caracteres).",
                schema, table_name, col, current, type_str, measured,
            )
        except Exception as exc:
            logger.warning(
                "No se pudo ensanchar la columna '%s' de '%s.%s' a %s: %s. "
                "Puede tener un indice o constraint asociado.",
                col, schema, table_name, type_str, exc,
            )


def _truncate_table(engine: Engine, schema: str, table_name: str) -> None:
    """Trunca la tabla destino antes de la primera insercion."""
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE [{schema}].[{table_name}]"))
    logger.info("Tabla '%s.%s' truncada.", schema, table_name)


def validate_no_duplicate_columns(columns: list, source_desc: str) -> None:
    """
    Verifica que no haya nombres de columna repetidos.

    dict(zip(columns, row)) colapsa silenciosamente las columnas duplicadas
    y se pierde una sin aviso. Pasa cuando la query hace
    'SELECT a.id, b.id FROM a JOIN b' sin alias.
    Es mejor fallar con un mensaje claro que cargar datos incompletos.
    """
    seen, dupes = set(), []
    for c in columns:
        if c in seen and c not in dupes:
            dupes.append(c)
        seen.add(c)

    if dupes:
        raise ValueError(
            f"La consulta a '{source_desc}' devuelve columnas duplicadas: {dupes}. "
            f"Cargar asi perderia datos silenciosamente. "
            f"Agrega alias en la query, por ejemplo: SELECT a.id AS a_id, b.id AS b_id"
        )


def _normalize_value(value: Any) -> Any:
    """
    Convierte tipos de numpy/pandas a tipos Python nativos.

    pandas devuelve numpy.int64, numpy.float64, numpy.bool_, pd.Timestamp, etc.
    pyodbc no sabe manejar esos tipos y falla con errores como
    'Numeric value out of range'. Esta funcion los normaliza.

    Tambien convierte NaN / NaT / pd.NA a None para que lleguen como NULL.
    """
    if value is None:
        return None

    # NaN, NaT, pd.NA - detectar sin importar pandas si no hace falta
    try:
        import pandas as pd
        if pd.isna(value):
            return None
    except (ImportError, ValueError, TypeError):
        # ValueError/TypeError: pd.isna falla con arrays o tipos raros
        pass

    # numpy scalars exponen .item() que devuelve el tipo Python nativo
    if hasattr(value, "item") and not isinstance(value, (str, bytes)):
        try:
            return value.item()
        except (AttributeError, ValueError):
            pass

    return value


def write_chunks(
    engine: Engine,
    schema: str,
    table_name: str,
    chunks: Iterator[list[dict]],
    col_lengths: Optional[dict] = None,
    nvarchar_max_length: Optional[int] = None,
) -> int:
    """
    Escribe chunks de datos en la tabla destino.
    - Primer chunk: verifica/crea tabla, TRUNCATE, INSERT
    - Chunks siguientes: INSERT directo

    Args:
        engine:              Engine SQLAlchemy del destino.
        schema:              Schema destino (ej: "raw").
        table_name:          Nombre de la tabla destino.
        chunks:              Iterable de listas de dicts.
        col_lengths:         Largos de columnas del schema origen (opcional).
        nvarchar_max_length: Tope maximo para strings inferidos (opcional).

    Returns:
        Total de filas insertadas.
    """
    total_rows     = 0
    first_chunk    = True
    current_widths = {}

    for chunk in chunks:
        if not chunk:
            continue

        # Normalizar tipos de numpy/pandas a Python nativo.
        # Sin esto pyodbc falla con numpy.int64, numpy.bool_, pd.Timestamp, NaN.
        chunk = [
            {k: _normalize_value(v) for k, v in row.items()}
            for row in chunk
        ]

        if first_chunk:
            _ensure_table(engine, schema, table_name, chunk, col_lengths, nvarchar_max_length)
            _truncate_table(engine, schema, table_name)
            # Cachear el ancho actual de las columnas string para poder
            # ensancharlas si un chunk trae valores mas largos
            current_widths = get_string_column_widths(engine, schema, table_name)
            first_chunk = False

        # Ensanchar columnas que no alcancen para este chunk
        if current_widths:
            widen_columns_if_needed(engine, schema, table_name, chunk, current_widths)

        cols    = list(chunk[0].keys())
        col_str = ", ".join(f"[{c}]" for c in cols)
        val_str = ", ".join("?" for _ in cols)
        sql     = f"INSERT INTO [{schema}].[{table_name}] ({col_str}) VALUES ({val_str})"

        # Determinar el tipo pyodbc correcto para cada columna segun el tipo
        # real del valor Python. Esto respeta fechas, numeros y strings del origen
        # y evita conversiones erroneas en el INSERT.
        input_sizes = []
        for c in cols:
            values   = [row[c] for row in chunk]
            non_null = [v for v in values if v is not None]

            if not non_null:
                input_sizes.append((pyodbc.SQL_WVARCHAR, 255, 0))
                continue

            sample = non_null[0]

            # Nota: pyodbc rechaza precision 0 con HY104 Invalid precision value.
            # Para los tipos de ancho fijo se declara la precision maxima del
            # tipo en vez de 0, aunque el driver la ignore.
            if isinstance(sample, bool):
                input_sizes.append((pyodbc.SQL_WVARCHAR, 10, 0))
            elif isinstance(sample, int):
                input_sizes.append((pyodbc.SQL_BIGINT, 19, 0))    # 19 digitos
            elif isinstance(sample, float):
                input_sizes.append((pyodbc.SQL_DOUBLE, 53, 0))    # 53 bits de mantisa
            elif isinstance(sample, datetime):
                input_sizes.append((pyodbc.SQL_TYPE_TIMESTAMP, 23, 3))
            elif isinstance(sample, date):
                input_sizes.append((pyodbc.SQL_TYPE_DATE, 10, 0))
            else:
                # String - el buffer se dimensiona con el largo real del chunk.
                #
                # SQL Server acepta ColumnSize de 1 a 4000 para SQL_WVARCHAR.
                # Si algun valor supera los 4000 caracteres hay que pasar 0,
                # que es como el driver representa NVARCHAR(MAX): pasarle el
                # largo real (6592, por ejemplo) se rechaza con HY104.
                #
                # El minimo de 1 cubre la columna entera de strings vacios,
                # que mediria 0 y se confundiria con la senal de MAX.
                max_len = max((len(str(v)) for v in non_null), default=1)
                if max_len > MAX_NVARCHAR_BIND:
                    size = NVARCHAR_MAX_BIND
                    logger.debug(
                        "Columna '%s': %d caracteres, se bindea como NVARCHAR(MAX).",
                        c, max_len,
                    )
                else:
                    size = max(1, max_len)
                input_sizes.append((pyodbc.SQL_WVARCHAR, size, 0))

        # Red de seguridad: pyodbc rechaza precision 0 con
        # 'HY104 Invalid precision value'. En vez de confiar en que cada rama
        # de arriba calcule bien, se normaliza aca al final. Asi ninguna rama
        # puede emitir 0, ni las actuales ni las que se agreguen despues.
        seguras = []
        for col, (sql_type, size, digits) in zip(cols, input_sizes):
            # Para SQL_WVARCHAR el 0 es intencional y significa NVARCHAR(MAX).
            # Para el resto de los tipos una precision menor a 1 es invalida.
            if size < 1 and sql_type != pyodbc.SQL_WVARCHAR:
                logger.warning(
                    "La columna '%s' de '%s.%s' calculo precision %s. "
                    "Se ajusta a 1 para evitar HY104.",
                    col, schema, table_name, size,
                )
                size = 1
            seguras.append((sql_type, size, digits))
        input_sizes = seguras

        # Pasar los valores tal cual vienen del origen, sin convertir a string.
        # Los tipos ya estan declarados en setinputsizes.
        rows_as_tuples = [
            tuple(row[c] for c in cols)
            for row in chunk
        ]

        with engine.begin() as conn:
            raw_conn = conn.connection
            cursor   = raw_conn.cursor()
            cursor.fast_executemany = True
            cursor.setinputsizes(input_sizes)
            cursor.executemany(sql, rows_as_tuples)
            cursor.close()

        total_rows += len(chunk)
        logger.debug(
            "Insertadas %d filas en '%s.%s'. Total: %d",
            len(chunk), schema, table_name, total_rows,
        )

    # El origen no devolvio ninguna fila.
    # Con semantica replace, RAW debe reflejar el origen: truncar la tabla.
    # Si no truncaramos, quedarian los datos del run anterior y la auditoria
    # reportaria exito con 0 filas - un falso positivo silencioso.
    if first_chunk:
        with engine.connect() as conn:
            exists = engine.dialect.has_table(conn, table_name, schema=schema)

        if exists:
            _truncate_table(engine, schema, table_name)
            logger.warning(
                "El origen no devolvio filas para '%s.%s'. La tabla quedo vacia "
                "(semantica replace). Verifica la query o el filtro de fecha.",
                schema, table_name,
            )
        else:
            logger.warning(
                "El origen no devolvio filas y la tabla '%s.%s' no existe. "
                "No se creo nada. Verifica la query o el filtro de fecha.",
                schema, table_name,
            )

    return total_rows
