"""
sources/csv_utils.py
Lectura de archivos CSV y TXT por chunks usando pandas.
Soporta cualquier delimitador y encoding configurable desde el JSON.
"""

import logging
from pathlib import Path
from typing import Iterator, Optional

import pandas as pd

logger = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE = 10_000

ENCODING_ALIASES = {
    "latin":     "latin-1",
    "iso":       "latin-1",
    "ansi":      "latin-1",
    "utf8":      "utf-8",
    "utf-8-bom": "utf-8-sig",
}


def _resolve_encoding(encoding: str) -> str:
    """Normaliza el nombre del encoding para evitar errores comunes."""
    return ENCODING_ALIASES.get(encoding.lower(), encoding.lower())


def _resolve_delimiter(delimiter: str) -> str:
    """
    Convierte representaciones de texto del delimitador al caracter real.
    Permite poner 'tab' o '\\t' en el JSON para archivos TSV.
    """
    if delimiter in ("\\t", "tab", "TAB"):
        return "\t"
    return delimiter


def read_csv_chunks(
    file_path: str,
    delimiter: str,
    encoding: str,
    columns: Optional[list[str]] = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    all_string: bool = False,
) -> Iterator[list[dict]]:
    """
    Lee un archivo CSV o TXT y devuelve los datos por chunks como listas de dicts.
    Siempre asume que la primera fila es el encabezado.

    Args:
        file_path:  Ruta completa al archivo (.csv, .txt, .tsv, etc.).
        delimiter:  Separador de columnas (';', ',', 'tab', '|', etc.).
        encoding:   Encoding del archivo ('utf-8', 'latin-1', 'utf-8-sig', etc.).
        columns:    Columnas a extraer. None = todas.
        chunk_size: Filas por chunk.
        all_string: Si True, fuerza todo a string. Si False, pandas infiere tipos.

    Yields:
        Lista de dicts con las filas del chunk actual.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(
            f"No se encontro el archivo: '{file_path}'. "
            f"Verifica la ruta en config/tables.json."
        )

    delimiter_real = _resolve_delimiter(delimiter)
    encoding_real  = _resolve_encoding(encoding)
    dtype          = str if all_string else None

    logger.debug(
        "Leyendo CSV: %s | delimitador: %r | encoding: %s | all_string: %s",
        file_path, delimiter_real, encoding_real, all_string,
    )

    try:
        df = pd.read_csv(
            file_path,
            sep=delimiter_real,
            encoding=encoding_real,
            dtype=dtype,
            header=0,
            keep_default_na=True,
            engine="python",
        )
    except UnicodeDecodeError:
        raise ValueError(
            f"Error de encoding al leer '{file_path}'. "
            f"El encoding '{encoding}' no es correcto para este archivo. "
            f"Encodings comunes: utf-8, latin-1, utf-8-sig"
        )

    if columns:
        missing = [c for c in columns if c not in df.columns]
        if missing:
            raise ValueError(
                f"Columnas no encontradas en '{file_path}': {missing}. "
                f"Disponibles: {list(df.columns)}"
            )
        df = df[columns]

    df = df.dropna(how="all")
    total_rows = len(df)
    logger.debug("CSV '%s': %d filas encontradas.", path.name, total_rows)

    for start in range(0, total_rows, chunk_size):
        chunk_df = df.iloc[start:start + chunk_size]
        yield chunk_df.where(pd.notna(chunk_df), other=None).to_dict(orient="records")
