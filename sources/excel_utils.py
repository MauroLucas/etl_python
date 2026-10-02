"""
sources/excel_utils.py
Lectura de archivos Excel por chunks usando pandas.
Soporta tres formatos:
  - .xlsx / .xlsm                -> openpyxl
  - .xls genuino (Excel 97-2003) -> xlrd
  - .xls SpreadsheetML (XML)     -> lxml (exportaciones web disfrazadas de .xls)

chunk_size: definido por tabla en config/tables.json
all_string: True -> dtype=str | False -> pandas no fuerza tipos
"""

import logging
from pathlib import Path
from typing import Iterator, Optional

import pandas as pd

logger = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE = 10_000


def _is_spreadsheetml(file_path: str) -> bool:
    """Detecta si el archivo es SpreadsheetML (XML con extension .xls)."""
    try:
        with open(file_path, "rb") as f:
            header = f.read(200).decode("utf-8", errors="ignore")
        return "<?xml" in header and "Workbook" in header
    except Exception:
        return False


def _read_spreadsheetml(
    file_path: str,
    sheet_name: str,
    columns: Optional[list],
    all_string: bool,
) -> pd.DataFrame:
    """
    Lee un archivo SpreadsheetML (XML exportado como .xls por sistemas web).
    Usa parser permisivo de lxml para tolerar namespaces malformados.
    """
    from lxml import etree

    ns = "urn:schemas-microsoft-com:office:spreadsheet"

    parser = etree.XMLParser(recover=True, resolve_entities=False)
    tree   = etree.parse(file_path, parser=parser)
    root   = tree.getroot()

    worksheets = root.findall(f".//{{{ns}}}Worksheet")
    target = None
    for ws in worksheets:
        name = ws.get(f"{{{ns}}}Name", "")
        if name == sheet_name:
            target = ws
            break

    if target is None:
        available = [ws.get(f"{{{ns}}}Name", "") for ws in worksheets]
        raise ValueError(
            f"Hoja '{sheet_name}' no encontrada en '{file_path}'. "
            f"Hojas disponibles: {available}"
        )

    rows_data = []
    for row in target.findall(f".//{{{ns}}}Row"):
        row_values = []
        for cell in row.findall(f"{{{ns}}}Cell"):
            data = cell.find(f"{{{ns}}}Data")
            row_values.append(data.text if data is not None else None)
        rows_data.append(row_values)

    if not rows_data:
        return pd.DataFrame()

    headers = [str(h) if h is not None else f"col_{i}" for i, h in enumerate(rows_data[0])]
    df = pd.DataFrame(rows_data[1:], columns=headers)

    if columns:
        missing = [c for c in columns if c not in df.columns]
        if missing:
            raise ValueError(
                f"Columnas no encontradas en '{sheet_name}' de '{file_path}': {missing}. "
                f"Disponibles: {list(df.columns)}"
            )
        df = df[columns]

    return df.astype(str) if all_string else df


def _read_dataframe(
    file_path: str,
    sheet_name: str,
    columns: Optional[list],
    all_string: bool,
) -> pd.DataFrame:
    """
    Lee el archivo con el engine correcto detectado automaticamente.
    all_string=True  -> dtype=str forzado
    all_string=False -> pandas no fuerza tipos
    """
    suffix = Path(file_path).suffix.lower()
    dtype  = str if all_string else None

    if _is_spreadsheetml(file_path):
        logger.debug("Archivo detectado como SpreadsheetML (XML). Parseando con lxml.")
        return _read_spreadsheetml(file_path, sheet_name, columns, all_string)

    if suffix == ".xls":
        logger.debug("Archivo .xls genuino. Usando engine xlrd.")
        return pd.read_excel(
            file_path,
            sheet_name=sheet_name,
            usecols=columns if columns else None,
            dtype=dtype,
            engine="xlrd",
        )

    logger.debug("Archivo .xlsx/.xlsm. Usando engine openpyxl.")
    return pd.read_excel(
        file_path,
        sheet_name=sheet_name,
        usecols=columns if columns else None,
        dtype=dtype,
        engine="openpyxl",
    )


def read_excel_chunks(
    file_path: str,
    sheet_name: str,
    columns: Optional[list[str]] = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    all_string: bool = False,
) -> Iterator[list[dict]]:
    """
    Lee un archivo Excel y devuelve los datos por chunks como listas de dicts.

    Args:
        file_path:  Ruta completa al archivo.
        sheet_name: Nombre de la hoja a leer.
        columns:    Lista de columnas a extraer. None = todas.
        chunk_size: Filas por chunk - viene de la config de cada tabla en el JSON.
        all_string: Si True, fuerza todo a string. Si False, no fuerza tipos.

    Yields:
        Lista de dicts con las filas del chunk actual.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(
            f"No se encontro el archivo Excel: '{file_path}'. "
            f"Verifica la ruta en config/tables.json."
        )

    logger.debug(
        "Leyendo Excel: %s | hoja: %s | chunk_size: %d | all_string: %s",
        file_path, sheet_name, chunk_size, all_string,
    )

    df = _read_dataframe(file_path, sheet_name, columns, all_string)

    if columns:
        missing = [c for c in columns if c not in df.columns]
        if missing:
            raise ValueError(
                f"Columnas no encontradas en '{sheet_name}' de '{file_path}': {missing}. "
                f"Disponibles: {list(df.columns)}"
            )

    df = df.dropna(how="all")
    total_rows = len(df)
    logger.debug("Excel '%s': %d filas encontradas.", path.name, total_rows)

    for start in range(0, total_rows, chunk_size):
        chunk_df = df.iloc[start:start + chunk_size]
        yield chunk_df.where(pd.notna(chunk_df), other=None).to_dict(orient="records")
