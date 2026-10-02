"""
sources/date_filter.py
Resolucion de filtros de fecha para queries dinamicas.
Calcula date_from y date_to segun el mode configurado en tables.json.
"""

from datetime import datetime, timedelta, date
from typing import Optional, Tuple

# Formatos en los que se puede renderizar date_from / date_to.
# Necesario porque muchos sistemas guardan fechas como texto en vez de
# usar un tipo DATE nativo. Ejemplo tipico: SAP guarda BUDAT, ERDAT, etc.
# como NVARCHAR(8) con formato YYYYMMDD ('20260810').
#
# YYYYMMDD y YYYYMM son lexicograficamente ordenables, asi que la comparacion
# '20260801' <= '20260810' funciona correctamente aunque sea texto.
DATE_FORMATS = {
    "date":     "%Y-%m-%d",   # default - columna DATE nativa o texto ISO
    "iso":      "%Y-%m-%d",   # alias de 'date'
    "YYYYMMDD": "%Y%m%d",     # SAP: '20260810'
    "DDMMYYYY": "%d%m%Y",     # '10082026'
    "YYYYMM":   "%Y%m",       # periodo mensual: '202608'
    "YYYY":     "%Y",         # ejercicio: '2026'
}


# Formatos aceptados al ESCRIBIR una fecha (en la CLI o en el JSON).
# Deliberadamente solo formatos que empiezan por el anio, para evitar la
# ambiguedad entre DD/MM/YYYY y MM/DD/YYYY.
INPUT_FORMATS = ("%Y%m%d", "%Y-%m-%d", "%Y/%m/%d")


def parse_date_input(value: str, label: str = "fecha") -> date:
    """
    Convierte una fecha escrita por el usuario en un objeto date.

    Acepta indistintamente:
        20260826
        2026-08-26
        2026/08/26

    Args:
        value: Fecha como texto.
        label: Nombre del campo, para que el error diga cual fallo.
    """
    value = str(value).strip()
    for fmt in INPUT_FORMATS:
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue

    raise ValueError(
        f"{label} '{value}' no tiene un formato de fecha valido. "
        f"Usa 20260826, 2026-08-26 o 2026/08/26."
    )


def format_date_value(value: date, date_format: str = "date"):
    """
    Renderiza una fecha en el formato que espera la columna del origen.

    Args:
        value:       Fecha a formatear.
        date_format: Clave de DATE_FORMATS ('date', 'YYYYMMDD', ...).

    Returns:
        String con la fecha formateada.
    """
    fmt = DATE_FORMATS.get(date_format)
    if fmt is None:
        raise ValueError(
            f"format '{date_format}' no reconocido en date_filter. "
            f"Validos: {sorted(DATE_FORMATS)}"
        )
    return value.strftime(fmt)


def resolve_date_filter(
    date_filter: dict,
    override_mode: Optional[str] = None,
    override_date_from: Optional[str] = None,
    override_date_to: Optional[str] = None,
) -> Tuple[Optional[date], Optional[date]]:
    """
    Calcula date_from y date_to segun el mode del date_filter.

    Args:
        date_filter:        Configuracion del filtro de fecha del JSON.
        override_mode:      Sobreescribe el mode del JSON desde la CLI.
        override_date_from: Sobreescribe date_from desde la CLI (formato YYYY-MM-DD).
        override_date_to:   Sobreescribe date_to desde la CLI (formato YYYY-MM-DD).

    Returns:
        Tupla (date_from, date_to). Ambos None si mode es "full".
    """
    if not date_filter:
        return None, None

    mode = override_mode or date_filter.get("mode", "full")
    today = datetime.now().date()

    if mode == "full":
        return None, None

    if mode == "today":
        return today, today

    if mode == "yesterday":
        yesterday = today - timedelta(days=1)
        return yesterday, yesterday

    if mode == "last_n_days":
        n = date_filter.get("n_days", 7)
        return today - timedelta(days=n), today

    if mode == "current_month":
        date_from = today.replace(day=1)
        return date_from, today

    if mode == "last_month":
        first_day_this_month = today.replace(day=1)
        last_day_prev_month  = first_day_this_month - timedelta(days=1)
        first_day_prev_month = last_day_prev_month.replace(day=1)
        return first_day_prev_month, last_day_prev_month

    if mode == "custom":
        # Prioridad: override CLI > valores del JSON
        raw_from = override_date_from or date_filter.get("date_from")
        raw_to   = override_date_to   or date_filter.get("date_to")

        if not raw_from or not raw_to:
            raise ValueError(
                "mode 'custom' requiere 'date_from' y 'date_to'. "
                "Definilos en el JSON o via --date-from y --date-to en la CLI."
            )

        parsed_from = parse_date_input(raw_from, "date_from")
        parsed_to   = parse_date_input(raw_to,   "date_to")

        if parsed_from > parsed_to:
            raise ValueError(
                f"date_from ({parsed_from}) es posterior a date_to ({parsed_to}). "
                f"El rango quedaria vacio."
            )

        return parsed_from, parsed_to

    raise ValueError(
        f"mode '{mode}' no reconocido. "
        f"Validos: full, today, yesterday, last_n_days, current_month, last_month, custom"
    )


def build_query(
    source_table: str,
    query: Optional[str],
    date_filter: Optional[dict],
    date_from: Optional[date],
    date_to: Optional[date],
) -> Tuple[str, dict]:
    """
    Construye la query final y sus parametros.

    Casos:
      - query definida + date_filter: inyecta :date_from y :date_to en la query
      - query definida + sin date_filter: usa la query tal cual
      - query null + date_filter: construye SELECT * con WHERE automatico
      - query null + sin date_filter: construye SELECT * sin WHERE

    Args:
        source_table: Nombre de la tabla origen (para construir query automatica).
        query:        Query SQL del JSON (puede ser null).
        date_filter:  Configuracion del filtro de fecha del JSON.
        date_from:    Fecha desde resuelta.
        date_to:      Fecha hasta resuelta.

    Returns:
        Tupla (query_final, params_dict).
    """
    params = {}
    column = date_filter.get("column") if date_filter else None

    # Sin filtro de fecha activo
    if not date_filter or date_from is None:
        if query:
            return query, {}
        return f"SELECT * FROM {source_table}", {}

    # Con filtro de fecha activo.
    # El formato lo define la columna del origen: una columna DATE nativa
    # espera '2026-08-10', pero una NVARCHAR(8) de SAP espera '20260810'.
    date_format = date_filter.get("format", "date")
    params = {
        "date_from": format_date_value(date_from, date_format),
        "date_to":   format_date_value(date_to, date_format),
    }

    if query:
        # Query definida - debe tener :date_from y :date_to como placeholders
        if ":date_from" not in query or ":date_to" not in query:
            raise ValueError(
                f"La query tiene date_filter pero no contiene ':date_from' y ':date_to'. "
                f"Agrega los placeholders o usa query: null para construccion automatica."
            )
        return query, params

    # Query automatica
    if not column:
        raise ValueError(
            "date_filter requiere el campo 'column' cuando query es null. "
            "Ejemplo: \"date_filter\": {\"column\": \"ERDAT\", \"mode\": \"today\"}"
        )

    auto_query = (
        f"SELECT * FROM {source_table} "
        f"WHERE {column} >= :date_from AND {column} <= :date_to"
    )
    return auto_query, params
