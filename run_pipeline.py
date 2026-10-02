"""
run_pipeline.py
Punto de entrada por linea de comandos.

Uso:
    python run_pipeline.py --source all --all
    python run_pipeline.py --source all --tag diario
    python run_pipeline.py --source hana --all
    python run_pipeline.py --source hana --table HANA_VBAK
    python run_pipeline.py --source hana --tag diario
    python run_pipeline.py --source hanadev --all
    python run_pipeline.py --source hana --all --date-mode yesterday
    python run_pipeline.py --source hana --table SAP_ACDOCA --date-from 20260801 --date-to 20260826
    python run_pipeline.py --source excel --all
    python run_pipeline.py --source postgres --all
    python run_pipeline.py --source oracle --all
    python run_pipeline.py --source csv --all
    python run_pipeline.py --source sqlserver --all
    python run_pipeline.py --source sqlserver2 --all
    python run_pipeline.py --source sqlserver3 --all
"""

import argparse
import logging
import logging.handlers
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# Forzar UTF-8 en la salida estandar.
# En Windows la consola usa cp1252 por defecto: si un orquestador como
# Prefect captura el stdout y lo decodifica como UTF-8, cualquier caracter
# fuera de ASCII (un acento en un mensaje de error del driver, por ejemplo)
# rompe con UnicodeDecodeError. Forzarlo aca hace que lo que se escribe
# y lo que se lee coincidan siempre.
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        # Python < 3.7 o stream sin reconfigure (ej. redirigido a un pipe custom)
        pass

# ---------------------------------------------------------------------------
# Logging: consola + archivo rotativo diario en logs/
# ---------------------------------------------------------------------------
LOGS_DIR = Path(__file__).parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

_formatter = logging.Formatter(
    fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

_console_handler = logging.StreamHandler(sys.stdout)
_console_handler.setFormatter(_formatter)

_file_handler = logging.handlers.TimedRotatingFileHandler(
    filename=LOGS_DIR / "pipeline.log",
    when="midnight",
    backupCount=30,
    encoding="utf-8",
)
_file_handler.setFormatter(_formatter)

logging.basicConfig(
    level=logging.INFO,
    handlers=[_console_handler, _file_handler],
)

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ejecuta el pipeline de ingesta de datos hacia la capa RAW.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--source",
        required=True,
        help=(
            "Nombre del origen a ejecutar, o 'all' para ejecutar todos.\n"
            "Opciones: all, hana, hanadev, excel, postgres, oracle, csv,\n"
            "          sharepoint, sqlserver, sqlserver2, sqlserver3"
        ),
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--all",
        action="store_true",
        dest="run_all",
        help="Ejecuta todas las tablas/archivos habilitados del origen.",
    )
    group.add_argument(
        "--table",
        dest="table_filter",
        metavar="TABLA",
        help="Ejecuta una tabla puntual por su target_table.",
    )
    group.add_argument(
        "--tag",
        dest="tag_filter",
        nargs="+",
        metavar="TAG",
        help=(
            "Ejecuta todas las tablas que contengan al menos uno de los tags.\n"
            "Ejemplos:\n"
            "  --tag diario\n"
            "  --tag ventas compras"
        ),
    )

    parser.add_argument(
        "--date-mode",
        dest="date_mode",
        choices=["full", "today", "yesterday", "last_n_days", "current_month", "last_month", "custom"],
        help="Sobreescribe el mode del date_filter para todas las tablas del run.",
    )
    parser.add_argument(
        "--date-from",
        dest="date_from",
        metavar="FECHA",
        help=(
            "Fecha desde. Acepta 20260826, 2026-08-26 o 2026/08/26.\n"
            "Usar junto con --date-to. No hace falta pasar --date-mode."
        ),
    )
    parser.add_argument(
        "--date-to",
        dest="date_to",
        metavar="FECHA",
        help="Fecha hasta. Mismo formato que --date-from.",
    )

    return parser.parse_args()


# ---------------------------------------------------------------------------
# Funciones runner por origen
# ---------------------------------------------------------------------------

def _run_hana(table_filter=None, tag_filter=None, run_all=False,
              date_mode=None, date_from=None, date_to=None):
    from pipelines.hana_pipeline import run_pipeline
    return run_pipeline(
        source_name="hana",
        table_filter=table_filter,
        tag_filter=tag_filter,
        run_all=run_all,
        override_mode=date_mode,
        override_date_from=date_from,
        override_date_to=date_to,
    )


def _run_hanadev(table_filter=None, tag_filter=None, run_all=False,
                 date_mode=None, date_from=None, date_to=None):
    from pipelines.hana_pipeline import run_pipeline
    return run_pipeline(
        source_name="hanadev",
        table_filter=table_filter,
        tag_filter=tag_filter,
        run_all=run_all,
        override_mode=date_mode,
        override_date_from=date_from,
        override_date_to=date_to,
    )


def _run_excel(table_filter=None, tag_filter=None, run_all=False,
               date_mode=None, date_from=None, date_to=None):
    from pipelines.excel_pipeline import run_pipeline
    return run_pipeline(
        source_name="excel",
        table_filter=table_filter,
        tag_filter=tag_filter,
        run_all=run_all,
    )


def _run_postgres(table_filter=None, tag_filter=None, run_all=False,
                  date_mode=None, date_from=None, date_to=None):
    from pipelines.postgres_pipeline import run_pipeline
    return run_pipeline(
        source_name="postgres",
        table_filter=table_filter,
        tag_filter=tag_filter,
        run_all=run_all,
        override_mode=date_mode,
        override_date_from=date_from,
        override_date_to=date_to,
    )


def _run_oracle(table_filter=None, tag_filter=None, run_all=False,
                date_mode=None, date_from=None, date_to=None):
    from pipelines.oracle_pipeline import run_pipeline
    return run_pipeline(
        source_name="oracle",
        table_filter=table_filter,
        tag_filter=tag_filter,
        run_all=run_all,
        override_mode=date_mode,
        override_date_from=date_from,
        override_date_to=date_to,
    )


def _run_csv(table_filter=None, tag_filter=None, run_all=False,
             date_mode=None, date_from=None, date_to=None):
    from pipelines.csv_pipeline import run_pipeline
    return run_pipeline(
        source_name="csv",
        table_filter=table_filter,
        tag_filter=tag_filter,
        run_all=run_all,
    )


def _run_sharepoint(table_filter=None, tag_filter=None, run_all=False,
                    date_mode=None, date_from=None, date_to=None):
    from pipelines.sharepoint_pipeline import run_pipeline
    return run_pipeline(
        source_name="sharepoint",
        table_filter=table_filter,
        tag_filter=tag_filter,
        run_all=run_all,
    )


def _run_sqlserver(table_filter=None, tag_filter=None, run_all=False,
                   date_mode=None, date_from=None, date_to=None):
    from pipelines.sqlserver_pipeline import run_pipeline
    return run_pipeline(
        source_name="sqlserver",
        table_filter=table_filter,
        tag_filter=tag_filter,
        run_all=run_all,
        override_mode=date_mode,
        override_date_from=date_from,
        override_date_to=date_to,
    )


def _run_sqlserver2(table_filter=None, tag_filter=None, run_all=False,
                    date_mode=None, date_from=None, date_to=None):
    from pipelines.sqlserver_pipeline import run_pipeline
    return run_pipeline(
        source_name="sqlserver2",
        table_filter=table_filter,
        tag_filter=tag_filter,
        run_all=run_all,
        override_mode=date_mode,
        override_date_from=date_from,
        override_date_to=date_to,
    )


def _run_sqlserver3(table_filter=None, tag_filter=None, run_all=False,
                    date_mode=None, date_from=None, date_to=None):
    from pipelines.sqlserver_pipeline import run_pipeline
    return run_pipeline(
        source_name="sqlserver3",
        table_filter=table_filter,
        tag_filter=tag_filter,
        run_all=run_all,
        override_mode=date_mode,
        override_date_from=date_from,
        override_date_to=date_to,
    )


# ---------------------------------------------------------------------------
# Registro de origenes - agregar nuevos aqui
# ---------------------------------------------------------------------------

SOURCE_RUNNERS = {
    "hana":       _run_hana,
    "hanadev":    _run_hanadev,
    "excel":      _run_excel,
    "postgres":   _run_postgres,
    "oracle":     _run_oracle,
    "csv":        _run_csv,
    "sharepoint": _run_sharepoint,
    "sqlserver":  _run_sqlserver,
    "sqlserver2": _run_sqlserver2,
    "sqlserver3": _run_sqlserver3,
}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _run_one_source(source: str, args) -> list:
    """
    Ejecuta un origen y devuelve sus PipelineRun.

    Si el origen no tiene tablas que coincidan con el filtro, devuelve
    lista vacia en lugar de fallar - al correr todos los origenes es
    esperable que algunos no tengan tablas con el tag pedido.
    """
    runner = SOURCE_RUNNERS[source]
    try:
        return runner(
            table_filter=args.table_filter,
            tag_filter=args.tag_filter,
            run_all=args.run_all,
            date_mode=args.date_mode,
            date_from=args.date_from,
            date_to=args.date_to,
        )
    except ValueError as exc:
        # "No se encontro la tabla X" / "Ninguna tabla contiene los tags"
        logger.info("Origen '%s': sin tablas para este filtro (%s).", source, exc)
        return []


def main() -> None:
    args = parse_args()

    # Pasar --date-from y --date-to implica un rango custom.
    # No hace falta escribir --date-mode custom.
    if (args.date_from or args.date_to) and not args.date_mode:
        args.date_mode = "custom"

    if args.date_mode == "custom" and (not args.date_from or not args.date_to):
        logger.error(
            "Un rango de fechas necesita --date-from y --date-to. "
            "Ejemplo: --date-from 20260801 --date-to 20260826"
        )
        sys.exit(1)

    if args.date_mode and args.date_mode != "custom" and (args.date_from or args.date_to):
        logger.error(
            "--date-from y --date-to no se combinan con --date-mode %s. "
            "Usalos solos para definir un rango, o usa --date-mode sin fechas.",
            args.date_mode,
        )
        sys.exit(1)

    # Validar el formato de las fechas antes de conectarse a ningun origen
    if args.date_from and args.date_to:
        from sources.date_filter import parse_date_input
        try:
            desde = parse_date_input(args.date_from, "--date-from")
            hasta = parse_date_input(args.date_to,   "--date-to")
        except ValueError as exc:
            logger.error(str(exc))
            sys.exit(1)

        if desde > hasta:
            logger.error(
                "--date-from (%s) es posterior a --date-to (%s). El rango quedaria vacio.",
                desde, hasta,
            )
            sys.exit(1)

        logger.info("Filtro de fecha: desde %s hasta %s", desde, hasta)

    # Resolver que origenes correr
    if args.source == "all":
        sources = list(SOURCE_RUNNERS)
        logger.info("Ejecutando todos los origenes: %s", ", ".join(sources))
    elif args.source in SOURCE_RUNNERS:
        sources = [args.source]
    else:
        logger.error(
            "Origen '%s' no implementado. Disponibles: %s, all",
            args.source, ", ".join(SOURCE_RUNNERS),
        )
        sys.exit(1)

    # Ejecutar. Un origen que falla no detiene a los demas.
    resultados_por_origen = {}
    for source in sources:
        try:
            resultados_por_origen[source] = _run_one_source(source, args)
        except Exception as exc:
            if len(sources) == 1:
                logger.exception("Error fatal en el pipeline: %s", exc)
                sys.exit(1)
            logger.error("Origen '%s' fallo por completo: %s", source, exc)
            resultados_por_origen[source] = None   # None = el origen no pudo correr

    _log_resumen(resultados_por_origen, multi=len(sources) > 1)

    hubo_errores = any(
        runs is None or any(r.status == "error" for r in runs)
        for runs in resultados_por_origen.values()
    )
    if hubo_errores:
        logger.warning("Hubo errores. Revisa logs/pipeline.log.")
        sys.exit(1)


def _log_resumen(resultados_por_origen: dict, multi: bool) -> None:
    """Imprime el resumen final. Con varios origenes desglosa por origen."""
    if not multi:
        return

    total_ok = total_error = 0
    logger.info("=" * 60)
    logger.info("RESUMEN POR ORIGEN")
    logger.info("=" * 60)

    for source, runs in resultados_por_origen.items():
        if runs is None:
            logger.info("  %-12s no pudo ejecutarse", source)
            continue
        if not runs:
            logger.info("  %-12s sin tablas", source)
            continue

        ok     = sum(1 for r in runs if r.status == "success")
        error  = sum(1 for r in runs if r.status == "error")
        filas  = sum(r.rows_loaded or 0 for r in runs if r.status == "success")
        total_ok    += ok
        total_error += error

        detalle = f"{ok} ok"
        if error:
            detalle += f", {error} con error"
        logger.info("  %-12s %-20s %s filas", source, detalle, f"{filas:,}")

    logger.info("=" * 60)
    logger.info("TOTAL: %d tablas ok, %d con error", total_ok, total_error)
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
