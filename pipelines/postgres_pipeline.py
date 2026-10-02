"""
pipelines/postgres_pipeline.py
Pipeline PostgreSQL -> SQL Server RAW.
Siempre truncate + insert via SQLAlchemy.
"""

import json
import logging
import os
import time
from pathlib import Path
from typing import Optional

from monitor.audit import PipelineRun, save_run_to_db
from sources.date_filter import DATE_FORMATS, build_query, resolve_date_filter
from sources.db_writer import get_destination_engine, write_chunks
from sources.postgres_utils import (
    read_postgres_chunks,
    get_postgres_column_lengths,
    DEFAULT_CHUNK_SIZE,
)

logger = logging.getLogger(__name__)

CONFIG_PATH   = Path(__file__).parent.parent / "config" / "tables.json"
PIPELINE_NAME = os.environ.get("PIPELINE_NAME", "postgres_to_raw")

MAX_RETRIES  = 3
RETRY_DELAYS = [5, 10, 20]

REQUIRED_FIELDS = {
    "source_name", "source_table", "target_table",
    "target_schema", "enabled",
}

VALID_DATE_MODES = {
    "full", "today", "yesterday", "last_n_days",
    "current_month", "last_month", "custom",
}


def validate_table_config(cfg: dict) -> list[str]:
    errors = []
    for field in REQUIRED_FIELDS:
        if field not in cfg:
            errors.append(f"Campo obligatorio ausente: '{field}'")

    date_filter = cfg.get("date_filter")
    query       = cfg.get("query")

    if date_filter:
        mode = date_filter.get("mode", "full")
        if mode not in VALID_DATE_MODES:
            errors.append(f"date_filter.mode '{mode}' invalido. Validos: {VALID_DATE_MODES}")
        if mode == "custom":
            if not date_filter.get("date_from") or not date_filter.get("date_to"):
                errors.append("date_filter.mode 'custom' requiere 'date_from' y 'date_to'.")

        date_format = date_filter.get("format", "date")
        if date_format not in DATE_FORMATS:
            errors.append(
                f"date_filter.format '{date_format}' invalido. "
                f"Validos: {sorted(DATE_FORMATS)}"
            )
        if mode != "full" and not query and not date_filter.get("column"):
            errors.append("date_filter con query:null requiere 'column'.")
        if query and mode != "full":
            if ":date_from" not in query or ":date_to" not in query:
                errors.append("La query con date_filter debe contener ':date_from' y ':date_to'.")

    tags = cfg.get("tags")
    if tags is not None and not isinstance(tags, list):
        errors.append("'tags' debe ser una lista.")

    return errors


def load_tables_config(source_name: str = "postgres") -> list[dict]:
    with open(CONFIG_PATH, encoding="utf-8") as f:
        all_tables = json.load(f)

    if not isinstance(all_tables, list):
        raise ValueError("config/tables.json debe ser un array JSON.")

    valid_tables = []
    for i, cfg in enumerate(all_tables):
        if cfg.get("source_name") != source_name:
            continue
        errors = validate_table_config(cfg)
        if errors:
            logger.warning(
                "Tabla #%d ('%s') skipeada:\n  - %s",
                i + 1, cfg.get("target_table", "sin nombre"),
                "\n  - ".join(errors),
            )
            continue
        valid_tables.append(cfg)

    return valid_tables


def filter_by_tags(tables: list[dict], tags: list[str]) -> list[dict]:
    tags_set = set(tags)
    return [cfg for cfg in tables if set(cfg.get("tags") or []) & tags_set]


def run_table_with_retry(
    table_cfg: dict,
    engine,
    audit_engine=None,
    override_mode: Optional[str] = None,
    override_date_from: Optional[str] = None,
    override_date_to: Optional[str] = None,
) -> PipelineRun:
    """Ejecuta una tabla con reintentos (5, 10, 20 segundos)."""
    target_table   = table_cfg["target_table"]
    last_exception = None

    for attempt in range(MAX_RETRIES):
        if attempt > 0:
            wait = RETRY_DELAYS[attempt - 1]
            logger.warning(
                "Tabla '%s': reintento %d/%d en %ds...",
                target_table, attempt + 1, MAX_RETRIES, wait,
            )
            time.sleep(wait)
        try:
            return _run_table(
                table_cfg, engine, audit_engine,
                override_mode, override_date_from, override_date_to,
            )
        except Exception as exc:
            last_exception = exc
            logger.warning(
                "Tabla '%s': intento %d/%d fallo: %s",
                target_table, attempt + 1, MAX_RETRIES, exc,
            )

    logger.error("Tabla '%s': todos los reintentos agotados.", target_table)
    run = PipelineRun(
        pipeline_name=PIPELINE_NAME,
        source_name=table_cfg["source_name"],
        table_name=target_table,
    )
    run.fail(str(last_exception))
    run.log()
    if audit_engine:
        save_run_to_db(run, audit_engine)
    return run


def _run_table(
    table_cfg: dict,
    engine,
    audit_engine=None,
    override_mode: Optional[str] = None,
    override_date_from: Optional[str] = None,
    override_date_to: Optional[str] = None,
) -> PipelineRun:
    """Ejecuta el pipeline para una tabla y registra auditoria."""
    target_table  = table_cfg["target_table"]
    target_schema = table_cfg["target_schema"]
    source_table  = table_cfg["source_table"]
    source_name   = table_cfg["source_name"]
    query_raw     = table_cfg.get("query")
    all_string          = table_cfg.get("all_string", False)
    nvarchar_max_length = table_cfg.get("nvarchar_max_length", None)
    chunk_size    = table_cfg.get("chunk_size", DEFAULT_CHUNK_SIZE)
    date_filter   = table_cfg.get("date_filter")

    run = PipelineRun(
        pipeline_name=PIPELINE_NAME,
        source_name=source_name,
        table_name=target_table,
    )

    # Resolver filtro de fecha
    date_from, date_to = resolve_date_filter(
        date_filter,
        override_mode=override_mode,
        override_date_from=override_date_from,
        override_date_to=override_date_to,
    )

    # Construir query final
    query, params = build_query(source_table, query_raw, date_filter, date_from, date_to)

    logger.info(
        "Extrayendo tabla '%s' desde PostgreSQL... "
        "(chunk_size=%d, all_string=%s, date_from=%s, date_to=%s)",
        source_table, chunk_size, all_string, date_from, date_to,
    )

    def _chunks():
        for chunk in read_postgres_chunks(query, params=params, chunk_size=chunk_size):
            if all_string:
                chunk = [{k: str(v) if v is not None else None for k, v in row.items()} for row in chunk]
            yield chunk

    # Obtener largos de columnas desde schema de PostgreSQL
    schema_name, table_name_pg = (
        source_table.split(".") if "." in source_table else ("public", source_table)
    )
    col_lengths = get_postgres_column_lengths(schema_name, table_name_pg)

    if col_lengths:
        logger.info(
            "Schema PostgreSQL: %d columnas string con largo definido para '%s'.",
            len(col_lengths), source_table,
        )
    else:
        logger.warning(
            "No se obtuvieron largos de columnas desde el schema de PostgreSQL para '%s'. "
            "Se usara inferencia desde el primer chunk%s.",
            source_table,
            f" con nvarchar_max_length={nvarchar_max_length}" if nvarchar_max_length else " sin tope",
        )

    rows_loaded = write_chunks(engine, target_schema, target_table, _chunks(), col_lengths=col_lengths, nvarchar_max_length=nvarchar_max_length)

    run.complete(rows_loaded=rows_loaded)
    run.log()
    if audit_engine:
        save_run_to_db(run, audit_engine)
    return run


def run_pipeline(
    source_name: str = "postgres",
    table_filter: Optional[str] = None,
    tag_filter: Optional[list[str]] = None,
    run_all: bool = False,
    override_mode: Optional[str] = None,
    override_date_from: Optional[str] = None,
    override_date_to: Optional[str] = None,
) -> list[PipelineRun]:
    """Punto de entrada principal del pipeline PostgreSQL."""
    if not run_all and not table_filter and not tag_filter:
        raise ValueError("Debes pasar --all, --table <nombre> o --tag <tag> [tag2 ...].")

    tables  = load_tables_config(source_name)
    enabled = [t for t in tables if t.get("enabled", False)]

    if table_filter:
        enabled = [t for t in enabled if t["target_table"] == table_filter]
        if not enabled:
            raise ValueError(f"No se encontro la tabla '{table_filter}'. Revisa config/tables.json.")
    elif tag_filter:
        enabled = filter_by_tags(enabled, tag_filter)
        if not enabled:
            raise ValueError(f"Ninguna tabla contiene los tags: {tag_filter}.")

    if not enabled:
        logger.warning("No hay tablas habilitadas para '%s'.", source_name)
        return []

    logger.info(
        "Iniciando pipeline '%s' | tablas: %s",
        PIPELINE_NAME, [t["target_table"] for t in enabled],
    )

    engine       = get_destination_engine()
    audit_engine = _get_audit_engine_safe()

    results = []
    for table_cfg in enabled:
        run = run_table_with_retry(
            table_cfg, engine, audit_engine,
            override_mode=override_mode,
            override_date_from=override_date_from,
            override_date_to=override_date_to,
        )
        results.append(run)

    success = sum(1 for r in results if r.status == "success")
    errors  = sum(1 for r in results if r.status == "error")
    logger.info(
        "Pipeline finalizado | exito: %d | errores: %d | total: %d",
        success, errors, len(results),
    )
    return results


def _get_audit_engine_safe():
    try:
        from monitor.audit import ensure_audit_table
        engine = get_destination_engine()
        ensure_audit_table(engine)
        return engine
    except Exception as exc:
        logger.warning("Auditoria en BD deshabilitada: %s", exc)
        return None
