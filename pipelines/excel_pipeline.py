"""
pipelines/excel_pipeline.py
Pipeline Excel -> SQL Server RAW.
Siempre truncate + insert via SQLAlchemy.
Control total del schema, tipos y errores.
"""

import json
import logging
import os
import time
from pathlib import Path
from typing import Optional

from monitor.audit import PipelineRun, save_run_to_db
from sources.db_writer import get_destination_engine, write_chunks
from sources.excel_utils import read_excel_chunks, DEFAULT_CHUNK_SIZE

logger = logging.getLogger(__name__)

CONFIG_PATH   = Path(__file__).parent.parent / "config" / "tables.json"
PIPELINE_NAME = os.environ.get("PIPELINE_NAME", "excel_to_raw")

MAX_RETRIES  = 3
RETRY_DELAYS = [5, 10, 20]

REQUIRED_FIELDS = {
    "source_name", "source_table", "sheet_name",
    "target_table", "target_schema", "enabled",
}


def validate_table_config(cfg: dict) -> list[str]:
    errors = []
    for field in REQUIRED_FIELDS:
        if field not in cfg:
            errors.append(f"Campo obligatorio ausente: '{field}'")
    columns = cfg.get("columns")
    if columns is not None and not isinstance(columns, list):
        errors.append("'columns' debe ser una lista o null.")
    if isinstance(columns, list) and len(columns) == 0:
        errors.append("'columns' no puede ser lista vacia. Usa null para traer todas.")
    tags = cfg.get("tags")
    if tags is not None and not isinstance(tags, list):
        errors.append("'tags' debe ser una lista.")
    return errors


def load_tables_config(source_name: str = "excel") -> list[dict]:
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
                "Excel #%d ('%s') skipeado:\n  - %s",
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
) -> PipelineRun:
    """Ejecuta un archivo Excel con reintentos (5, 10, 20 segundos)."""
    target_table   = table_cfg["target_table"]
    last_exception = None

    for attempt in range(MAX_RETRIES):
        if attempt > 0:
            wait = RETRY_DELAYS[attempt - 1]
            logger.warning(
                "Excel '%s': reintento %d/%d en %ds...",
                target_table, attempt + 1, MAX_RETRIES, wait,
            )
            time.sleep(wait)
        try:
            return _run_table(table_cfg, engine, audit_engine)
        except Exception as exc:
            last_exception = exc
            logger.warning(
                "Excel '%s': intento %d/%d fallo: %s",
                target_table, attempt + 1, MAX_RETRIES, exc,
            )

    logger.error("Excel '%s': todos los reintentos agotados.", target_table)
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
) -> PipelineRun:
    """Ejecuta el pipeline para un archivo Excel y registra auditoria."""
    target_table  = table_cfg["target_table"]
    target_schema = table_cfg["target_schema"]
    file_path     = table_cfg["source_table"]
    sheet_name    = table_cfg.get("sheet_name", "Sheet1")
    columns       = table_cfg.get("columns")
    all_string          = table_cfg.get("all_string", False)
    nvarchar_max_length = table_cfg.get("nvarchar_max_length", None)
    chunk_size    = table_cfg.get("chunk_size", DEFAULT_CHUNK_SIZE)
    source_name   = table_cfg["source_name"]

    run = PipelineRun(
        pipeline_name=PIPELINE_NAME,
        source_name=source_name,
        table_name=target_table,
    )

    logger.info(
        "Extrayendo Excel: '%s' | hoja: '%s' | columnas: %s | chunk_size: %d | all_string: %s",
        file_path, sheet_name, columns or "todas", chunk_size, all_string,
    )

    chunks = read_excel_chunks(
        file_path,
        sheet_name,
        columns=columns,
        chunk_size=chunk_size,
        all_string=all_string,
    )

    rows_loaded = write_chunks(engine, target_schema, target_table, chunks, nvarchar_max_length=nvarchar_max_length)

    run.complete(rows_loaded=rows_loaded)
    run.log()
    if audit_engine:
        save_run_to_db(run, audit_engine)
    return run


def run_pipeline(
    source_name: str = "excel",
    table_filter: Optional[str] = None,
    tag_filter: Optional[list[str]] = None,
    run_all: bool = False,
) -> list[PipelineRun]:
    """Punto de entrada principal del pipeline Excel."""
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
        logger.warning("No hay archivos Excel habilitados.")
        return []

    logger.info(
        "Iniciando pipeline '%s' | archivos: %s",
        PIPELINE_NAME, [t["target_table"] for t in enabled],
    )

    engine       = get_destination_engine()
    audit_engine = _get_audit_engine_safe()

    results = []
    for table_cfg in enabled:
        run = run_table_with_retry(table_cfg, engine, audit_engine)
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
