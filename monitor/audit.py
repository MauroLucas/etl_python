"""
monitor/audit.py
Registro de ejecuciones del pipeline.
- Loguea siempre por consola.
- Opcionalmente persiste en monitor.pipeline_runs (SQL Server).
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

# DDL para crear la tabla de auditoria si no existe
_CREATE_AUDIT_TABLE_SQL = """
IF NOT EXISTS (
    SELECT 1 FROM sys.schemas WHERE name = 'monitor'
)
EXEC('CREATE SCHEMA monitor');

IF NOT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'monitor' AND table_name = 'pipeline_runs'
)
CREATE TABLE monitor.pipeline_runs (
    id              INT IDENTITY(1,1) PRIMARY KEY,
    pipeline_name   NVARCHAR(200)  NOT NULL,
    source_name     NVARCHAR(100)  NOT NULL,
    table_name      NVARCHAR(200)  NOT NULL,
    started_at      DATETIME2      NOT NULL,
    finished_at     DATETIME2,
    status          NVARCHAR(20)   NOT NULL,   -- 'success' | 'error' | 'running'
    rows_loaded     INT,
    error_message   NVARCHAR(MAX)
);
"""

_INSERT_RUN_SQL = """
INSERT INTO monitor.pipeline_runs
    (pipeline_name, source_name, table_name, started_at, finished_at,
     status, rows_loaded, error_message)
VALUES
    (:pipeline_name, :source_name, :table_name, :started_at, :finished_at,
     :status, :rows_loaded, :error_message)
"""


@dataclass
class PipelineRun:
    """Representa una ejecucion individual de una tabla en el pipeline."""
    pipeline_name: str
    source_name:   str
    table_name:    str
    started_at:    datetime = field(default_factory=datetime.now)
    finished_at:   Optional[datetime] = None
    status:        str = "running"
    rows_loaded:   Optional[int] = None
    error_message: Optional[str] = None

    def complete(self, rows_loaded: int) -> None:
        self.finished_at = datetime.now()
        self.status      = "success"
        self.rows_loaded = rows_loaded

    def fail(self, error: str) -> None:
        self.finished_at  = datetime.now()
        self.status       = "error"
        self.error_message = str(error)[:2000]  # truncar mensajes muy largos

    def log(self) -> None:
        """Imprime el resultado de la ejecucion en el logger."""
        duration = (
            (self.finished_at - self.started_at).total_seconds()
            if self.finished_at else None
        )
        if self.status == "success":
            logger.info(
                "[AUDIT] OK    %s | tabla: %s | filas: %s | duracion: %.1fs",
                self.source_name, self.table_name, self.rows_loaded, duration or 0,
            )
        else:
            logger.error(
                "[AUDIT] ERROR %s | tabla: %s | error: %s | duracion: %.1fs",
                self.source_name, self.table_name, self.error_message, duration or 0,
            )


def ensure_audit_table(engine) -> None:
    """
    Crea el schema 'monitor' y la tabla 'pipeline_runs' si no existen.
    Llamar una sola vez al inicio del pipeline.
    """
    from sqlalchemy import text
    try:
        with engine.begin() as conn:
            for stmt in _CREATE_AUDIT_TABLE_SQL.strip().split(";"):
                stmt = stmt.strip()
                if stmt:
                    conn.execute(text(stmt))
        logger.info("Tabla de auditoria monitor.pipeline_runs verificada/creada.")
    except Exception as exc:
        logger.warning("No se pudo crear la tabla de auditoria: %s", exc)


def save_run_to_db(run: PipelineRun, engine) -> None:
    """
    Persiste un PipelineRun en monitor.pipeline_runs.
    Si falla, solo loguea el error (no interrumpe el pipeline).
    """
    from sqlalchemy import text
    try:
        with engine.begin() as conn:
            conn.execute(
                text(_INSERT_RUN_SQL),
                {
                    "pipeline_name": run.pipeline_name,
                    "source_name":   run.source_name,
                    "table_name":    run.table_name,
                    "started_at":    run.started_at,
                    "finished_at":   run.finished_at,
                    "status":        run.status,
                    "rows_loaded":   run.rows_loaded,
                    "error_message": run.error_message,
                },
            )
    except Exception as exc:
        logger.warning("No se pudo guardar auditoria en BD: %s", exc)
