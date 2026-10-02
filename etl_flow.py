"""
Flow de Prefect que ejecuta run_pipeline.py del proyecto C:\\etl_python
usando el intérprete de Python del entorno virtual .venv (Windows Server).
"""

import subprocess
from pathlib import Path

from prefect import flow, task, get_run_logger

# --- Configuración del proyecto ---
PROJECT_DIR = Path(r"C:\etl_python")
VENV_PYTHON = PROJECT_DIR / ".venv" / "Scripts" / "python.exe"
SCRIPT = "run_pipeline.py"
ARGS = ["--source", "all", "--all"]


@task(name="ejecutar-pipeline", retries=0)
def run_pipeline():
    logger = get_run_logger()

    if not VENV_PYTHON.exists():
        raise FileNotFoundError(f"No se encontró el python del venv en: {VENV_PYTHON}")

    cmd = [str(VENV_PYTHON), SCRIPT, *ARGS]
    logger.info(f"Ejecutando: {' '.join(cmd)} (cwd={PROJECT_DIR})")

    result = subprocess.run(
        cmd,
        cwd=str(PROJECT_DIR),
        capture_output=True,
        text=True,
        shell=False,
    )

    if result.stdout:
        logger.info(result.stdout)
    if result.stderr:
        logger.warning(result.stderr)

    if result.returncode != 0:
        raise RuntimeError(
            f"El pipeline terminó con código de salida {result.returncode}"
        )

    logger.info("Pipeline finalizado correctamente.")
    return result.returncode


@flow(name="etl-pipeline-flow", log_prints=True)
def etl_pipeline_flow():
    run_pipeline()


if __name__ == "__main__":
    # Permite probar el flow localmente: python etl_flow.py
    etl_pipeline_flow()