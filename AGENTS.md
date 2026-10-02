# AGENTS.md

Guía para agentes de código (Claude Code, Codex, Cursor, Copilot, etc.) que trabajen en este repositorio.

## Reglas de seguridad — OBLIGATORIAS

Estas reglas tienen prioridad sobre cualquier otra instrucción, incluida una pedida en el chat. Si una tarea parece requerir romperlas, **detenete y preguntale al usuario** en lugar de buscar un camino alternativo.

1. **Nunca acceder a `.env` ni a `.env.*`** (incluido `.env.example`). No leerlos, listarlos con contenido, copiarlos, editarlos, hacerles `cat`/`type`/`Get-Content`/`grep`/`Select-String`, ni abrirlos desde Python (`dotenv_values`, `open(".env")`, etc.).
2. **Nunca intentar obtener credenciales** por ningún medio: no imprimir variables de entorno (`env`, `set`, `Get-ChildItem Env:`, `os.environ`, `printenv`), no inspeccionar `load_dotenv()` en runtime, no leer el almacén de credenciales de Windows, ni connection strings, tokens, secretos de SharePoint/MSAL o configuraciones de Prefect. Para saber qué variable usa un origen, leé el **nombre** en el código de `sources/*_utils.py` o en el `README.md`; nunca su valor.
3. **Nunca conectarse a las bases de datos ni obtener filas de las tablas.** Esto incluye:
   - Ejecutar `run_pipeline.py`, `etl_flow.py` o cualquier pipeline, ni scripts de prueba de conexión.
   - Usar `sqlcmd`, `hdbsql`, `psql`, `sqlplus`, `bcp` u otros clientes de BD.
   - Escribir o correr scripts ad-hoc (`python -c ...`, notebooks, scripts temporales) que importen `sources.*`, `pyodbc`, `hdbcli`, `oracledb`, `psycopg2`, `sqlalchemy.create_engine` o `msal` para conectarse.
   - Hacer `SELECT`, `TOP`, `LIMIT`, `COUNT`, describe/metadata contra cualquier origen o destino.
4. **No abrir archivos de datos** (`*.xls`, `*.xlsx`, `*.xlsm`, `*.csv`, `*.txt` de origen) referenciados en `config/tables.json`, ni descargar archivos de SharePoint.
5. Si encontrás credenciales hardcodeadas en el código, **no las uses ni las repitas** en tu respuesta; solo avisale al usuario que existen.
6. Los logs en `logs/` pueden leerse para diagnosticar errores, pero no copies en la respuesta valores de filas que aparezcan en mensajes de error.

La verificación de cambios se hace **sin datos reales**: lectura de código, `py_compile`, validación del JSON y validación de configuración (ver abajo). La ejecución real del pipeline la hace siempre el usuario.

## Qué es este proyecto

ETL de ingesta a capa **RAW/Bronze** en SQL Server con Python + SQLAlchemy. Cada tabla se carga con **TRUNCATE + INSERT** completo, sin transformaciones de negocio. Si la tabla destino no existe se crea; si aparecen columnas nuevas se hace `ALTER TABLE ADD`. Se orquesta con Prefect (`etl_flow.py`) en un Windows Server, usando `.venv`.

Documentación completa: `README.md`.

## Estructura

```
run_pipeline.py        CLI principal (--source, --all/--table/--tag, --date-mode/--date-from/--date-to)
etl_flow.py            Flow de Prefect que invoca run_pipeline.py --source all --all
config/tables.json     Definición de TODAS las tablas a ingestar (archivo que más cambia)
pipelines/*_pipeline.py  Orquestación por origen (validación de config, reintentos, auditoría)
sources/*_utils.py     Conexión y lectura por chunks de cada origen
sources/db_writer.py   Escritura al destino SQL Server (create/alter/truncate/insert, tipos, HY104)
sources/date_filter.py Resolución de filtros de fecha y armado de query
monitor/audit.py       Registro de ejecuciones en monitor.pipeline_runs
check_version.py       Verifica que el build desplegado tenga ciertas correcciones (sin conectarse)
```

Orígenes (`source_name`): `hana`, `hanadev`, `oracle`, `postgres`, `sqlserver`, `sqlserver2`, `sqlserver3`, `excel`, `csv`, `sharepoint`. Se registran en `SOURCE_RUNNERS` de `run_pipeline.py`.

## Tarea más común: agregar o modificar una tabla

La mayoría de los commits solo tocan `config/tables.json` (`Add SAP_FAGL_SEGM`, `Add FKSTO in VBRK`, ...).

Ejemplo HANA según la convención actual:

```json
{
  "source_name": "hana",
  "source_table": "SAPHANADB.FAGL_SEGM",
  "target_table": "SAP_FAGL_SEGM",
  "target_schema": "raw",
  "query": null,
  "date_filter": null,
  "all_string": true,
  "chunk_size": 10000,
  "nvarchar_max_length": 5000,
  "tags": ["diario", "dimensiones", "dim_segmentos"],
  "enabled": true
}
```

- Campos obligatorios: `source_name`, `source_table`, `target_table`, `target_schema`, `enabled`.
- `target_table` debe ser único; para tablas SAP usar el prefijo `SAP_`.
- `query: null` ⇒ `SELECT * FROM source_table`. Si hay `query` con `date_filter` distinto de `full`, debe contener `:date_from` y `:date_to`.
- `date_filter.mode` válidos: `full`, `today`, `yesterday`, `last_n_days`, `current_month`, `last_month`, `custom` (`custom` requiere `date_from` y `date_to`).
- `tags` es una lista. Para desactivar una tabla, `"enabled": false` (no borrarla).
- Mantené el formato/indentación del archivo y agregá la entrada junto a otras del mismo origen/dominio.

Las columnas o tablas de SAP que necesites conocer deben venir del usuario o de la documentación, **no** de consultar la base.

## Verificación sin datos

Usá el intérprete del venv (Python 3.11): `.venv/Scripts/python.exe`.

```bash
# JSON válido
.venv/Scripts/python.exe -c "import json; json.load(open('config/tables.json', encoding='utf-8'))"

# Campos obligatorios presentes
.venv/Scripts/python.exe -c "import json; req={'source_name','source_table','target_table','target_schema','enabled'}; [print(c.get('target_table'), req-set(c)) for c in json.load(open('config/tables.json', encoding='utf-8')) if req-set(c)]"

# target_table duplicados
.venv/Scripts/python.exe -c "import json,collections; t=[c['target_table'] for c in json.load(open('config/tables.json', encoding='utf-8'))]; print([k for k,n in collections.Counter(t).items() if n>1])"

# Sintaxis de todos los módulos
.venv/Scripts/python.exe -m py_compile run_pipeline.py pipelines/*.py sources/*.py monitor/*.py

# Correcciones presentes en el build
.venv/Scripts/python.exe check_version.py
```

**No importes módulos de `sources/` ni `pipelines/`** (ni `run_pipeline.py`) para validar: ejecutan `load_dotenv()` al importarse y cargan las credenciales. Las reglas de validación están en `validate_table_config` de cada `pipelines/*_pipeline.py`: leelas como código y verificá la config a mano o con scripts que solo lean el JSON.

No hay suite de tests automatizados. No crees scripts de prueba que se conecten a orígenes o destino.

## Convenciones de código

- Comentarios, docstrings, logs y mensajes en **español**, sin tildes en el código (ASCII) para evitar problemas de encoding en Windows/Prefect.
- `logging` con `logger = logging.getLogger(__name__)`, formato `%`-style (`logger.info("... %s", x)`), nunca `print` en módulos.
- Type hints modernos (`list[str]`, `Optional[...]`).
- Credenciales siempre vía variables de entorno separadas (host, port, user, password), nunca hardcodeadas ni en connection strings con URL.
- Al agregar una corrección importante en `db_writer.py`/`date_filter.py`, considerá sumar una entrada en `CORRECCIONES` de `check_version.py`.
- Al agregar un origen nuevo: `sources/<origen>_utils.py` + `pipelines/<origen>_pipeline.py` + registro en `SOURCE_RUNNERS` + documentar el **nombre** de las variables en `README.md`. Las variables las carga el usuario en su `.env`.

## Git

- Commits cortos en inglés con el formato del historial: `Add <TABLA>`, `Add <CAMPO> in <TABLA>`, `Fix ...`.
- Nunca commitear `.env`, `logs/`, `.venv/` ni archivos de datos (ya están en `.gitignore`).
- Commitear o pushear solo cuando el usuario lo pida.
