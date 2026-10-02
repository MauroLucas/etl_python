# etl_python — Pipeline de Ingesta RAW

Pipeline de datos profesional para ingestar tablas desde múltiples orígenes hacia una capa RAW/Bronze en SQL Server, usando Python + SQLAlchemy.

---

## Índice

1. [Arquitectura](#arquitectura)
2. [Estructura del proyecto](#estructura-del-proyecto)
3. [Instalación](#instalación)
4. [Configuración de credenciales](#configuración-de-credenciales)
5. [Configuración de tablas](#configuración-de-tablas)
6. [Ejecución del pipeline](#ejecución-del-pipeline)
7. [Cómo agregar una nueva tabla](#cómo-agregar-una-nueva-tabla)
8. [Cómo agregar un nuevo origen](#cómo-agregar-un-nuevo-origen)
9. [Cómo funciona la capa RAW](#cómo-funciona-la-capa-raw)
10. [Orígenes soportados](#orígenes-soportados)
11. [Tipos de datos](#tipos-de-datos)
12. [Filtros de fecha](#filtros-de-fecha)
13. [Auditoría](#auditoría)
14. [Manejo de errores y reintentos](#manejo-de-errores-y-reintentos)
15. [Logs](#logs)
16. [Buenas prácticas](#buenas-prácticas)

---

## Arquitectura

```
SAP HANA / Excel
       │
       ▼
  sources/               ← Extracción y lectura por chunks
       │
       ▼
  db_writer.py           ← TRUNCATE + INSERT via SQLAlchemy
       │
       ▼
  SQL Server: schema raw ← Solo tus tablas de negocio
```

La capa RAW es **sólo ingesta** — sin transformaciones de negocio. Cada ejecución hace un **TRUNCATE + INSERT** completo de la tabla. Las transformaciones se realizan después con SQL directo sobre las tablas raw.

---

## Estructura del proyecto

```
etl_python/
├── run_pipeline.py                  ← CLI principal — punto de entrada
├── config/
│   └── tables.json                  ← Definición de todas las tablas a ingestar
├── pipelines/
│   ├── hana_pipeline.py             ← Orquestación SAP HANA
│   ├── oracle_pipeline.py           ← Orquestación Oracle
│   ├── postgres_pipeline.py         ← Orquestación PostgreSQL
│   ├── sqlserver_pipeline.py        ← Orquestación SQL Server (multi-servidor)
│   ├── excel_pipeline.py            ← Orquestación Excel
│   └── csv_pipeline.py              ← Orquestación CSV/TXT
├── sources/
│   ├── hana_utils.py                ← Conexión SAP HANA + lectura por chunks
│   ├── oracle_utils.py              ← Conexión Oracle + lectura por chunks
│   ├── postgres_utils.py            ← Conexión PostgreSQL + lectura por chunks
│   ├── sqlserver_utils.py           ← Conexión SQL Server + lectura por chunks
│   ├── excel_utils.py               ← Lectura de archivos Excel
│   ├── csv_utils.py                 ← Lectura de archivos CSV/TXT
│   ├── db_writer.py                 ← Escritura hacia SQL Server (TRUNCATE + INSERT)
│   └── date_filter.py               ← Resolución de filtros de fecha dinámicos
├── monitor/
│   └── audit.py                     ← Registro de ejecuciones en monitor.pipeline_runs
├── logs/                            ← Logs rotativos diarios (generado automáticamente)
├── .env.example                     ← Plantilla de credenciales
├── .gitignore
├── requirements.txt
└── README.md
```

---

## Instalación

### 1. Clonar el repositorio

```bash
git clone https://tu-repo/etl_python.git
cd etl_python
```

### 2. Crear entorno virtual

```bash
python -m venv .venv
.venv\Scripts\activate      # Windows
# source .venv/bin/activate  # Linux/Mac
```

### 3. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 4. Verificar driver ODBC para SQL Server

```powershell
Get-OdbcDriver | Where-Object { $_.Name -like "*SQL Server*" } | Select-Object Name
```

Si no está instalado: https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server

**El pipeline detecta automáticamente el driver instalado**, prefiriendo el más nuevo disponible:

| Prioridad | Driver |
|---|---|
| 1 | ODBC Driver 18 for SQL Server |
| 2 | ODBC Driver 17 for SQL Server |
| 3 | SQL Server Native Client 11.0 |
| 4 | SQL Server (legacy) |

No hace falta configurar nada. Si necesitás forzar un driver específico, definilo en el `.env`:

```env
DESTINATION_DRIVER=ODBC Driver 17 for SQL Server
SQLSERVER_DRIVER=ODBC Driver 17 for SQL Server
```

---

## Configuración de credenciales

### Paso 1 — Copiar el archivo de ejemplo

```bash
copy .env.example .env
```

### Paso 2 — Completar con valores reales

```env
# SAP HANA — origen (source_name: hana)
HANA_HOST=MI_SERVIDOR_HANA
HANA_PORT=30015
HANA_DATABASE=MI_DATABASE
HANA_USERNAME=mi_usuario
HANA_PASSWORD=mi_password

# SAP HANA — segunda instancia (source_name: hanadev)
HANADEV_HOST=MI_SERVIDOR_HANA_DEV
HANADEV_PORT=30015
HANADEV_DATABASE=MI_DATABASE_DEV
HANADEV_USERNAME=mi_usuario
HANADEV_PASSWORD=mi_password

# SQL Server — destino (capa RAW)
DESTINATION_HOST=MI_SERVIDOR_DESTINO
DESTINATION_PORT=1433
DESTINATION_DATABASE=EDW
DESTINATION_USERNAME=mi_usuario
DESTINATION_PASSWORD="mi_password#123"
```

> ⚠️ **NUNCA commitear `.env` al repositorio.** Ya está en `.gitignore`.

### Passwords con caracteres especiales

Las credenciales de SQL Server (origen y destino) van en **variables separadas**, no como connection string. Esto evita dos problemas:

1. **`python-dotenv` corta en el `#`** — interpreta todo lo que sigue como comentario
2. **La URL corta en el `#`** — lo interpreta como inicio de fragmento

Si la password contiene `#`, usar comillas dobles en el `.env`:

```env
DESTINATION_PASSWORD="miPassword#123"
```

El pipeline conecta usando `pyodbc` directamente (via `creator` de SQLAlchemy), así la password se pasa tal cual sin URL encoding. Funciona con `#`, `@`, `%`, `ñ` y cualquier otro carácter.

### Compatibilidad legacy

Si preferís usar una connection string completa, el pipeline la acepta como fallback cuando no están definidas las variables individuales:

```env
DESTINATION_CONNECTION_STRING="mssql+pyodbc://usuario:password@servidor,1433/EDW?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
```

Este modo es más frágil con caracteres especiales — se recomiendan las variables separadas.

---

## Configuración de tablas

Todas las tablas se definen en `config/tables.json`.

### Campos comunes

| Campo | Tipo | Requerido | Descripción |
|---|---|---|---|
| `source_name` | string | ✅ | Origen: `hana`, `excel` |
| `source_table` | string | ✅ | Tabla origen (HANA) o ruta al archivo (Excel) |
| `target_table` | string | ✅ | Nombre de la tabla en destino RAW |
| `target_schema` | string | ✅ | Schema destino (normalmente `raw`) |
| `all_string` | boolean | ❌ | `true`: todo NVARCHAR. `false` (default): tipos nativos |
| `chunk_size` | integer | ❌ | Filas por chunk. Default: 10000 |
| `nvarchar_max_length` | integer | ❌ | Tope máximo para columnas string inferidas. `null` = sin tope |
| `tags` | array | ❌ | Etiquetas para filtrar ejecuciones |
| `enabled` | boolean | ✅ | `true`: se ejecuta. `false`: se omite |

### Campos específicos de SAP HANA

| Campo | Tipo | Requerido | Descripción |
|---|---|---|---|
| `query` | string o null | ❌ | Query SQL. `null` = `SELECT * FROM source_table` |
| `date_filter` | objeto o null | ❌ | Filtro de fecha dinámico |

### Campos específicos de Excel

| Campo | Tipo | Requerido | Descripción |
|---|---|---|---|
| `sheet_name` | string | ✅ | Nombre de la hoja a leer |
| `columns` | array o null | ❌ | Columnas a extraer. `null` = todas |

### Campos específicos de CSV/TXT

| Campo | Tipo | Requerido | Descripción |
|---|---|---|---|
| `delimiter` | string | ✅ | Separador: `;`, `,`, `\|`, `tab` |
| `encoding` | string | ✅ | `utf-8`, `latin-1`, `utf-8-sig` |
| `columns` | array o null | ❌ | Columnas a extraer. `null` = todas |

### Campos específicos de Oracle, PostgreSQL y SQL Server

Mismos campos que SAP HANA: `query` y `date_filter` opcionales.

### Ejemplo completo — SAP HANA

```json
{
  "source_name": "hana",
  "source_table": "SAPDB.MARA",
  "target_table": "HANA_MARA",
  "target_schema": "raw",
  "query": "SELECT MATNR, MAKTX, MEINS FROM SAPDB.MARA",
  "date_filter": null,
  "all_string": false,
  "chunk_size": 5000,
  "tags": ["diario", "materiales"],
  "enabled": true
}
```

### Ejemplo completo — Excel

```json
{
  "source_name": "excel",
  "source_table": "C:\\reportes\\ventas.xls",
  "sheet_name": "Sheet",
  "columns": ["Fecha", "Cliente", "Importe"],
  "target_table": "EXCEL_VENTAS",
  "target_schema": "raw",
  "all_string": false,
  "chunk_size": 10000,
  "tags": ["mensual"],
  "enabled": true
}
```

---

## Ejecución del pipeline

### Ejecutar todos los orígenes de una vez

```bash
python run_pipeline.py --source all --all
```

Recorre los diez orígenes en orden. Un origen que falla no detiene a los demás, y al final muestra un resumen desglosado:

```
============================================================
RESUMEN POR ORIGEN
============================================================
  hana         2 ok                 81,200 filas
  hanadev      sin tablas
  excel        1 ok                 7,071 filas
  postgres     1 ok, 1 con error    450 filas
  oracle       no pudo ejecutarse
  sqlserver    1 ok                 15,300 filas
============================================================
TOTAL: 6 tablas ok, 1 con error
============================================================
```

Se combina con cualquier filtro:

```bash
# todas las tablas con tag "diario", de todos los orígenes
python run_pipeline.py --source all --tag diario

# rango de fechas aplicado a todos los orígenes
python run_pipeline.py --source all --all --date-from 20260801 --date-to 20260826

# buscar una tabla sin acordarte a qué origen pertenece
python run_pipeline.py --source all --table SAP_ACDOCA
```

Los orígenes que no tengan tablas para el filtro se saltean con un `sin tablas` en el resumen, no fallan.

El **exit code es 1** si alguna tabla falló o si algún origen no pudo ejecutarse, así que sirve para encadenar en un scheduler.

### Ejecutar un origen puntual

```bash
# Todas las tablas habilitadas de un origen
python run_pipeline.py --source hana --all
python run_pipeline.py --source hanadev --all
python run_pipeline.py --source oracle --all
python run_pipeline.py --source postgres --all
python run_pipeline.py --source sqlserver --all
python run_pipeline.py --source sqlserver2 --all
python run_pipeline.py --source sqlserver3 --all
python run_pipeline.py --source excel --all
python run_pipeline.py --source csv --all

# Una tabla puntual
python run_pipeline.py --source hana --table HANA_MARA

# Por tags (OR — ejecuta tablas que tengan al menos uno)
python run_pipeline.py --source hana --tag diario
python run_pipeline.py --source hana --tag ventas compras

# Con filtro de fecha (solo bases de datos, no archivos)
python run_pipeline.py --source hana --all --date-mode yesterday
python run_pipeline.py --source oracle --table ORA_VENTAS --date-mode custom --date-from 2026-01-01 --date-to 2026-01-31
```

Orígenes disponibles: `hana`, `hanadev`, `oracle`, `postgres`, `sqlserver`, `sqlserver2`, `sqlserver3`, `excel`, `csv`, `sharepoint`

---

## Cómo agregar una nueva tabla

### SAP HANA

```json
{
  "source_name": "hana",
  "source_table": "SCHEMA.NOMBRE_TABLA",
  "target_table": "NOMBRE_EN_DESTINO",
  "target_schema": "raw",
  "query": null,
  "date_filter": null,
  "all_string": false,
  "chunk_size": 10000,
  "tags": ["diario"],
  "enabled": true
}
```

### Excel

```json
{
  "source_name": "excel",
  "source_table": "C:\\ruta\\completa\\archivo.xlsx",
  "sheet_name": "Hoja1",
  "columns": ["Col1", "Col2", "Col3"],
  "target_table": "NOMBRE_EN_DESTINO",
  "target_schema": "raw",
  "all_string": false,
  "chunk_size": 10000,
  "tags": ["mensual"],
  "enabled": true
}
```

Para **deshabilitar** una tabla sin borrarla: `"enabled": false`.

---

## Cómo agregar un nuevo origen

1. Crear `sources/nuevo_origen_utils.py` — conexión y lectura por chunks
2. Crear `pipelines/nuevo_origen_pipeline.py` — orquestación
3. Registrar en `run_pipeline.py`:

```python
source_runners = {
    "hana":          _run_hana,
    "excel":         _run_excel,
    "nuevo_origen":  _run_nuevo_origen,
}
```

4. Agregar credenciales en `.env`
5. Agregar tablas en `config/tables.json` con `"source_name": "nuevo_origen"`

---

## Cómo funciona la capa RAW

### TRUNCATE + INSERT

Cada ejecución hace:
1. **Verifica** si la tabla existe en destino
2. Si no existe → **CREATE TABLE** automático
3. Si existe → verifica columnas nuevas → **ALTER TABLE** si las hay
4. **TRUNCATE** de la tabla
5. **INSERT** de todos los registros en chunks

### Creación automática de tablas

Las tablas se crean automáticamente en el primer run inferiendo los tipos desde los datos del primer chunk. No hace falta crear nada manualmente en SQL Server.

### Columnas nuevas

Si el origen agrega una columna que no existe en destino, el pipeline ejecuta un `ALTER TABLE ADD COLUMN` automáticamente antes de insertar.

### Ensanchado automático de columnas

Si un chunk trae un valor más largo que el ancho de la columna en destino, el pipeline ejecuta un `ALTER TABLE ALTER COLUMN` para ensancharla antes de insertar. Ensanchar nunca pierde datos, así que es seguro hacerlo automáticamente.

El nuevo ancho se redondea al siguiente nivel estándar para no hacer un `ALTER` por cada byte de crecimiento:

```
requiere   52 chars → NVARCHAR(255)
requiere  256 chars → NVARCHAR(500)
requiere 1328 chars → NVARCHAR(2000)
requiere 6592 chars → NVARCHAR(MAX)
```

Esto evita el error `String or binary data would be truncated` cuando el archivo o la tabla origen crece con valores más largos que los del primer run. Queda registrado en el log:

```
Columna 'raw.MI_TABLA.Observaciones' ensanchada de NVARCHAR(255) a NVARCHAR(500)
(el dato requeria 312 caracteres).
```

Si la columna tiene un índice o constraint que impide el `ALTER`, se registra un warning y el pipeline continúa — en ese caso hay que ampliarla manualmente.

---

## Orígenes soportados

### SAP HANA

- **Driver**: `hdbcli` (driver nativo de SAP)
- **Tipos**: respeta los tipos nativos de HANA
- Consulta largos desde `SYS.TABLE_COLUMNS`
- Soporta **múltiples instancias** (producción, desarrollo, etc.)

Cada instancia tiene su propio `source_name` y su prefijo de variables en el `.env`:

| `source_name` | Prefijo en `.env` |
|---|---|
| `hana` | `HANA_*` |
| `hanadev` | `HANADEV_*` |

```json
{ "source_name": "hana",    "source_table": "SAPHANADB.MARA", ... }
{ "source_name": "hanadev", "source_table": "SAPHANADB.MARA", ... }
```

Para agregar otra instancia, agregar la entrada en `sources/hana_utils.py`:

```python
HANA_ENV_PREFIX = {
    "hana":     "HANA",
    "hanadev":  "HANADEV",
    "hanaqas":  "HANAQAS",   # nueva
}
```

Registrar el runner en `run_pipeline.py` y agregar las variables `HANAQAS_*` en el `.env`.

### Excel

Soporta tres variantes automáticamente:

| Formato | Extensión | Cómo se detecta | Engine |
|---|---|---|---|
| Excel moderno | `.xlsx`, `.xlsm` | extensión | openpyxl |
| Excel clásico | `.xls` binario | magic bytes | xlrd |
| SpreadsheetML | `.xls` XML | cabecera `<?xml` | lxml |

SpreadsheetML es el formato que generan muchos sistemas web al exportar "a Excel". Se detecta y parsea automáticamente.

### CSV / TXT

- **Delimitador configurable** por tabla: `;`, `,`, `|`, `tab`
- **Encoding configurable** por tabla: `utf-8`, `latin-1`, `utf-8-sig`
- Siempre asume que la primera fila es el encabezado
- Extensiones soportadas: `.csv`, `.txt`, `.tsv`, `.tab`

```json
{
  "source_name": "csv",
  "source_table": "C:\\reportes\\ventas.csv",
  "delimiter": ";",
  "encoding": "latin-1",
  "columns": null,
  "target_table": "CSV_VENTAS",
  "target_schema": "raw",
  "all_string": false,
  "chunk_size": 10000,
  "tags": ["diario"],
  "enabled": true
}
```

### SharePoint Online

Descarga archivos de una biblioteca de documentos vía **Microsoft Graph API** y los procesa como Excel o CSV según la extensión.

**Requisito previo:** una app registrada en Entra ID (Azure AD) con permiso de aplicación `Sites.Read.All` y consentimiento del administrador.

```env
SHAREPOINT_TENANT_ID=00000000-0000-0000-0000-000000000000
SHAREPOINT_CLIENT_ID=00000000-0000-0000-0000-000000000000
SHAREPOINT_CLIENT_SECRET="mi_client_secret"
SHAREPOINT_HOST=miempresa.sharepoint.com
SHAREPOINT_SITE_PATH=/sites/PowerBI
# SHAREPOINT_DRIVE_NAME=Documentos    # opcional
```

Campos específicos en `tables.json`:

| Campo | Requerido | Descripción |
|---|---|---|
| `source_table` | ✅ | Ruta del archivo **dentro de la biblioteca**, ej: `Planes/Planes_ARG.xlsx` |
| `sheet_name` | Solo Excel | Hoja a leer. Los `.csv`/`.txt` no lo necesitan |
| `columns` | ❌ | Columnas a extraer. `null` = todas |
| `site_path` | ❌ | Otro sitio de SharePoint, si difiere del `.env` |
| `delimiter` / `encoding` | Solo CSV | Por defecto `;` y `utf-8` |

```json
{
  "source_name": "sharepoint",
  "source_table": "Planes/Planes_ARG.xlsx",
  "sheet_name": "Cantidades",
  "columns": null,
  "target_table": "EXCEL_Planes_ARG_Cantidades",
  "target_schema": "raw",
  "all_string": true,
  "chunk_size": 10000,
  "tags": ["planes", "mensual"],
  "enabled": true
}
```

```bash
python run_pipeline.py --source sharepoint --all
python run_pipeline.py --source sharepoint --tag planes
```

**Detalles de implementación:**

- El archivo se descarga a un temporal y la lectura se delega en `excel_utils` o `csv_utils` según la extensión, así hereda toda la detección de formato (`.xlsx`, `.xls` binario, `.xls` SpreadsheetML, `.csv`). El temporal se borra siempre, incluso si la carga falla.
- El token y los ids de sitio se cachean por proceso: aunque proceses veinte archivos, autentica una sola vez.
- **La ruta es relativa a la biblioteca**, sin incluir su nombre. Si el archivo está en `Documentos/Planes/Planes_ARG.xlsx`, en el JSON va `Planes/Planes_ARG.xlsx`.
- Varias hojas del mismo archivo son entradas separadas del JSON, cada una con su `target_table`.

### PostgreSQL

- **Driver**: `psycopg2-binary`
- **Tipos**: respeta los tipos nativos de PostgreSQL
- **Credenciales**: variables `POSTGRES_*` en `.env`
- Consulta largos desde `information_schema.columns`

### Oracle

- **Driver**: `oracledb` en modo thick (requiere Instant Client)
- **Tipos**: respeta los tipos nativos de Oracle
- **Credenciales**: variables `ORACLE_*` en `.env` incluyendo `ORACLE_CLIENT_DIR`
- Consulta largos desde `ALL_TAB_COLUMNS`
- Nombres de tablas y columnas en **mayúsculas**

### SQL Server (origen)

Soporta múltiples servidores SQL Server como orígenes independientes.

- **Driver**: `pyodbc` via `creator` de SQLAlchemy
- **Tipos**: respeta los tipos nativos de SQL Server
- **Credenciales**: variables separadas por servidor en `.env`
- Consulta largos desde `INFORMATION_SCHEMA.COLUMNS`

```env
# Servidor 1 (source_name: sqlserver)
SQLSERVER_HOST=servidor1
SQLSERVER_PORT=1433
SQLSERVER_DATABASE=base1
SQLSERVER_USERNAME=usuario
SQLSERVER_PASSWORD="password#con#especiales"

# Servidor 2 (source_name: sqlserver2)
SQLSERVER2_HOST=servidor2
SQLSERVER2_PORT=1433
SQLSERVER2_DATABASE=base2
SQLSERVER2_USERNAME=usuario
SQLSERVER2_PASSWORD="otra#password"

# Servidor 3 (source_name: sqlserver3)
SQLSERVER3_HOST=servidor3
SQLSERVER3_PORT=1433
SQLSERVER3_DATABASE=base3
SQLSERVER3_USERNAME=usuario
SQLSERVER3_PASSWORD="tercera#password"
```

Las credenciales van separadas (no como connection string) para soportar caracteres especiales en la password sin problemas de encoding. Usar comillas dobles en el `.env` si la password contiene `#`.

Para agregar otro servidor, agregar la entrada en `sources/sqlserver_utils.py`:

```python
SQLSERVER_ENV_PREFIX = {
    "sqlserver":  "SQLSERVER",
    "sqlserver2": "SQLSERVER2",
    "sqlserver3": "SQLSERVER3",
    "sqlserver4": "SQLSERVER4",   # nuevo
}
```

Registrar el runner en `run_pipeline.py` y agregar las variables `SQLSERVER4_*` en el `.env`.

---

## Tipos de datos

Los tipos se infieren automáticamente desde los valores Python que llegan del origen:

| Python | SQL Server |
|---|---|
| `int` | `BIGINT` |
| `float` | `FLOAT` |
| `date` | `DATE` |
| `datetime` | `DATETIME2` |
| `str` (≤ 255 chars) | `NVARCHAR(255)` |
| `str` (≤ 500 chars) | `NVARCHAR(500)` |
| `str` (> 500 chars) | `NVARCHAR(MAX)` |

El largo de NVARCHAR se determina con esta prioridad:

1. **Schema del origen** (HANA, Oracle, PostgreSQL) → largo exacto definido en el sistema origen
2. **Inferencia desde el primer chunk** → mide el valor más largo y redondea al nivel más cercano (255, 500, 1000, 2000, 4000, MAX)
3. **Columna completamente nula** → usa `nvarchar_max_length` como fallback o `NVARCHAR(MAX)` si no está definido

### `nvarchar_max_length`

Define el tope máximo para strings inferidos desde el chunk (no aplica cuando el largo viene del schema origen):

```json
"nvarchar_max_length": 500
```

| Escenario | Sin `nvarchar_max_length` | Con `nvarchar_max_length: 500` |
|---|---|---|
| Columna de HANA `NVARCHAR(6)` | `NVARCHAR(6)` | `NVARCHAR(500)` |
| Columna de HANA `NVARCHAR(100)` | `NVARCHAR(100)` | `NVARCHAR(500)` |
| Columna de HANA `NVARCHAR(800)` | `NVARCHAR(800)` | `NVARCHAR(500)` |
| Sin schema, chunk max 45 chars | `NVARCHAR(255)` | `NVARCHAR(500)` |
| Sin schema, chunk max 800 chars | `NVARCHAR(1000)` | `NVARCHAR(500)` |
| Columna completamente nula | `NVARCHAR(MAX)` | `NVARCHAR(500)` |

**Con `nvarchar_max_length`**: todas las columnas string usan ese tamaño fijo sin excepción. No se consulta el schema origen ni se infiere desde el chunk. Recomendado cuando hay inconsistencias entre el schema y los datos reales.

**Sin `nvarchar_max_length`**: el pipeline intenta inferir el largo más preciso posible desde el schema origen o el primer chunk.

### `all_string: true`

Fuerza todas las columnas a `NVARCHAR` independientemente del tipo del origen. Útil cuando querés máxima fidelidad sin riesgo de conversiones.

---

## Filtros de fecha

### Configuración en tables.json

```json
"date_filter": {
  "column": "FECHA_PEDIDO",
  "mode": "today"
}
```

### Modos disponibles

| Mode | Qué filtra |
|---|---|
| `full` | Sin filtro — tabla completa |
| `today` | Solo hoy |
| `yesterday` | Solo ayer |
| `last_n_days` | Últimos N días (`n_days` requerido) |
| `current_month` | Desde el 1ro del mes hasta hoy |
| `last_month` | El mes anterior completo |
| `custom` | Fechas fijas (`date_from`, `date_to`) |

### Formato de la columna de fecha

Muchos sistemas no guardan las fechas con un tipo `DATE` nativo sino como texto. El caso más común es **SAP**, que guarda `BUDAT`, `ERDAT`, `AEDAT`, etc. como `NVARCHAR(8)` con formato `20260810`.

El campo `format` define cómo se renderizan `date_from` y `date_to` antes de ir a la query:

| `format` | Valor generado | Cuándo usarlo |
|---|---|---|
| `date` (default) | `2026-08-10` | Columna `DATE`/`DATETIME` nativa, o texto ISO |
| `YYYYMMDD` | `20260810` | **SAP**: `BUDAT`, `ERDAT`, `AEDAT`, `LAEDA` |
| `DDMMYYYY` | `10082026` | Sistemas legacy con fecha invertida |
| `YYYYMM` | `202608` | Columnas de período mensual |
| `YYYY` | `2026` | Ejercicio fiscal (`GJAHR` en SAP) |

Ejemplo para ACDOCA filtrando por fecha de contabilización:

```json
{
  "source_name": "hana",
  "source_table": "SAPHANADB.ACDOCA",
  "target_table": "SAP_ACDOCA",
  "target_schema": "raw",
  "query": null,
  "date_filter": {
    "column": "BUDAT",
    "mode": "last_n_days",
    "n_days": 7,
    "format": "YYYYMMDD"
  },
  "all_string": true,
  "chunk_size": 5000,
  "tags": ["diario"],
  "enabled": true
}
```

Genera:

```sql
SELECT * FROM SAPHANADB.ACDOCA
WHERE BUDAT >= '20260803' AND BUDAT <= '20260810'
```

**Por qué funciona la comparación como texto:** `YYYYMMDD` y `YYYYMM` son lexicográficamente ordenables — `'20251231' <= '20260101'` es correcto. Por eso SAP eligió ese formato. Con `DDMMYYYY` en cambio la comparación como texto **no** es válida (`'31122025'` sería mayor que `'01012026'`); en ese caso hay que castear en una query manual.

Si no ponés `format`, el comportamiento es el de siempre (`2026-08-10`).

### Tablas sin columna de fecha

No todas las tablas tienen por dónde filtrar. Para esas, `date_filter: null` y se carga completa:

```json
"date_filter": null
```

Como el pipeline siempre hace truncate + insert, una carga completa es consistente igual — el filtro de fecha es una optimización de volumen, no un requisito.

### Query automática vs manual

Con `query: null` el pipeline construye el WHERE automáticamente:
```sql
SELECT * FROM SCHEMA.VENTAS WHERE FECHA_PEDIDO >= '2026-07-24' AND FECHA_PEDIDO <= '2026-07-24'
```

Con query definida, usar `:date_from` y `:date_to` como placeholders:
```json
"query": "SELECT * FROM SCHEMA.VENTAS WHERE FECHA >= :date_from AND FECHA <= :date_to"
```

### Sobreescribir desde CLI

```bash
python run_pipeline.py --source hana --all --date-mode yesterday
python run_pipeline.py --source hana --table VENTAS --date-mode custom --date-from 2026-01-01 --date-to 2026-01-31
python run_pipeline.py --source hana --all --date-mode full   # ignora date_filter del JSON
```

---

## Auditoría

Cada ejecución queda registrada en consola, archivo de log y en `monitor.pipeline_runs`:

| Columna | Descripción |
|---|---|
| `pipeline_name` | `hana_to_raw`, `excel_to_raw` |
| `source_name` | `hana`, `excel` |
| `table_name` | `target_table` ejecutada |
| `started_at` | Timestamp inicio (UTC) |
| `finished_at` | Timestamp fin (UTC) |
| `status` | `success` o `error` |
| `rows_loaded` | Filas insertadas en ese run |
| `error_message` | Mensaje si falló |

La tabla se crea automáticamente en el primer run.

---

## Casos borde y limitaciones conocidas

### Origen sin filas

Si la query o el archivo no devuelve ninguna fila, la tabla destino se **trunca igual** (semántica replace: RAW debe reflejar el origen) y se registra un warning:

```
El origen no devolvio filas para 'raw.MI_TABLA'. La tabla quedo vacia
(semantica replace). Verifica la query o el filtro de fecha.
```

Esto es intencional: si no truncáramos, quedarían los datos del run anterior y la auditoría reportaría éxito con 0 filas — un falso positivo silencioso.

### Columnas duplicadas en la query

Si la query devuelve dos columnas con el mismo nombre, el pipeline **falla con un error claro** en lugar de perder una silenciosamente:

```
La consulta a 'SQL Server origen' devuelve columnas duplicadas: ['id'].
Agrega alias en la query, por ejemplo: SELECT a.id AS a_id, b.id AS b_id
```

### Limitaciones que siguen abiertas

| Limitación | Cuándo aparece | Cómo se manifiesta |
|---|---|---|
| Fila real > 8060 bytes | Tablas muy anchas con datos largos en muchas columnas a la vez | Error de SQL Server al insertar. Solución: reducir columnas en la query |
| Columnas de más de 4000 caracteres | Campos JSON, observaciones largas | Se bindean como `NVARCHAR(MAX)` automáticamente. Sin acción requerida |
| Nombre de columna con `]` | Muy raro | Error de sintaxis SQL. Solución: alias en la query |
| Query pide una columna que no existe | El origen cambió su estructura | `Invalid column name 'X'`. Solución: actualizar la query o usar `query: null` |
| Máximo 1024 columnas por tabla | Tablas SAP extremadamente anchas | Error al crear la tabla. Solución: dividir en dos tablas |
| Excel y CSV se leen completos en memoria | Archivos de cientos de MB | Consumo alto de RAM. `chunk_size` afecta la escritura, no la lectura del archivo |

### Encoding de la salida

El pipeline fuerza **UTF-8** en `stdout` y `stderr`, y todos los mensajes de log son **ASCII puro** (sin emoji ni acentos).

Esto importa cuando un orquestador captura la salida del proceso. En Windows la consola usa `cp1252` por defecto, y la combinación rompía de dos formas:

```
UnicodeEncodeError: 'charmap' codec can't encode character '\u2705'
  -> el orquestador imprime la salida en una consola cp1252 que no soporta emoji

UnicodeDecodeError: 'utf-8' codec can't decode byte 0xe9
  -> el orquestador lee como UTF-8 pero el proceso escribió cp1252
```

Si escribís un wrapper que capture la salida, conviene ser explícito con el encoding:

```python
subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
```

---

## Manejo de errores y reintentos

Si una tabla falla, el pipeline reintenta 3 veces con espera progresiva:

```
Intento 1 → falla → espera 5 segundos
Intento 2 → falla → espera 10 segundos
Intento 3 → falla → error definitivo en auditoría
```

Un error en una tabla no detiene el resto del pipeline.

### Validación del JSON

Al iniciar, el pipeline valida cada tabla antes de ejecutar. Si tiene errores de configuración, la skipea con un warning y continúa con las demás.

---

## Logs

Los logs se escriben en consola y en `logs/pipeline.log` (rotativo diario, 30 días de historial):

```
2026-07-24 10:42:04 | INFO  | Iniciando pipeline 'hana_to_raw' | tablas: ['HANA_MARA']
2026-07-24 10:42:05 | INFO  | Tabla 'raw.HANA_MARA' creada con 45 columnas.
2026-07-24 10:42:05 | INFO  | Tabla 'raw.HANA_MARA' truncada.
2026-07-24 10:42:17 | INFO  | [AUDIT] OK hana | tabla: HANA_MARA | filas: 80000 | duracion: 12.3s
```

---

## Buenas prácticas

- Nunca hardcodear credenciales — usar siempre `.env`
- Usar `enabled: false` para desactivar tablas sin borrar su config
- Para tablas muy anchas (100+ columnas), bajar `chunk_size` a 1000-2000
- Para tablas angostas, subir `chunk_size` a 50000 para mayor velocidad
- Usar tags para agrupar por frecuencia: `diario`, `semanal`, `mensual`
- La capa RAW no debe tener lógica de negocio — los casteos van en Silver/Gold
- Si una tabla ya existe con tipos distintos a los del origen, borrarla y dejar que el pipeline la recree

---

## Oracle

### Requisitos previos

Oracle 12 usa un protocolo de autenticación antiguo incompatible con el modo estándar de `oracledb`. Se requiere **Oracle Instant Client** instalado localmente.

#### Instalación del Oracle Instant Client

1. Descargar el **Basic Package** desde:
   https://www.oracle.com/database/technologies/instant-client/winx64-64-downloads.html
   Archivo: `instantclient-basic-windows.x64-23.26.zip`

2. Descomprimir en una carpeta sin espacios, por ejemplo:
   ```
   C:\oracle\instantclient_23_26
   ```

3. No requiere instalación — solo descomprimir.

### Credenciales en `.env`

```env
ORACLE_HOST=MI_SERVIDOR
ORACLE_PORT=1521
ORACLE_DATABASE=MI_SERVICE_NAME
ORACLE_USERNAME=mi_usuario
ORACLE_PASSWORD=mi_password
ORACLE_CLIENT_DIR=C:\oracle\instantclient_23_26
```

`ORACLE_DATABASE` puede ser un **Service Name** o un **SID** según cómo esté configurado el servidor. Verificarlo en DBeaver en la configuración de la conexión.

### Configuración en tables.json

```json
{
  "source_name": "oracle",
  "source_table": "SCHEMA.MI_TABLA",
  "target_table": "ORA_MI_TABLA",
  "target_schema": "raw",
  "query": null,
  "date_filter": null,
  "all_string": false,
  "chunk_size": 10000,
  "tags": ["diario"],
  "enabled": true
}
```

### Ejecución

```bash
python run_pipeline.py --source oracle --all
python run_pipeline.py --source oracle --table ORA_MI_TABLA
python run_pipeline.py --source oracle --tag diario
python run_pipeline.py --source oracle --all --date-mode yesterday
```

### Placeholders en queries

Oracle usa `:nombre` como placeholder, igual que HANA:

```json
"query": "SELECT * FROM SCHEMA.VENTAS WHERE FECHA >= :date_from AND FECHA <= :date_to"
```

### Nota sobre mayúsculas

Oracle guarda nombres de tablas y columnas en **mayúsculas** por defecto. Si la tabla se creó sin comillas, usar mayúsculas en `source_table`:

```json
"source_table": "MYSCHEMA.VENTAS"
```
