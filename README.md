# Pivoteo de Tabla Moodle Response

Script de Python para procesar y pivotar respuestas de cuestionarios de Moodle desde AWS Athena.

## Descripción

Este proyecto consulta datos de cuestionarios de Moodle almacenados en AWS Athena, procesa las respuestas y genera tablas pivotadas en formato Excel. Está diseñado específicamente para trabajar con encuestas de satisfacción y cuestionarios educativos.

## Características

- Consulta datos desde AWS Athena
- Procesamiento y normalización de respuestas de cuestionarios
- Generación de tablas pivotadas por estudiante y pregunta
- Exportación a formato Excel
- Limpieza automática de recursos temporales en S3

## Requisitos

- Python 3.8 o superior
- Cuenta de AWS con acceso a Athena y S3
- Credenciales de AWS configuradas

## Instalación

1. Clonar el repositorio:
```bash
git clone <url-del-repositorio>
cd Pivoteo-de-la tabla-moodle-response
```

2. Crear un entorno virtual (opcional pero recomendado):
```bash
python -m venv venv
source venv/bin/activate  # En Windows: venv\Scripts\activate
```

3. Instalar dependencias:
```bash
pip install -r requirements.txt
```

## Configuración

### AWS Credentials

Asegúrate de tener configuradas tus credenciales de AWS. Puedes hacerlo de varias formas:

1. Usando AWS CLI:
```bash
aws configure
```

2. Variables de entorno:
```bash
export AWS_ACCESS_KEY_ID=tu_access_key
export AWS_SECRET_ACCESS_KEY=tu_secret_key
export AWS_DEFAULT_REGION=us-east-1
```

3. Archivo de credenciales (`~/.aws/credentials`)

## Uso

### Notebook Jupyter

Abre y ejecuta el notebook principal:
```bash
jupyter notebook script_pivot.ipynb
```

### Parámetros de configuración

En el notebook, modifica las siguientes variables según tu proyecto:

```python
project_id = 82  # ID del proyecto
tag_test = 'cuestionario de satisfacción modular'  # Tag del test a procesar
* Puedes modificar el where de la query si algo extra se debe filtrar
```

## Estructura del Proyecto

```
.
├── athena_utils.py          # Utilidades para consultas a Athena
├── script_pivot.ipynb       # Notebook principal de procesamiento
├── NocoDB/                  # Datos y scripts relacionados con NocoDB
├── requirements.txt         # Dependencias del proyecto
└── README.md               # Este archivo
```

## Funcionalidades Principales

### athena_utils.py

- `run_athena_query()`: Ejecuta consultas en Athena y retorna DataFrames
- `export_dataframe_to_s3_json()`: Exporta DataFrames a S3 en formato JSON
- `create_athena_table()`: Crea tablas externas en Athena

### script_pivot.ipynb

1. Consulta datos de cuestionarios desde Athena
2. Normaliza y limpia respuestas
3. Pivotea datos para generar una fila por estudiante
4. Exporta resultados a Excel

## Salida

El script genera archivos Excel con el formato:
```
{tag_test}_{project_id}.xlsx
```

Cada archivo contiene:
- Una fila por estudiante
- Columnas con información del estudiante (nombre, institución, grado)
- Columnas con las respuestas a cada pregunta

## Notas

- Los archivos de datos (.xlsx, .csv) están excluidos del repositorio mediante .gitignore
- Los recursos temporales en S3 se eliminan automáticamente después de cada consulta
- Las consultas están optimizadas para usar formato Parquet con compresión SNAPPY

## Contribuir

Si deseas contribuir a este proyecto, por favor:
1. Haz un fork del repositorio
2. Crea una rama para tu feature
3. Realiza tus cambios
4. Envía un pull request

## Licencia

Este proyecto es de uso interno de Crack The Code.
