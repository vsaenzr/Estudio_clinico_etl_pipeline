# Clinical ETL Pipeline

Pipeline ETL para la recuperación y organización de datos de un estudio clínico.

## Tecnologías utilizadas

* Python
* MySQL
* Pandas
* Power BI

## Arquitectura del proyecto

Extracción de datos desde:

* CSV del sistema administrativo (legacy\_admin\_db.csv)
* JSON exportado desde REDCap

Transformaciones realizadas:

* Estandarización de fechas a formato YYYY-MM-DD
* Conversión de unidades de peso (lbs → kg)
* Anonimización de datos sensibles usando SHA256 + Salt
* Validación de calidad de datos

Carga de datos en base de datos MySQL estructurada.

## Estructura del proyecto

clinical\_etl/

data/

* legacy\_admin\_db.csv
* redcap\_export\_raw.json

etl/

* etl\_clinico.py

sql/

* crear\_tablas.sql

requirements.txt

## Reglas de gobernanza implementadas

1. Normalización de fechas de ingreso
2. Conversión de unidades de peso según sede
3. Anonimización de nombres y documentos
4. Registro de errores ETL

## Dashboard

El dashboard muestra:

* Peso promedio por sede y evento clínico
* Alertas de calidad de datos cuando un seguimiento ocurre antes del evento basal

## Valery Saenz Robayo

Prueba de Conocimientos 

