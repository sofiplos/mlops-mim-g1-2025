# mlops-mim-g1-2025
Trabajo práctico final de MLOps (MiM 2025, Grupo 1)
TP Final MLOps – MiM 2025
========================

Este repositorio contiene el código del trabajo práctico final de la materia
Machine Learning Operations (MLOps) del Master in Management + Analytics (MiM).

El proyecto implementa un sistema de recomendación de productos para la
industria AdTech, simulando el rol de una Media Agency que decide qué productos
mostrar en espacios publicitarios digitales.

La solución contempla tanto la generación offline de recomendaciones mediante
un pipeline de datos, como el serving online de dichas recomendaciones a través
de una API REST.

Arquitectura general
--------------------
- Pipeline de datos orquestado con Apache Airflow (EC2).
- Datos almacenados en Amazon S3 (capas Bronze y Silver).
- Recomendaciones persistidas en PostgreSQL (AWS RDS).
- API desarrollada con FastAPI y dockerizada.
- Despliegue de la API mediante Amazon ECR + App Runner.

Pipeline de datos
-----------------
El pipeline de Airflow se ejecuta diariamente y realiza:
- Lectura de logs desde S3.
- Filtrado de advertisers activos.
- Cálculo de recomendaciones TopProduct y TopCTR.
- Escritura de los resultados en la base de datos PostgreSQL.

API
---
La API expone endpoints para:
- Obtener recomendaciones del día por advertiser y modelo.
- Consultar el histórico de recomendaciones.
- Obtener estadísticas generales del sistema.

Las credenciales se gestionan mediante variables de entorno.

Estructura del repositorio
--------------------------
App/
- main.py                    -> API FastAPI
- nano README.md
- requirements.txt
- Dockerfile

DAG/
- nano README.md- pipeline_recommendations.py -> DAG de Airflow
- pipeline_recommendations_security.py -> DAG de Airflow con mejoras de seguridad

Ejemplos S3/
- Bronze/
- Silver/

.gitignore
README.md

Tecnologías utilizadas
----------------------
Python, Apache Airflow, FastAPI, Docker, PostgreSQL y AWS.
