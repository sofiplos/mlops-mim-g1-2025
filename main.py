import psycopg2
from fastapi import FastAPI, HTTPException
from datetime import date, timedelta
from typing import List, Dict, Any
import os
# ======================================================
# CONFIGURACIÓN DB (Carga desde Variables de Entorno de App Runner)
# ======================================================

# Usamos os.environ.get() para cargar los valores.
# El segundo argumento es un valor por defecto, útil para pruebas locales,
# pero App Runner garantiza que las variables estén definidas.
DB_HOST = os.environ.get("DB_HOST")
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_PORT = int(os.environ.get("DB_PORT", 5432))

# Verificación crítica: Aseguramos que las credenciales vitales se cargaron
if not all([DB_HOST, DB_NAME, DB_USER, DB_PASSWORD]):
    raise ValueError("Faltan variables de entorno esenciales para la conexión a la base de datos.")


# ======================================================
# MAPEO DE MODELOS (FIX FINAL: Garantiza el casing correcto para la DB)
# ======================================================
MODEL_MAPPING = {
    "topctr": "TopCTR",
    "topproduct": "TopProduct",
}

# ======================================================
# FASTAPI INSTANCE
# ======================================================

app = FastAPI(
    title="AdTech Recommendations API",
    description="API para servir recomendaciones generadas por el pipeline de Airflow",
    version="1.0"
)

# ----------------------------------------------------------------------
# Funciones auxiliares de conexión y manejo de errores
# ----------------------------------------------------------------------

def get_db_connection():
    """Establece la conexión a la base de datos RDS.
    Lanza HTTPException si la conexión falla."""
    try:
        # Los valores se usan directamente desde las variables globales cargadas por os.environ
        return psycopg2.connect(
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            connect_timeout=15
        )
    except psycopg2.OperationalError as e:
        # 503 Service Unavailable: Error al intentar conectar con el servicio (DB)
        print(f"Error al conectar con la base de datos: {e}")
        # Se verifica si la variable DB_HOST está definida para un mensaje de error más claro
        db_host_display = DB_HOST if DB_HOST else "No definido"
        raise HTTPException(
            status_code=503,
            detail=f"Error de conexión con la base de datos en {db_host_display}. Verifique Security Groups."
        )


# ----------------------------------------------------------------------
# 1) /recommendations/<ADV>/<Modelo> Endpoint
# ----------------------------------------------------------------------

@app.get("/recommendations/{adv_id}/{model_name}")
def get_recommendations(adv_id: str, model_name: str):
    # Normalizar el nombre del modelo y validar
    model_name_norm = model_name.lower()
    # Validar si el modelo normalizado existe en nuestro mapeo
    if model_name_norm not in MODEL_MAPPING:
        raise HTTPException(
            status_code=400,
            detail="Modelo inválido. Use TopCTR o TopProduct."
        )

    # OBTENER EL NOMBRE EXACTO DE LA DB (TopCTR o TopProduct)
    model_name_db = MODEL_MAPPING[model_name_norm]

    conn = None
    try:
        conn = get_db_connection() # Puede lanzar HTTPException(503)
        cur = conn.cursor()

        # Se asume que la EC2 tiene la fecha correcta o que los datos están cargados
        today_date = date.today().strftime("%Y-%m-%d")

        query = """
        SELECT
            product_id, metric
        FROM
            recommendations
        WHERE
            advertiser_id = %s AND model = %s AND date = %s
        ORDER BY
            metric DESC
        LIMIT 20;
        """

        # Ejecución segura de la consulta:
        cur.execute(query, (adv_id, model_name_db, today_date))
        results = cur.fetchall()
        cur.close()

        if not results:
            # 404 Not Found: Si la consulta no devuelve resultados
            raise HTTPException(
                status_code=404,
                detail=f"No hay recomendaciones de {model_name_db} para el ADV {adv_id} hoy ({today_date})."
            )

        recommendations_list = [
            {"product_id": row[0], "metric": float(row[1])} for row in results
        ]

        return {
            "advertiser_id": adv_id,
            "model_name": model_name_db,
            "date": today_date,
            "recommendations": recommendations_list
        }

    except psycopg2.Error as e:
        # 500 Internal Server Error: Error durante la ejecución de la consulta
        print(f"Error de base de datos durante la consulta: {e}")
        raise HTTPException(status_code=500, detail="Error interno al consultar las recomendaciones.")
    finally:
        if conn:
            conn.close()


# ----------------------------------------------------------------------
# 2) /history/<ADV>/ Endpoint
# ----------------------------------------------------------------------

@app.get("/history/{adv_id}")
def get_history(adv_id: str):
    conn = None
    try:
        # Calcular el rango de fechas
        today = date.today().strftime("%Y-%m-%d")
        seven_days_ago = (date.today() - timedelta(days=7)).strftime("%Y-%m-%d")

        conn = get_db_connection() # Puede lanzar HTTPException(503)
        cur = conn.cursor()

        # Consulta SQL para el historial de los últimos 7 días
        query = """
        SELECT
            date, model, product_id, metric
        FROM
            recommendations
        WHERE
            advertiser_id = %s AND date >= %s
        ORDER BY
            date DESC, model, metric DESC;
        """

        cur.execute(query, (adv_id, seven_days_ago))
        results = cur.fetchall()
        cur.close()

        if not results:
            # 404 Not Found
            raise HTTPException(
                status_code=404,
                detail=f"No se encontró historial para el ADV {adv_id} en los últimos 7 días."
            )

        # Procesamiento y Agrupación por fecha
        history_grouped: Dict[str, List[Dict[str, Any]]] = {}
        for row in results:
            date_str = row[0]
            model = row[1]
            product_id = row[2]
            metric_value = row[3]

            if date_str not in history_grouped:
                history_grouped[date_str] = []

            history_grouped[date_str].append({
                "model": model,
                "product_id": product_id,
                "metric_value": float(metric_value)
            })

        return {
            "advertiser_id": adv_id,
            "period_from": seven_days_ago,
            "period_to": today,
            "history_by_date": history_grouped
        }

    except psycopg2.Error as e:
        print(f"Error de base de datos en /history: {e}")
        raise HTTPException(status_code=500, detail="Error interno al recuperar el historial.")
    finally:
        if conn:
            conn.close()


# ----------------------------------------------------------------------
# 3) /stats/ Endpoint
# ----------------------------------------------------------------------

@app.get("/stats")
def get_stats():
    conn = None
    today_date = date.today().strftime("%Y-%m-%d")
    stats_data: Dict[str, Any] = {}

    try:
        conn = get_db_connection() # Puede lanzar HTTPException(503)
        cur = conn.cursor()

        # 1. Cantidad de advertisers activos hoy
        cur.execute("""
        SELECT COUNT(DISTINCT advertiser_id)
        FROM recommendations
        WHERE date = %s;
        """, (today_date,))
        stats_data["num_advertisers_today"] = cur.fetchone()[0]

        # 2. Cantidad de productos únicos recomendados hoy
        cur.execute("""
        SELECT COUNT(DISTINCT product_id)
        FROM recommendations
        WHERE date = %s;
        """, (today_date,))
        stats_data["num_products_unique_today"] = cur.fetchone()[0]

        # 3. Coincidencia entre modelos para los diferentes advs (Métrica del ejemplo)
        # Cuenta cuántos productos aparecen en el TopCTR y TopProduct para el mismo anunciante y día.
        cur.execute("""
            SELECT r1.advertiser_id,
                    COUNT(r1.product_id)
            FROM (
                SELECT advertiser_id, product_id, date FROM recommendations WHERE model = 'TopProduct' AND date = %s
            ) r1
            INNER JOIN (
                SELECT advertiser_id, product_id, date FROM recommendations WHERE model = 'TopCTR' AND date = %s
            ) r2
            ON r1.advertiser_id = r2.advertiser_id AND r1.product_id = r2.product_id
            GROUP BY r1.advertiser_id;
        """, (today_date, today_date))
        coincidencias = cur.fetchall()

        stats_data["coincidencias_modelo_por_adv_hoy"] = [
            {"advertiser_id": adv, "productos_en_comun": c}
            for adv, c in coincidencias
        ]

        cur.close()

        return stats_data

    except psycopg2.Error as e:
        print(f"Error de base de datos en /stats: {e}")
        raise HTTPException(status_code=500, detail="Error interno al calcular las estadísticas.")
    finally:
        if conn:
            conn.close()