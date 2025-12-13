from datetime import datetime, timedelta
import pandas as pd
import boto3
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

BUCKET = "grupo-1-2025"
AWS_REGION = "us-east-2"
origen_folder = "Bronze"
destino_folder = "Silver"
folder_Filtered = "Filtered"
folder_CTR = "Top_CTR"
folder_TopProducts = "Top_Products"


s3 = boto3.client("s3", region_name=AWS_REGION)

# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

def read_csv_from_s3(path):
    obj = s3.get_object(Bucket=BUCKET, Key=path)
    return pd.read_csv(obj["Body"])

def write_df_to_s3(df, path):
    s3.put_object(
        Bucket=BUCKET,
        Key=path,
        Body=df.to_csv(index=False).encode("utf-8")
    )

# ----------------------------------------------------------------------
# 1) FiltrarDatos
# ----------------------------------------------------------------------

def filtrar_datos(**kwargs):
    today = kwargs["ds"]

    df_products = read_csv_from_s3(f"{origen_folder}/product_views")
    df_ads = read_csv_from_s3(f"{origen_folder}/ads_views")
    df_active = read_csv_from_s3(f"{origen_folder}/advertiser_ids")

    active_list = set(df_active["advertiser_id"])

    df_products_f = df_products[df_products["advertiser_id"].isin(active_list)]
    df_ads_f = df_ads[df_ads["advertiser_id"].isin(active_list)]

    write_df_to_s3(df_products_f, f"{destino_folder}/{folder_Filtered}/{today}/products.csv")
    write_df_to_s3(df_ads_f, f"{destino_folder}/{folder_Filtered}/{today}/ads.csv")

# ----------------------------------------------------------------------
# 2) TopCTR
# ----------------------------------------------------------------------

def compute_topctr(**kwargs):
    today = kwargs["ds"]

    df = read_csv_from_s3(f"{destino_folder}/{folder_Filtered}/{today}/ads.csv")

    df_ctr = (
        df.groupby(["advertiser_id", "product_id", "type"])
          .size()
          .unstack(fill_value=0)
          .reset_index()
    )

    df_ctr["ctr"] = df_ctr["click"] / df_ctr["impression"].replace(0, 1)

    df_top = (
        df_ctr.sort_values(["advertiser_id", "ctr"], ascending=[True, False])
              .groupby("advertiser_id")
              .head(20)
              .reset_index(drop=True)
    )

    write_df_to_s3(df_top, f"{destino_folder}/{folder_CTR}/{today}/topctr.csv")

# ----------------------------------------------------------------------
# 3) TopProduct
# ----------------------------------------------------------------------

def compute_topproduct(**kwargs):
    today = kwargs["ds"]

    df = read_csv_from_s3(f"{destino_folder}/{folder_Filtered}/{today}/products.csv")

    df_count = (
        df.groupby(["advertiser_id", "product_id"])
          .size()
          .reset_index(name="views")
    )

    df_top = (
        df_count
        .sort_values(["advertiser_id", "views"], ascending=[True, False])
        .groupby("advertiser_id")
        .head(20)
    )
    write_df_to_s3(df_top, f"{destino_folder}/{folder_TopProducts}/{today}/topproduct.csv")


# ----------------------------------------------------------------------
# 4) DBWriting
# ----------------------------------------------------------------------

def write_to_db(**kwargs):
    today = kwargs["ds"]

    # Leer outputs del día
    df_tp = read_csv_from_s3(f"{destino_folder}/{folder_TopProducts}/{today}/topproduct.csv")
    df_ctr = read_csv_from_s3(f"{destino_folder}/{folder_CTR}/{today}/topctr.csv")

    conn = psycopg2.connect(
        database="postgres",
        user="postgres",
        password="tomibelucapos123!",
        host="grupo-1-2025-rds.cpomi0gaon83.us-east-2.rds.amazonaws.com",
        port=5432
    )
    cur = conn.cursor()

    TABLA = "recommendations"

    try:
        # -----------------------------------------------------------
        # 1) Validar si la tabla existe
        # -----------------------------------------------------------
        print(f"Verificando existencia de tabla '{TABLA}'...")

        cur.execute("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = %s
            );
        """, (TABLA,))
        exists = cur.fetchone()[0]

        if not exists:
            print(f"La tabla '{TABLA}' no existe. Creándola...")

            cur.execute("""
                CREATE TABLE IF NOT EXISTS recommendations (
                    date VARCHAR(20),
                    advertiser_id VARCHAR(100),
                    model VARCHAR(50),
                    product_id VARCHAR(100),
                    metric FLOAT
                );
            """)
            conn.commit()
            print(f"Tabla '{TABLA}' creada correctamente.")
        else:
            print(f"La tabla '{TABLA}' ya existe.")

        # -----------------------------------------------------------
        # 2) Verificar si hay datos hoy para esta tabla
        # -----------------------------------------------------------
        cur.execute(f"SELECT COUNT(*) FROM {TABLA} WHERE date = %s;", (today,))
        count_today = cur.fetchone()[0]

        print(f"Registros existentes para hoy ({today}) en '{TABLA}': {count_today}")

        # -----------------------------------------------------------
        # 3) Borrar solo registros hoy
        # -----------------------------------------------------------
        if count_today > 0:
            print(f"Eliminando registros existentes del día {today} en '{TABLA}'...")
            cur.execute(f"DELETE FROM {TABLA} WHERE date = %s;", (today,))
            print(f"Registros eliminados: {cur.rowcount}")
        else:
            print(f"No hay registros previos para hoy en '{TABLA}'. No se elimina nada.")

        # -----------------------------------------------------------
        # 4) Insertar nuevos datos TopProduct
        # -----------------------------------------------------------
        for _, row in df_tp.iterrows():
            cur.execute(
                f"INSERT INTO {TABLA} VALUES (%s, %s, %s, %s, %s)",
                (today, row["advertiser_id"], "TopProduct",
                 row["product_id"], row["views"])
            )
        print(f"Insertados {len(df_tp)} registros de TopProduct.")

        # -----------------------------------------------------------
        # 5) Insertar nuevos datos TopCTR
        # -----------------------------------------------------------
        for _, row in df_ctr.iterrows():
            cur.execute(
                f"INSERT INTO {TABLA} VALUES (%s, %s, %s, %s, %s)",
                (today, row["advertiser_id"], "TopCTR",
                 row["product_id"], row["ctr"])
            )
        print(f"Insertados {len(df_ctr)} registros de TopCTR.")

        # -----------------------------------------------------------
        # 6) Commit final
        # -----------------------------------------------------------
        conn.commit()

    except Exception as e:
        conn.rollback()
        print("Error durante write_to_db:", e)
        raise

    finally:
        cur.close()
        conn.close()


# ----------------------------------------------------------------------
# DAG
# ----------------------------------------------------------------------

default_args = {
    "start_date": datetime(2025,11,30),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": True
}

with DAG(
    dag_id="pipeline_recommendations",
    default_args=default_args,
    schedule='0 0 * * *',
) as dag:

    t1 = PythonOperator(
        task_id="filtrar_datos",
        python_callable=filtrar_datos,
    )

    t2 = PythonOperator(
        task_id="top_product",
        python_callable=compute_topproduct,
    )

    t3 = PythonOperator(
        task_id="top_ctr",
        python_callable=compute_topctr,
    )

    t4 = PythonOperator(
        task_id="write_to_db",
        python_callable=write_to_db,
    )

    # Definir dependencias
    t1 >> [t2, t3] >> t4