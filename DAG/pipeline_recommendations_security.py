from datetime import datetime, timedelta
import pandas as pd
import boto3

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ----------------------------------------------------------------------
# Configuración
# ----------------------------------------------------------------------

BUCKET = "grupo-1-2025"
AWS_REGION = "us-east-2"

origen_folder = "Bronze"
destino_folder = "Silver"
folder_Filtered = "Filtered"
folder_CTR = "Top_CTR"
folder_TopProducts = "Top_Products"

POSTGRES_CONN_ID = "postgres_recommendations"

s3 = boto3.client("s3", region_name=AWS_REGION)

# ----------------------------------------------------------------------
# Helpers S3 (boto3)
# ----------------------------------------------------------------------

def read_csv_from_s3(key):
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return pd.read_csv(obj["Body"])

def write_df_to_s3(df, key):
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=df.to_csv(index=False).encode("utf-8")
    )

# ----------------------------------------------------------------------
# 1) FiltrarDatos
# ----------------------------------------------------------------------

def filtrar_datos(**kwargs):
    today = kwargs["ds"]

    df_products = read_csv_from_s3(f"{origen_folder}/product_views.csv")
    df_ads = read_csv_from_s3(f"{origen_folder}/ads_views.csv")
    df_active = read_csv_from_s3(f"{origen_folder}/advertiser_ids.csv")

    active_list = set(df_active["advertiser_id"])

    df_products_f = df_products[df_products["advertiser_id"].isin(active_list)]
    df_ads_f = df_ads[df_ads["advertiser_id"].isin(active_list)]

    write_df_to_s3(
        df_products_f,
        f"{destino_folder}/{folder_Filtered}/{today}/products.csv"
    )
    write_df_to_s3(
        df_ads_f,
        f"{destino_folder}/{folder_Filtered}/{today}/ads.csv"
    )

# ----------------------------------------------------------------------
# 2) TopCTR
# ----------------------------------------------------------------------

def compute_topctr(**kwargs):
    today = kwargs["ds"]

    df = read_csv_from_s3(
        f"{destino_folder}/{folder_Filtered}/{today}/ads.csv"
    )

    df_ctr = (
        df.groupby(["advertiser_id", "product_id", "type"])
          .size()
          .unstack(fill_value=0)
          .reset_index()
    )

    df_ctr["ctr"] = df_ctr["click"] / df_ctr["impression"]
    df_ctr["ctr"] = df_ctr["ctr"].fillna(0)

    df_top = (
        df_ctr
        .sort_values(["advertiser_id", "ctr"], ascending=[True, False])
        .groupby("advertiser_id")
        .head(20)
        .reset_index(drop=True)
    )

    write_df_to_s3(
        df_top,
        f"{destino_folder}/{folder_CTR}/{today}/topctr.csv"
    )

# ----------------------------------------------------------------------
# 3) TopProduct
# ----------------------------------------------------------------------

def compute_topproduct(**kwargs):
    today = kwargs["ds"]

    df = read_csv_from_s3(
        f"{destino_folder}/{folder_Filtered}/{today}/products.csv"
    )

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

    write_df_to_s3(
        df_top,
        f"{destino_folder}/{folder_TopProducts}/{today}/topproduct.csv"
    )

# ----------------------------------------------------------------------
# 4) DBWriting (PostgresHook)
# ----------------------------------------------------------------------

def write_to_db(**kwargs):
    today = kwargs["ds"]

    df_tp = read_csv_from_s3(
        f"{destino_folder}/{folder_TopProducts}/{today}/topproduct.csv"
    )
    df_ctr = read_csv_from_s3(
        f"{destino_folder}/{folder_CTR}/{today}/topctr.csv"
    )

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    TABLA = "recommendations"

    try:
        # 1) Verificar existencia de tabla
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
            cur.execute("""
                CREATE TABLE recommendations (
                    date VARCHAR(20),
                    advertiser_id VARCHAR(100),
                    model VARCHAR(50),
                    product_id VARCHAR(100),
                    metric FLOAT
                );
            """)
            conn.commit()

        # 2) Borrar registros del día si existen
        cur.execute(
            f"SELECT COUNT(*) FROM {TABLA} WHERE date = %s;",
            (today,)
        )
        count_today = cur.fetchone()[0]

        if count_today > 0:
            cur.execute(
                f"DELETE FROM {TABLA} WHERE date = %s;",
                (today,)
            )

        # 3) Insert TopProduct
        for _, row in df_tp.iterrows():
            cur.execute(
                f"INSERT INTO {TABLA} VALUES (%s, %s, %s, %s, %s)",
                (
                    today,
                    row["advertiser_id"],
                    "TopProduct",
                    row["product_id"],
                    row["views"]
                )
            )

        # 4) Insert TopCTR
        for _, row in df_ctr.iterrows():
            cur.execute(
                f"INSERT INTO {TABLA} VALUES (%s, %s, %s, %s, %s)",
                (
                    today,
                    row["advertiser_id"],
                    "TopCTR",
                    row["product_id"],
                    row["ctr"]
                )
            )

        conn.commit()

    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

# ----------------------------------------------------------------------
# DAG
# ----------------------------------------------------------------------

default_args = {
    "start_date": datetime(2025, 11, 20),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="pipeline_recommendations",
    default_args=default_args,
    schedule_interval="0 0 * * *",
    catchup=True,
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

    t1 >> [t2, t3] >> t4