import pendulum 
from pathlib import Path
import pandas as pd 
from airflow.operators.python import PythonOperator
from airflow import DAG
from src.bronze.extract import extract, load_bronze_datalake
from src.bronze.extract import upload_s3 as upload_s3_bronze
from src.silver.transform import transform, load_silver_datalake
from src.silver.transform import upload_s3 as upload_s3_silver
from src.gold.build_metrics import build_metrics, load_gold_datalake
from src.gold.build_metrics import upload_s3 as upload_s3_gold
from src.gold.load import load as load_dw

BRONZE_PATH = "/opt/airflow/data_lake/bronze/analise_dificuldade_programacao_bruto.parquet"
SILVER_PATH = "/opt/airflow/data_lake/silver/analise_dificuldade_programacao_tratado.parquet"
GOLD_PATH = "/opt/airflow/data_lake/gold"

def task_extract_load_bronze():
    df = extract()
    load_bronze_datalake(df)

def task_upload_s3_bronze():
    upload_s3_bronze(BRONZE_PATH, "bronze/analise_dificuldade_programacao_bruto.parquet")

def task_transform_load_silver():
    df = pd.read_parquet(BRONZE_PATH)
    df_silver = transform(df)
    load_silver_datalake(df_silver)

def task_upload_s3_silver():
    upload_s3_silver(SILVER_PATH, "silver/analise_dificuldade_programacao_tratado.parquet")

def task_build_metrics_load_gold():
    df = pd.read_parquet(SILVER_PATH)
    tabelas = build_metrics(df)
    load_gold_datalake(tabelas)

def task_upload_s3_gold():
    for nome in ["dim_usuario", "dim_tempo", "dim_tags", "dim_perguntas", "fato_perguntas", "bridge_tags"]:
        path = f"{GOLD_PATH}/{nome}.parquet"
        upload_s3_gold(path, f"gold/{nome}.parquet")

def task_load_dw():
    df_usuarios = pd.read_parquet(f"{GOLD_PATH}/dim_usuario.parquet")
    df_tempo = pd.read_parquet(f"{GOLD_PATH}/dim_tempo.parquet")
    df_tags = pd.read_parquet(f"{GOLD_PATH}/dim_tags.parquet")
    df_perguntas = pd.read_parquet(f"{GOLD_PATH}/dim_perguntas.parquet")
    fato_perguntas = pd.read_parquet(f"{GOLD_PATH}/fato_perguntas.parquet")
    df_bridge = pd.read_parquet(f"{GOLD_PATH}/bridge_tags.parquet")
    
    load_dw(
        df_usuarios, 
        df_tempo, 
        df_tags, 
        df_perguntas, 
        fato_perguntas, 
        df_bridge
    )

with DAG(
    dag_id="Pipeline_StackOverFlow",
    description="Perguntas mais frequentes no StackOverFlow sobre Python",
    start_date=pendulum.datetime(2026, 2, 4, tz="America/Sao_Paulo"),
    schedule=None,
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=task_extract_load_bronze
    )

    upload_s3_bronze_task = PythonOperator(
        task_id="upload_s3_bronze",
        python_callable=task_upload_s3_bronze
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=task_transform_load_silver
    )

    upload_s3_silver_task = PythonOperator(
        task_id="upload_s3_silver",
        python_callable=task_upload_s3_silver
    )

    build_metrics_task = PythonOperator(
        task_id="build_metrics",
        python_callable=task_build_metrics_load_gold
    )

    upload_s3_gold_task = PythonOperator(
        task_id="upload_s3_gold",
        python_callable=task_upload_s3_gold
    )

    load_dw_task = PythonOperator(
        task_id="load_dw",
        python_callable=task_load_dw
    )

    extract_task >> upload_s3_bronze_task

    upload_s3_bronze_task >> transform_task >> upload_s3_silver_task

    upload_s3_silver_task >> build_metrics_task >> upload_s3_gold_task

    upload_s3_gold_task >> load_dw_task