import awswrangler as wr
import pandas as pd
import boto3
import logging

log = logging.getLogger(__name__)

NOME_BUCKET   = "austin-datalake-enem-2023"
CAMINHO_S3    = f"s3://{NOME_BUCKET}/processado/"
NOME_DATABASE = "enem_db"
NOME_TABELA   = "microdados_2023"

boto3_session = boto3.Session(region_name="sa-east-1")


def carregar_aws(df: pd.DataFrame):
    log.info(f"LOAD — Enviando para s3://{NOME_BUCKET}...")

    # Cria o database no Glue se nao existir
    databases = wr.catalog.databases(boto3_session=boto3_session)
    if NOME_DATABASE not in databases["Database"].values:
        wr.catalog.create_database(
            name=NOME_DATABASE,
            boto3_session=boto3_session
        )
        log.info(f"  Database '{NOME_DATABASE}' criado no Glue.")

    wr.s3.to_parquet(
        df=df,
        path=CAMINHO_S3,
        dataset=True,
        database=NOME_DATABASE,
        table=NOME_TABELA,
        mode="overwrite",
        partition_cols=["uf"],
        index=False,
        boto3_session=boto3_session
    )
    log.info("Carga AWS finalizada!")
