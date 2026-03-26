"""
Pipeline ENEM 2023 — Microdados INEP
=====================================

Objetivo:
    Ler os microdados brutos do ENEM 2023, tratar encoding e colunas
    codificadas, selecionar variáveis relevantes, calcular media geral
    e salvar um arquivo limpo pronto para análise exploratória.

Fonte:
    INEP — Instituto Nacional de Estudos e Pesquisas Educacionais
    https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/enem

Autor: Austin
"""

import awswrangler as wr
import pandas as pd
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

# Caminho do arquivo bruto
CAMINHO_BRUTO = r"C:\Users\austi\OneDrive\Documentos\Data_Estudos\enem-pipeline\dados_enem\DADOS\MICRODADOS_ENEM_2023.csv"
CAMINHO_SAIDA = r"C:\Users\austi\OneDrive\Documentos\Data_Estudos\enem-pipeline\dados_enem\DADOS\enem_2023_tratado.parquet"


# Colunas que vamos usar — ignoramos o resto para economizar memoria
COLUNAS = [
    # Identificacao
    "NU_ANO",
    "NO_MUNICIPIO_PROVA",
    "SG_UF_PROVA",

    # Notas
    "NU_NOTA_CN",       # Ciencias da Natureza
    "NU_NOTA_CH",       # Ciencias Humanas
    "NU_NOTA_LC",       # Linguagens e Codigos
    "NU_NOTA_MT",       # Matematica
    "NU_NOTA_REDACAO",

    # Tipo de escola
    "TP_ESCOLA",        # 1=Nao respondeu 2=Publica 3=Privada

    # Perfil socioeconomico
    "Q006",             # Renda familiar
    "Q001",             # Escolaridade do pai
    "Q002",             # Escolaridade da mae
    "Q025",             # Acesso a internet em casa

    # Situacao do candidato
    "TP_PRESENCA_CN",
    "TP_PRESENCA_CH",
    "TP_PRESENCA_LC",
    "TP_PRESENCA_MT",
]

# Dicionario de renda — Q006
# Fonte: dicionario de variaveis INEP 2023
RENDA = {
    "A": "Nenhuma renda",
    "B": "Ate R$ 1.320",
    "C": "R$ 1.320 a R$ 1.980",
    "D": "R$ 1.980 a R$ 2.640",
    "E": "R$ 2.640 a R$ 3.300",
    "F": "R$ 3.300 a R$ 3.960",
    "G": "R$ 3.960 a R$ 5.280",
    "H": "R$ 5.280 a R$ 6.600",
    "I": "R$ 6.600 a R$ 7.920",
    "J": "R$ 7.920 a R$ 9.240",
    "K": "R$ 9.240 a R$ 10.560",
    "L": "R$ 10.560 a R$ 11.880",
    "M": "R$ 11.880 a R$ 13.200",
    "N": "R$ 13.200 a R$ 15.840",
    "O": "R$ 15.840 a R$ 19.800",
    "P": "Acima de R$ 19.800",
    "Q": "Nao respondeu",
}

# Ordem das faixas para ordenacao em graficos
RENDA_ORDEM = list(RENDA.values())

TIPO_ESCOLA = {
    1: "Nao respondeu",
    2: "Publica",
    3: "Privada",
}

INTERNET = {
    "A": "Sim",
    "B": "Nao",
}


# EXTRACT

def extrair(caminho: str) -> pd.DataFrame:
    log.info("EXTRACT — lendo microdados em chunks...")

    chunks = []
    for chunk in pd.read_csv(
        caminho,
        sep=";",
        encoding="latin-1",
        usecols=COLUNAS,
        low_memory=False,
        chunksize=100000
    ):
        chunks.append(chunk)
        log.info(f"  chunk carregado: {len(chunks) * 100000:,} linhas aprox.")

    df = pd.concat(chunks, ignore_index=True)
    log.info(f"  {len(df):,} candidatos carregados")
    return df



# TRANSFORM 

def transformar(df: pd.DataFrame) -> pd.DataFrame:
    log.info("TRANSFORM — tratando e enriquecendo...")

    # Remove quem faltou em qualquer prova
    # TP_PRESENCA: 0=faltou, 1=presente, 2=eliminado
    presentes = (
        (df["TP_PRESENCA_CN"] == 1) &
        (df["TP_PRESENCA_CH"] == 1) &
        (df["TP_PRESENCA_LC"] == 1) &
        (df["TP_PRESENCA_MT"] == 1)
    )
    df = df[presentes].copy()
    log.info(f"  Candidatos presentes em todas as provas: {len(df):,}")

    # Remove quem nao tem nota em nenhuma das areas
    notas = ["NU_NOTA_CN", "NU_NOTA_CH", "NU_NOTA_LC", "NU_NOTA_MT", "NU_NOTA_REDACAO"]
    df = df.dropna(subset=notas)
    log.info(f"  Candidatos com todas as notas: {len(df):,}")

    # Media geral das 5 areas
    df["MEDIA_GERAL"] = df[notas].mean(axis=1).round(2)

    # Decodifica renda
    df["FAIXA_RENDA"] = df["Q006"].map(RENDA).fillna("Nao respondeu")
    df["FAIXA_RENDA_ORDEM"] = df["FAIXA_RENDA"].map(
        {v: i for i, v in enumerate(RENDA_ORDEM)}
    )

    # Decodifica tipo de escola
    df["TIPO_ESCOLA"] = df["TP_ESCOLA"].map(TIPO_ESCOLA).fillna("Nao respondeu")

    # Decodifica internet
    df["INTERNET"] = df["Q025"].map(INTERNET).fillna("Nao respondeu")

    # Remove colunas que nao precisamos mais
    df = df.drop(columns=["Q006", "Q025", "TP_ESCOLA",
                           "TP_PRESENCA_CN", "TP_PRESENCA_CH",
                           "TP_PRESENCA_LC", "TP_PRESENCA_MT"])

    # Renomeia para facilitar a analise
    df = df.rename(columns={
        "NU_ANO":                 "ano",
        "NO_MUNICIPIO_PROVA":     "municipio",
        "SG_UF_PROVA":            "uf",
        "NU_NOTA_CN":             "nota_ciencias_natureza",
        "NU_NOTA_CH":             "nota_ciencias_humanas",
        "NU_NOTA_LC":             "nota_linguagens",
        "NU_NOTA_MT":             "nota_matematica",
        "NU_NOTA_REDACAO":        "nota_redacao",
        "Q001":                   "escolaridade_pai",
        "Q002":                   "escolaridade_mae",
        "MEDIA_GERAL":            "media_geral",
        "FAIXA_RENDA":            "faixa_renda",
        "FAIXA_RENDA_ORDEM":      "faixa_renda_ordem",
        "TIPO_ESCOLA":            "tipo_escola",
        "INTERNET":               "internet",
    })

    log.info(f"  Colunas no dataset final: {list(df.columns)}")
    return df



# LOAD

def carregar_para_aws(df: pd.DataFrame):
    log.info("Enviando dados para o S3 e atualizando o Athena...")
    
    # Isso aqui faz a mágica: 
    # 1. Salva no S3 como Parquet
    # 2. Cria a tabela no banco de dados do Athena automaticamente
    wr.s3.to_parquet(
        df=df,
        path="s3://NOME-DO-SEU-BUCKET/dados-tratados/", 
        dataset=True,
        database="enem_db",      # Nome do banco no Athena
        table="microdados_2023", # Nome da tabela no Athena
        mode="overwrite",        # Se rodar de novo, ele substitui
        partition_cols=["uf"]    # IMPORTANTE: Particionar por UF economiza $$$ no Athena
    )
    log.info("Carga concluída na AWS!")

if __name__ == "__main__":
    df_bruto = extrair(CAMINHO_BRUTO)
    df_tratado = transformar(df_bruto)
    carregar_para_aws(df_tratado)

# EXECUCAO

def executar_pipeline():
    log.info("=" * 60)
    log.info("PIPELINE ENEM 2023 — MICRODADOS INEP")
    log.info("=" * 60)

    df_bruto = extrair(CAMINHO_BRUTO)
    df_tratado = transformar(df_bruto)
    carregar(df_tratado, CAMINHO_SAIDA)

    log.info("=" * 60)
    log.info("Pipeline concluido.")
    log.info("=" * 60)

    return df_tratado


if __name__ == "__main__":
    executar_pipeline()
