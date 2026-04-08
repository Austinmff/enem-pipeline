import pandas as pd
import logging

log = logging.getLogger(__name__)

COLUNAS = [
    "NU_ANO",
    "NO_MUNICIPIO_PROVA",
    "SG_UF_PROVA",
    "NU_NOTA_CN",
    "NU_NOTA_CH",
    "NU_NOTA_LC",
    "NU_NOTA_MT",
    "NU_NOTA_REDACAO",
    "TP_ESCOLA",
    "Q006",
    "Q001",
    "Q002",
    "Q025",
    "TP_PRESENCA_CN",
    "TP_PRESENCA_CH",
    "TP_PRESENCA_LC",
    "TP_PRESENCA_MT",
]


def extrair(caminho: str) -> pd.DataFrame:
    log.info("EXTRACT — lendo microdados em chunks...")
    log.info("  O arquivo tem ~4 milhoes de linhas — pode demorar alguns minutos.")

    chunks = []
    for i, chunk in enumerate(pd.read_csv(
        caminho,
        sep=";",
        encoding="latin-1",
        usecols=COLUNAS,
        low_memory=False,
        chunksize=100000
    ), start=1):
        chunks.append(chunk)
        log.info(f"  chunk {i} carregado ({i * 100000:,} linhas aprox.)")

    df = pd.concat(chunks, ignore_index=True)
    log.info(f"  Total: {len(df):,} candidatos carregados")
    return df
