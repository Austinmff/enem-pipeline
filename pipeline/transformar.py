import pandas as pd
import logging

log = logging.getLogger(__name__)

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


def transformar(df: pd.DataFrame) -> pd.DataFrame:
    log.info("TRANSFORM — tratando e enriquecendo...")

    presentes = (
        (df["TP_PRESENCA_CN"] == 1) &
        (df["TP_PRESENCA_CH"] == 1) &
        (df["TP_PRESENCA_LC"] == 1) &
        (df["TP_PRESENCA_MT"] == 1)
    )
    df = df[presentes].copy()
    log.info(f"  Candidatos presentes em todas as provas: {len(df):,}")

    notas = ["NU_NOTA_CN", "NU_NOTA_CH", "NU_NOTA_LC", "NU_NOTA_MT", "NU_NOTA_REDACAO"]
    df = df.dropna(subset=notas)
    log.info(f"  Candidatos com todas as notas: {len(df):,}")

    df["MEDIA_GERAL"] = df[notas].mean(axis=1).round(2)
    df["FAIXA_RENDA"] = df["Q006"].map(RENDA).fillna("Nao respondeu")
    df["FAIXA_RENDA_ORDEM"] = df["FAIXA_RENDA"].map(
        {v: i for i, v in enumerate(RENDA_ORDEM)}
    )
    df["TIPO_ESCOLA"] = df["TP_ESCOLA"].map(TIPO_ESCOLA).fillna("Nao respondeu")
    df["INTERNET"] = df["Q025"].map(INTERNET).fillna("Nao respondeu")

    df = df.drop(columns=["Q006", "Q025", "TP_ESCOLA",
                           "TP_PRESENCA_CN", "TP_PRESENCA_CH",
                           "TP_PRESENCA_LC", "TP_PRESENCA_MT"])

    df = df.rename(columns={
        "NU_ANO":             "ano",
        "NO_MUNICIPIO_PROVA": "municipio",
        "SG_UF_PROVA":        "uf",
        "NU_NOTA_CN":         "nota_ciencias_natureza",
        "NU_NOTA_CH":         "nota_ciencias_humanas",
        "NU_NOTA_LC":         "nota_linguagens",
        "NU_NOTA_MT":         "nota_matematica",
        "NU_NOTA_REDACAO":    "nota_redacao",
        "Q001":               "escolaridade_pai",
        "Q002":               "escolaridade_mae",
        "MEDIA_GERAL":        "media_geral",
        "FAIXA_RENDA":        "faixa_renda",
        "FAIXA_RENDA_ORDEM":  "faixa_renda_ordem",
        "TIPO_ESCOLA":        "tipo_escola",
        "INTERNET":           "internet",
    })

    log.info(f"  Colunas no dataset final: {list(df.columns)}")
    return df
