import streamlit as st
import awswrangler as wr
import boto3
import pandas as pd
import plotly.express as px

st.set_page_config(
    page_title="ENEM 2023 — Desigualdade Educacional",
    layout="wide"
)

# Sessao AWS
boto3_session = boto3.Session(region_name="sa-east-1")

BUCKET   = "austin-datalake-enem-2023"
DATABASE = "enem_db"
TABELA   = "microdados_2023"
PATH_S3  = f"s3://{BUCKET}/processado/"


# Carregamento de dados 

@st.cache_data
def carregar_ufs():
    df_temp = wr.s3.read_parquet(
        path=PATH_S3,
        dataset=True,
        columns=["uf"],
        boto3_session=boto3_session
    )
    return sorted(df_temp["uf"].unique())


@st.cache_data
def carregar_dados(uf: str) -> pd.DataFrame:
    return wr.s3.read_parquet(
        path=PATH_S3,
        dataset=True,
        partition_filter=lambda x: x["uf"] == uf,
        boto3_session=boto3_session
    )


@st.cache_data
def carregar_nacional() -> pd.DataFrame:
    return wr.s3.read_parquet(
        path=PATH_S3,
        dataset=True,
        columns=["uf", "media_geral", "tipo_escola", "faixa_renda", "faixa_renda_ordem", "internet"],
        boto3_session=boto3_session
    )


# Sidebar 

st.sidebar.title("Filtros")

ufs = carregar_ufs()
uf_selecionada = st.sidebar.selectbox("Estado", ufs, index=ufs.index("SE") if "SE" in ufs else 0)

st.sidebar.markdown("---")
st.sidebar.caption("Dados: INEP — Microdados ENEM 2023")
st.sidebar.caption("Fonte: gov.br/inep")


# Carrega dados do estado selecionado 

df = carregar_dados(uf_selecionada)

notas = ["nota_ciencias_natureza", "nota_ciencias_humanas",
         "nota_linguagens", "nota_matematica", "nota_redacao"]


# Header 

st.title(f"ENEM 2023 — {uf_selecionada}")
st.caption("Análise de desigualdade educacional com base nos microdados públicos do INEP")
st.divider()


# KPIs 

k1, k2, k3, k4 = st.columns(4)

k1.metric("Candidatos", f"{len(df):,}".replace(",", "."))
k2.metric("Média Geral", f"{df['media_geral'].mean():.1f}")
k3.metric("Média Redação", f"{df['nota_redacao'].mean():.1f}")
k4.metric("Média Matemática", f"{df['nota_matematica'].mean():.1f}")

st.divider()


# Linha 1: escola pública vs privada e renda 

col1, col2 = st.columns(2)

with col1:
    st.subheader("Escola pública vs privada")
    escola = (
        df[df["tipo_escola"].isin(["Publica", "Privada"])]
        .groupby("tipo_escola")["media_geral"]
        .mean()
        .reset_index()
        .rename(columns={"tipo_escola": "Tipo", "media_geral": "Média Geral"})
    )
    fig = px.bar(
        escola, x="Tipo", y="Média Geral",
        color="Tipo",
        color_discrete_map={"Publica": "#4472C4", "Privada": "#ED7D31"},
        text_auto=".1f"
    )
    fig.update_layout(showlegend=False, yaxis_range=[400, escola["Média Geral"].max() + 30])
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Média geral por faixa de renda")
    renda = (
        df.groupby(["faixa_renda", "faixa_renda_ordem"])["media_geral"]
        .mean()
        .reset_index()
        .sort_values("faixa_renda_ordem")
        .rename(columns={"faixa_renda": "Renda", "media_geral": "Média Geral"})
    )
    fig2 = px.line(
        renda, x="Renda", y="Média Geral",
        markers=True, color_discrete_sequence=["#4472C4"]
    )
    fig2.update_xaxes(tickangle=45)
    st.plotly_chart(fig2, use_container_width=True)

st.divider()


#  Linha 2: internet e top municípios 

col3, col4 = st.columns(2)

with col3:
    st.subheader("Impacto do acesso à internet")
    internet = (
        df[df["internet"].isin(["Sim", "Nao"])]
        .groupby("internet")["media_geral"]
        .mean()
        .reset_index()
        .rename(columns={"internet": "Internet", "media_geral": "Média Geral"})
    )
    fig3 = px.bar(
        internet, x="Internet", y="Média Geral",
        color="Internet",
        color_discrete_map={"Sim": "#4472C4", "Nao": "#ED7D31"},
        text_auto=".1f"
    )
    fig3.update_layout(showlegend=False, yaxis_range=[400, internet["Média Geral"].max() + 30])
    st.plotly_chart(fig3, use_container_width=True)

with col4:
    st.subheader("Top 10 municípios por média geral")
    municipios = (
        df.groupby("municipio")
        .agg(media=("media_geral", "mean"), candidatos=("media_geral", "count"))
        .reset_index()
        .query("candidatos >= 50")
        .sort_values("media", ascending=False)
        .head(10)
        .rename(columns={"municipio": "Município", "media": "Média Geral"})
    )
    fig4 = px.bar(
        municipios, x="Média Geral", y="Município",
        orientation="h",
        text_auto=".1f",
        color_discrete_sequence=["#4472C4"]
    )
    fig4.update_layout(yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig4, use_container_width=True)

st.divider()


# Linha 3: comparação nacional 

st.subheader("Comparação com a média nacional")

df_nacional = carregar_nacional()

media_uf       = df["media_geral"].mean()
media_nacional = df_nacional["media_geral"].mean()
diferenca      = media_uf - media_nacional

c1, c2, c3 = st.columns(3)
c1.metric("Média nacional", f"{media_nacional:.1f}")
c2.metric(f"Média {uf_selecionada}", f"{media_uf:.1f}")
c3.metric("Diferença", f"{diferenca:+.1f}", delta_color="normal")

ranking = (
    df_nacional.groupby("uf")["media_geral"]
    .mean()
    .reset_index()
    .sort_values("media_geral", ascending=False)
    .reset_index(drop=True)
)
ranking.index += 1
pos = ranking[ranking["uf"] == uf_selecionada].index[0]
st.caption(f"{uf_selecionada} ocupa a posição {pos} de {len(ranking)} estados no ranking nacional.")

fig5 = px.bar(
    ranking.sort_values("media_geral"),
    x="media_geral", y="uf",
    orientation="h",
    color=ranking.sort_values("media_geral")["uf"].apply(
        lambda x: "Selecionado" if x == uf_selecionada else "Outros"
    ),
    color_discrete_map={"Selecionado": "#ED7D31", "Outros": "#4472C4"},
    labels={"media_geral": "Média Geral", "uf": "Estado"}
)
fig5.update_layout(showlegend=False, height=600)
st.plotly_chart(fig5, use_container_width=True)