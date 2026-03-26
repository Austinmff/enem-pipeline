import streamlit as st
import duckdb
import pandas as pd

# Configuração da página
st.set_page_config(page_title="Analytics ENEM 2023", layout="wide")

st.title("📊 Painel de Microdados ENEM 2023")

# Caminho para o seu arquivo gerado pelo pipeline
PATH_PARQUET = r"C:\Users\austi\OneDrive\Documentos\Data_Estudos\enem-pipeline\dados_enem\DADOS\enem_2023_tratado.parquet"

# Função para conectar ao DuckDB (usamos cache para não reabrir o arquivo toda hora)
@st.cache_resource
def get_connection():
    return duckdb.connect()

con = get_connection()


# --- SIDEBAR / FILTROS ---
st.sidebar.header("Filtros")


# Buscando as UFs únicas usando SQL (muito rápido com DuckDB)
ufs = con.execute(f"SELECT DISTINCT uf FROM '{PATH_PARQUET}' ORDER BY uf").df()
uf_selecionada = st.sidebar.selectbox("Selecione o Estado", ufs['uf'])


#  QUERY PRINCIPAL 
query = f"""
    SELECT 
        municipio, 
        AVG(nota_matematica) as media_mat,
        AVG(nota_redacao) as media_red,
        COUNT(*) as total_candidatos
    FROM '{PATH_PARQUET}'
    WHERE uf = '{uf_selecionada}'
    GROUP BY municipio
    HAVING total_candidatos > 50
    ORDER BY media_mat DESC
    LIMIT 10
"""

df_resultado = con.execute(query).df()

#  VISUALIZAÇÃO
col1, col2 = st.columns([1, 1])

with col1:
    st.subheader(f"Top 10 Municípios em Matemática ({uf_selecionada})")
    st.bar_chart(df_resultado, x="municipio", y="media_mat", color="#ff4b4b")

with col2:
    st.subheader("Resumo Estatístico")
    st.dataframe(df_resultado, use_container_width=True)

st.divider()

# Exemplo de KPI
media_estado = con.execute(f"SELECT AVG(media_geral) FROM '{PATH_PARQUET}' WHERE uf = '{uf_selecionada}'").fetchone()[0]
st.metric(label=f"Média Geral do Estado de {uf_selecionada}", value=f"{media_estado:.2f}")