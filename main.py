import logging
import sys
import os

# Add project root to path if needed
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from pipeline.extrair import extrair
    from pipeline.transformar import transformar
    from pipeline.carregar_aws import carregar_aws
except ImportError as e:
    print(f"Import error: {e}. Ensure 'pipeline' is a package and modules exist.")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

CAMINHO_BRUTO = r"C:\Users\austi\OneDrive\Documentos\Data_Estudos\enem-pipeline\dados_enem\DADOS\MICRODADOS_ENEM_2023.csv"


def executar_pipeline():
    log.info("=" * 60)
    log.info("PIPELINE ENEM 2023 — MICRODADOS INEP")
    log.info("=" * 60)

    df_bruto   = extrair(CAMINHO_BRUTO)
    df_tratado = transformar(df_bruto)
    carregar_aws(df_tratado)

    log.info("=" * 60)
    log.info("Pipeline concluido.")
    log.info("=" * 60)


if __name__ == "__main__":
    executar_pipeline()