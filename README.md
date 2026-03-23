# Pipeline ENEM 2023 — Microdados INEP

Python | Pandas | Dados públicos

---

## Sobre

Pipeline ETL para tratamento dos microdados do ENEM 2023 disponibilizados
publicamente pelo INEP. O arquivo bruto chega com encoding latin-1, separador
ponto e vírgula e variáveis socioeconômicas codificadas em letras. O pipeline
trata tudo isso e entrega um CSV limpo pronto para análise.

Este projeto é a primeira etapa de uma análise sobre desigualdade educacional
no Brasil. O dataset tratado alimenta o projeto de análise exploratória
`enem-analise-exploratoria`.

---

## O que o pipeline faz

**Extract** — lê o CSV bruto em chunks de 100 mil linhas para não estourar
a memória RAM. O arquivo completo tem cerca de 3,9 milhões de registros e
passa de 2 GB descompactado.

**Transform** — filtra candidatos presentes em todas as provas, remove registros
sem nota, calcula a média geral das cinco áreas, decodifica as variáveis
socioeconômicas (renda, tipo de escola, acesso à internet) e renomeia as colunas
para nomes legíveis.

**Load** — salva o dataset tratado em UTF-8 pronto para análise.

---

## Variáveis do dataset de saída

| Coluna | Descrição |
|--------|-----------|
| `ano` | Ano do exame |
| `municipio` | Município onde a prova foi aplicada |
| `uf` | Estado |
| `nota_ciencias_natureza` | Nota em Ciências da Natureza |
| `nota_ciencias_humanas` | Nota em Ciências Humanas |
| `nota_linguagens` | Nota em Linguagens e Códigos |
| `nota_matematica` | Nota em Matemática |
| `nota_redacao` | Nota da Redação |
| `media_geral` | Média das cinco áreas |
| `faixa_renda` | Renda familiar decodificada |
| `faixa_renda_ordem` | Índice numérico da faixa para ordenação em gráficos |
| `tipo_escola` | Pública ou Privada |
| `internet` | Acesso à internet em casa (Sim/Não) |
| `escolaridade_pai` | Escolaridade do pai (código INEP) |
| `escolaridade_mae` | Escolaridade da mãe (código INEP) |

---

## Como executar

Instale as dependências:

```bash
pip install pandas
```

Ajuste os caminhos no início do pipeline.py se necessário:

```python
CAMINHO_BRUTO = r"caminho\para\MICRODADOS_ENEM_2023.csv"
CAMINHO_SAIDA = r"caminho\para\enem_2023_tratado.csv"
```

Execute:

```bash
python pipeline.py
```

O arquivo é lido em chunks de 100 mil linhas — o processo pode levar
entre 5 e 15 minutos dependendo da máquina.

---

## Observações técnicas

O arquivo bruto usa encoding latin-1 — comum em sistemas legados do governo
brasileiro. Tentar abrir com UTF-8 gera erros em caracteres especiais como
acentos e cedilha.

A leitura em chunks (chunksize=100000) é necessária porque o arquivo completo
não cabe na memória RAM de máquinas com menos de 16 GB. Cada chunk é carregado
e concatenado ao final.

---

## Fonte dos dados

INEP — Instituto Nacional de Estudos e Pesquisas Educacionais Anísio Teixeira

Microdados do ENEM 2023 — acesso público via Lei de Acesso à Informação

https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/enem
