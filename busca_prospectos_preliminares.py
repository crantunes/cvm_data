# busca_prospectos_preliminares.py

import os
import requests
import pandas as pd
import psycopg2
import time
from dotenv import load_dotenv

load_dotenv()

# =========================================================
# 1. Conexão com o banco
# =========================================================
conn = psycopg2.connect(
    host     = os.getenv("DB_HOST"),
    port     = os.getenv("DB_PORT"),
    dbname   = os.getenv("DB_NAME"),
    user     = os.getenv("DB_USER"),
    password = os.getenv("DB_PASSWORD")
)
cursor = conn.cursor()

# =========================================================
# 2. Buscar os 98 IPOs com codigo_cvm e ticker
# =========================================================
query_ipos = """
WITH ipo_base AS (
    SELECT DISTINCT ON (i.cnpj_emissor)
        c.codigo_cvm,
        v.codigo_negociacao          AS ticker,
        i.cnpj_emissor,
        i.nome_emissor,
        i.data_abertura_processo     AS data_ipo_cvm,
        i.preco_unitario             AS preco_emissao
    FROM ipo_oferta_distribuicao i
    JOIN cad_cia_aberta       c ON c.cnpj_companhia = i.cnpj_emissor
    JOIN cad_valor_mobiliario v ON v.cnpj_companhia = i.cnpj_emissor
    WHERE i.oferta_inicial = 'S'::bpchar
      AND i.data_abertura_processo >= '2010-01-01'::date
      AND i.tipo_oferta::text = ANY (ARRAY['Primária','Secundária','Mista']::text[])
      AND lower(i.nome_emissor) !~ 'banco'
      AND lower(i.nome_emissor) !~ 'segur'
      AND lower(i.nome_emissor) !~ 'insur'
      AND (i.tipo_componente_oferta_mista::text <> 'Secundária'::text
           OR i.tipo_componente_oferta_mista IS NULL)
      AND i.cnpj_emissor IS NOT NULL
      AND LENGTH(TRIM(v.codigo_negociacao)) BETWEEN 4 AND 7
      AND v.codigo_negociacao IS NOT NULL
      AND v.codigo_negociacao ~ '^[A-Z]{4}[0-9]{1,2}$'
    ORDER BY i.cnpj_emissor,
             i.data_abertura_processo ASC,
             i.data_registro_oferta   ASC,
             v.codigo_negociacao      ASC
),
d1_b3 AS (
    SELECT DISTINCT ON (symbol)
        symbol,
        MIN(refdate) OVER (PARTITION BY symbol) AS data_d1
    FROM b3_equities
),
preco_d1 AS (
    SELECT e.symbol, e.refdate AS data_d1, e.close AS preco_fechamento_d1
    FROM b3_equities e
    JOIN (SELECT symbol, MIN(refdate) AS data_d1 FROM b3_equities GROUP BY symbol) p
      ON p.symbol = e.symbol AND p.data_d1 = e.refdate
)
SELECT
    b.codigo_cvm,
    b.ticker,
    b.cnpj_emissor,
    b.nome_emissor,
    b.data_ipo_cvm,
    d.data_d1,
    b.preco_emissao,
    d.preco_fechamento_d1
FROM ipo_base b
JOIN preco_d1 d ON d.symbol = b.ticker
ORDER BY d.data_d1 ASC
"""

df_ipos = pd.read_sql(query_ipos, conn)
print(f"Total de IPOs na amostra: {len(df_ipos)}")

# =========================================================
# 3. Buscar documentos na API da CVM (eDocs)
#    Endpoint: https://www.rad.cvm.gov.br/ENET/frmConsultaExternaCVM.aspx
#    API de dados abertos: https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/
# =========================================================

BASE_URL = "https://dados.cvm.gov.br/api/dados/cia_aberta/doc/prospecto/"

def buscar_prospecto_preliminar(codigo_cvm: str, nome_emissor: str) -> dict:
    """
    Busca prospecto preliminar de um IPO via API de dados abertos da CVM.
    Retorna dicionário com link e data do documento, se encontrado.
    """
    url = f"https://www.rad.cvm.gov.br/ENET/frmConsultaExternaCVM.aspx"
    
    # Endpoint alternativo via dados abertos CVM
    url_api = (
        f"https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/PROSPECTO/DADOS/"
        f"prospecto_cia_aberta.csv"
    )
    
    resultado = {
        "codigo_cvm"  : codigo_cvm,
        "nome_emissor": nome_emissor,
        "link"        : None,
        "data_doc"    : None,
        "categoria"   : None,
        "status"      : "nao_encontrado"
    }
    return resultado


# =========================================================
# 4. Abordagem correta: arquivo CSV de prospectos da CVM
#    Download único do arquivo consolidado
# =========================================================
print("\nBaixando arquivo de prospectos da CVM...")
url_prospectos = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/PROSPECTO/DADOS/prospecto_cia_aberta.csv"

try:
    resp = requests.get(url_prospectos, timeout=60)
    resp.encoding = "latin-1"
    
    # Salvar localmente
    with open("d:/VSCode/cvm_data/prospecto_cia_aberta.csv", "wb") as f:
        f.write(resp.content)
    
    print("Arquivo baixado com sucesso.")
    
    # Ler o CSV
    df_prospectos = pd.read_csv(
        "d:/VSCode/cvm_data/prospecto_cia_aberta.csv",
        sep=";",
        encoding="latin-1",
        on_bad_lines="skip"
    )
    
    print(f"Colunas disponíveis: {df_prospectos.columns.tolist()}")
    print(f"Total de registros: {len(df_prospectos)}")
    print(df_prospectos.head(3).to_string())

except Exception as e:
    print(f"Erro ao baixar prospectos: {e}")

# =========================================================
# 5. Cruzar prospectos com os 98 IPOs
#    Filtrar por: categoria = "Prospecto Preliminar"
#                 codigo_cvm in lista dos 98 IPOs
# =========================================================
try:
    # Filtrar apenas prospectos preliminares
    col_categoria = [c for c in df_prospectos.columns if "categ" in c.lower() or "tipo" in c.lower()]
    col_codigo    = [c for c in df_prospectos.columns if "codigo" in c.lower() or "cd_" in c.lower()]
    
    print(f"\nColunas de categoria: {col_categoria}")
    print(f"Colunas de código: {col_codigo}")
    
    # Listar categorias únicas para identificar a certa
    for col in col_categoria:
        print(f"\nValores únicos em '{col}':")
        print(df_prospectos[col].value_counts().head(10))

except Exception as e:
    print(f"Erro no cruzamento: {e}")

conn.close()
print("\nScript finalizado.")
