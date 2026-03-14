"""
Script de diagnóstico — rode ANTES de corrigir os scripts principais.
Inspeciona:
  1. Colunas reais do IVol-BR.xls (para corrigir o parse)
  2. Formatos de CNPJ nas tabelas do banco (para corrigir o join BRAPI)

Execução:
  python diagnostico.py
"""

import io, os, sys
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()
DB_URL = "postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}".format(**{
    k: os.getenv(k, "") for k in ["DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT", "DB_NAME"]
})

IVOL_PATH = Path(r"D:\DATACVM\NEFIN\IVol-BR_20260313.xls")

# ── 1. IVol-BR XLS ───────────────────────────────────────────────────────────
print("=" * 60)
print("1. ESTRUTURA DO IVol-BR.xls")
print("=" * 60)

if IVOL_PATH.exists():
    xls = pd.ExcelFile(IVOL_PATH, engine="xlrd")
    print(f"Abas: {xls.sheet_names}")
    for sheet in xls.sheet_names:
        df = xls.parse(sheet, nrows=5)
        print(f"\n  Aba: [{sheet}]")
        print(f"  Colunas: {list(df.columns)}")
        print(f"  Primeiras linhas:")
        print(df.to_string(index=False))
else:
    print(f"ARQUIVO NÃO ENCONTRADO: {IVOL_PATH}")
    print("Ajuste o caminho IVOL_PATH no script.")

# ── 2. Formatos de CNPJ no banco ─────────────────────────────────────────────
print("\n" + "=" * 60)
print("2. FORMATOS DE CNPJ NAS TABELAS")
print("=" * 60)

try:
    conn = psycopg2.connect(DB_URL)
    cur  = conn.cursor()

    # Amostra de CNPJs em cad_valor_mobiliario
    cur.execute("""
        SELECT cnpj_companhia, valor_mobiliario, codigo_negociacao
        FROM cvm_data.cad_valor_mobiliario
        WHERE codigo_negociacao IS NOT NULL
          AND TRIM(codigo_negociacao) <> ''
        LIMIT 10
    """)
    rows = cur.fetchall()
    print("\ncad_valor_mobiliario (amostra):")
    print(f"  {'cnpj_companhia':<22} {'valor_mobiliario':<30} {'ticker'}")
    for r in rows:
        print(f"  {str(r[0]):<22} {str(r[1]):<30} {r[2]}")

    # Amostra de CNPJs em ipo_oferta_distribuicao
    cur.execute("""
        SELECT cnpj_emissor, nome_emissor, oferta_inicial
        FROM cvm_data.ipo_oferta_distribuicao
        WHERE oferta_inicial = 'S'
        LIMIT 10
    """)
    rows = cur.fetchall()
    print("\nipo_oferta_distribuicao (oferta_inicial=S, amostra):")
    print(f"  {'cnpj_emissor':<22} {'nome_emissor':<35} oferta_inicial")
    for r in rows:
        print(f"  {str(r[0]):<22} {str(r[1]):<35} {r[2]}")

    # Testar o join com REGEXP_REPLACE
    cur.execute("""
        SELECT COUNT(*) AS matches
        FROM cvm_data.ipo_oferta_distribuicao o
        JOIN cvm_data.cad_valor_mobiliario    v
            ON REGEXP_REPLACE(v.cnpj_companhia, '[^0-9]', '', 'g')
             = REGEXP_REPLACE(o.cnpj_emissor,   '[^0-9]', '', 'g')
            AND v.valor_mobiliario ILIKE '%AÇÃO%'
        WHERE o.oferta_inicial = 'S'
    """)
    n = cur.fetchone()[0]
    print(f"\nJOIN com REGEXP_REPLACE — matches encontrados: {n}")

    if n == 0:
        # Testar sem filtro de valor_mobiliario
        cur.execute("""
            SELECT COUNT(*) FROM cvm_data.ipo_oferta_distribuicao
            WHERE oferta_inicial = 'S'
        """)
        n_ipo = cur.fetchone()[0]
        print(f"  ipo_oferta_distribuicao WHERE oferta_inicial='S': {n_ipo} linhas")

        cur.execute("SELECT COUNT(*) FROM cvm_data.cad_valor_mobiliario")
        n_vm = cur.fetchone()[0]
        print(f"  cad_valor_mobiliario total: {n_vm} linhas")

        cur.execute("""
            SELECT DISTINCT valor_mobiliario
            FROM cvm_data.cad_valor_mobiliario
            LIMIT 20
        """)
        vals = [r[0] for r in cur.fetchall()]
        print(f"  valores distintos em valor_mobiliario: {vals}")

    conn.close()
except Exception as e:
    print(f"Erro banco: {e}")