"""
Script: load_fca_cia_aberta.py
Descrição: Popula a tabela cvm_data.fca_cia_aberta com dados do CSV da CVM.
           Utiliza UPSERT (INSERT ... ON CONFLICT DO UPDATE) para evitar duplicatas.

Dependências:
    pip install pandas psycopg2-binary python-dotenv

Arquivo .env esperado (na mesma pasta do script):
    DB_USER=postgres
    DB_PASSWORD=sua_senha
    DB_HOST=localhost
    DB_PORT=5432
    DB_NAME=cvm_data

Uso:
    python load_fca_cia_aberta.py
"""

import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import logging
import sys
from pathlib import Path

# ─── CONFIGURAÇÕES DE CONEXÃO (via .env) ─────────────────────────────────────
load_dotenv()
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_URL  = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

CSV_PATH = r"D:\DATACVM\Formulário Cadastral (FCA)\fca_cia_aberta_2026\fca_cia_aberta_2026.csv"
CSV_SEPARATOR = ";"
CSV_ENCODING  = "utf-8"
BATCH_SIZE    = 1000   # Número de linhas por lote de inserção

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("load_fca_cia_aberta.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# MAPEAMENTO DE COLUNAS: CSV → tabela
# Chave = nome original no CSV | Valor = nome na tabela
# ---------------------------------------------------------------------------

COLUMN_MAP = {
    "CNPJ_CIA":  "cnpj_cia",
    "DT_REFER":  "dt_refer",
    "VERSAO":    "versao",
    "DENOM_CIA": "denom_cia",
    "CD_CVM":    "cd_cvm",
    "CATEG_DOC": "categ_doc",
    "ID_DOC":    "id_doc",
    "DT_RECEB":  "dt_receb",
    "LINK_DOC":  "link_doc",
}

# Colunas que serão atualizadas em caso de conflito (exceto a chave de conflito)
CONFLICT_KEY    = "id_doc"          # coluna usada para detectar duplicata
UPDATE_COLS     = [c for c in COLUMN_MAP.values() if c != CONFLICT_KEY]

# ---------------------------------------------------------------------------
# FUNÇÕES
# ---------------------------------------------------------------------------

def read_csv(path: str) -> pd.DataFrame:
    """Lê o CSV e renomeia colunas para o padrão da tabela."""
    log.info(f"Lendo arquivo: {path}")
    if not Path(path).exists():
        log.error(f"Arquivo não encontrado: {path}")
        sys.exit(1)

    df = pd.read_csv(
        path,
        sep=CSV_SEPARATOR,
        encoding=CSV_ENCODING,
        dtype=str,          # lê tudo como string; conversão feita abaixo
        low_memory=False,
    )

    log.info(f"Total de linhas lidas: {len(df):,}")

    # Padroniza nomes de colunas (remove espaços e coloca em maiúsculo)
    df.columns = df.columns.str.strip().str.upper()

    # Verifica se as colunas esperadas existem
    missing = [c for c in COLUMN_MAP.keys() if c not in df.columns]
    if missing:
        log.error(f"Colunas não encontradas no CSV: {missing}")
        log.error(f"Colunas disponíveis: {list(df.columns)}")
        sys.exit(1)

    # Seleciona e renomeia apenas as colunas necessárias
    df = df[list(COLUMN_MAP.keys())].rename(columns=COLUMN_MAP)

    return df


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Trata tipos de dados e valores nulos."""
    log.info("Aplicando limpeza e conversão de tipos...")

    # Datas
    for col in ["dt_refer", "dt_receb"]:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    # Numéricos
    df["versao"] = pd.to_numeric(df["versao"], errors="coerce").astype("Int16")
    df["id_doc"] = pd.to_numeric(df["id_doc"],  errors="coerce").astype("Int64")

    # Strings — remove espaços extras e substitui string vazia por None
    str_cols = ["cnpj_cia", "denom_cia", "cd_cvm", "categ_doc", "link_doc"]
    for col in str_cols:
        df[col] = df[col].str.strip().replace("", None)

    # Remove linhas sem id_doc (chave de conflito obrigatória)
    before = len(df)
    df = df.dropna(subset=["id_doc"])
    dropped = before - len(df)
    if dropped:
        log.warning(f"{dropped} linhas removidas por ausência de id_doc.")

    log.info(f"Linhas válidas após limpeza: {len(df):,}")
    return df


def build_upsert_query() -> str:
    """Monta a query de UPSERT dinâmica."""
    cols        = list(COLUMN_MAP.values())
    cols_str    = ", ".join(cols)
    placeholders = "%s"                          # execute_values usa %s por tupla
    update_set  = ", ".join(f"{c} = EXCLUDED.{c}" for c in UPDATE_COLS)

    query = f"""
        INSERT INTO cvm_data.fca_cia_aberta ({cols_str})
        VALUES %s
        ON CONFLICT ({CONFLICT_KEY})
        DO UPDATE SET {update_set};
    """
    return query


def insert_batches(conn, df: pd.DataFrame) -> None:
    """Insere os dados em lotes usando execute_values."""
    query   = build_upsert_query()
    cols    = list(COLUMN_MAP.values())
    total   = len(df)
    inserted = 0

    with conn.cursor() as cur:
        for start in range(0, total, BATCH_SIZE):
            batch = df.iloc[start : start + BATCH_SIZE]
            # Converte para lista de tuplas, substituindo pd.NA por None
            records = [
                tuple(None if pd.isna(v) else v for v in row)
                for row in batch[cols].itertuples(index=False, name=None)
            ]
            execute_values(cur, query, records)
            conn.commit()
            inserted += len(records)
            log.info(f"  Progresso: {inserted:,} / {total:,} linhas inseridas/atualizadas")

    log.info(f"✔ Carga concluída: {inserted:,} linhas processadas.")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    log.info("=== Início da carga: fca_cia_aberta ===")

    # 1. Lê e limpa o CSV
    df = read_csv(CSV_PATH)
    df = clean_data(df)

    # 2. Conecta ao banco
    log.info(f"Conectando ao PostgreSQL: {DB_HOST}:{DB_PORT} / {DB_NAME}")
    try:
        conn = psycopg2.connect(DB_URL)
    except psycopg2.OperationalError as e:
        log.error(f"Falha na conexão: {e}")
        sys.exit(1)

    # 3. Insere os dados
    try:
        insert_batches(conn, df)
    except Exception as e:
        conn.rollback()
        log.error(f"Erro durante a inserção: {e}")
        raise
    finally:
        conn.close()
        log.info("Conexão encerrada.")

    log.info("=== Carga finalizada com sucesso ===")


if __name__ == "__main__":
    main()