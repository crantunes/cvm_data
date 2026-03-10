"""
Script: fca_cia_aberta_pais_estrangeiro_negociacao.py

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

# ─── CONFIGURAÇÕES DO CSV ─────────────────────────────────────────────────────
CSV_PATH      = r"D:\DATACVM\Formulário Cadastral (FCA)\fca_cia_aberta_2026\fca_cia_aberta_pais_estrangeiro_negociacao_2026.csv"
CSV_SEPARATOR = ";"
CSV_ENCODING  = "latin-1"
BATCH_SIZE    = 1000  # Número de linhas por lote de inserção

# ─── ANO DE REFERÊNCIA DO CSV ─────────────────────────────────────────────────
# Altere este valor manualmente ao carregar CSVs de outros anos (ex: 2025, 2027)
FCA_ANO = 2026

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("load_fca_cia_aberta_pais_estrangeiro_negociacao.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ─── MAPEAMENTO DE COLUNAS: CSV → tabela ──────────────────────────────────────
# Chave = nome original no CSV | Valor = nome na tabela
COLUMN_MAP = {
    "CNPJ_Companhia":        "cnpj_companhia",
    "Data_Referencia":       "data_referencia",
    "Versao":                "versao",
    "ID_Documento":          "id_documento",
    "Nome_Empresarial":      "nome_empresarial",
    "Pais":                  "pais",
    "Data_Admissao_Negociacao": "data_admissao_negociacao",
}

# fca_ano não vem do CSV — é adicionado manualmente
ALL_COLS = list(COLUMN_MAP.values()) + ["fca_ano"]

# ─── FUNÇÕES ──────────────────────────────────────────────────────────────────

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
        dtype=str,
        low_memory=False,
    )

    log.info(f"Total de linhas lidas: {len(df):,}")

    # Padroniza nomes de colunas (remove espaços extras)
    df.columns = df.columns.str.strip()

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
    for col in ["data_referencia", "data_admissao_negociacao"]:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    # Numéricos
    df["versao"]       = pd.to_numeric(df["versao"],       errors="coerce").astype("Int16")
    df["id_documento"] = pd.to_numeric(df["id_documento"], errors="coerce").astype("Int64")

    # Strings — remove espaços extras e substitui string vazia por None
    for col in ["cnpj_companhia", "nome_empresarial", "pais"]:
        df[col] = df[col].str.strip().replace("", None)

    # Remove linhas sem campos mínimos obrigatórios
    before = len(df)
    df = df.dropna(subset=["cnpj_companhia", "id_documento"])
    dropped = before - len(df)
    if dropped:
        log.warning(f"{dropped} linhas removidas por ausência de cnpj_companhia ou id_documento.")

    # Adiciona coluna fca_ano com valor fixo definido em FCA_ANO
    df["fca_ano"] = FCA_ANO

    log.info(f"Linhas válidas após limpeza: {len(df):,}")
    return df


def build_insert_query() -> str:
    """Monta a query de INSERT simples (sem ON CONFLICT — duplicatas são permitidas)."""
    cols_str = ", ".join(ALL_COLS)
    query = f"""
        INSERT INTO cvm_data.fca_cia_aberta_pais_estrangeiro_negociacao ({cols_str})
        VALUES %s;
    """
    return query


def year_already_loaded(conn) -> bool:
    """Verifica se já existem registros do fca_ano no banco para evitar reprocessamento."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(1) FROM cvm_data.fca_cia_aberta_pais_estrangeiro_negociacao WHERE fca_ano = %s",
            (FCA_ANO,)
        )
        count = cur.fetchone()[0]
    if count > 0:
        log.warning(
            f"Atenção: já existem {count:,} registros com fca_ano={FCA_ANO} no banco. "
            f"Abortando para evitar duplicação. "
            f"Para forçar o recarregamento, apague os registros com: "
            f"DELETE FROM cvm_data.fca_cia_aberta_pais_estrangeiro_negociacao WHERE fca_ano = {FCA_ANO};"
        )
        return True
    return False


def insert_batches(conn, df: pd.DataFrame) -> None:
    """Insere os dados em lotes usando execute_values."""
    query    = build_insert_query()
    total    = len(df)
    inserted = 0

    with conn.cursor() as cur:
        for start in range(0, total, BATCH_SIZE):
            batch = df.iloc[start : start + BATCH_SIZE]
            records = [
                tuple(None if pd.isna(v) else v.item() if hasattr(v, "item") else v for v in row)
                for row in batch[ALL_COLS].itertuples(index=False, name=None)
            ]
            execute_values(cur, query, records)
            conn.commit()
            inserted += len(records)
            log.info(f"  Progresso: {inserted:,} / {total:,} linhas inseridas")

    log.info(f"✔ Carga concluída: {inserted:,} linhas inseridas.")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    log.info("=== Início da carga: fca_cia_aberta_pais_estrangeiro_negociacao ===")

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

    # 3. Verifica reprocessamento e insere os dados
    try:
        if year_already_loaded(conn):
            sys.exit(0)
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