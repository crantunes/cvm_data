"""
Script : load_fre_historico_emissor.py
Tabela : cvm_data.fre_historico_emissor
Fonte  : fre_cia_aberta_historico_emissor_<ANO>.csv
Path   : D:\\DATACVM\\Formulário de Referência (FRE)\\fre_cia_aberta_<ANO>\\

Variáveis do paper:
  ln(Age) → data_constituicao_emissor
    Permite calcular idade histórica por data de referência, complementando
    o campo data_constituicao que já existe em cad_cia_aberta (FCA).

Disponível: 2010–2023 (CVM descontinuou a partir de 2024).
Anos 2024+ não terão arquivo — o script pula silenciosamente.

Estratégia:
  • Proteção por ano: pula se fre_ano já tiver dados.
    Para reprocessar: DELETE FROM cvm_data.fre_historico_emissor WHERE fre_ano = XXXX;
  • Inserção raw. Filtro de versão mais recente via query:
      SELECT DISTINCT ON (cnpj_companhia, data_referencia)
             *
      FROM cvm_data.fre_historico_emissor
      ORDER BY cnpj_companhia, data_referencia, versao DESC;
"""

import os
import sys
import logging
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from pathlib import Path

# ─── CONFIGURAÇÕES ────────────────────────────────────────────────────────────
load_dotenv()
DB_URL = "postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}".format(**{
    k: os.getenv(k, "") for k in ["DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT", "DB_NAME"]
})

# 2024+ não existem — o script pula arquivos ausentes automaticamente
ANOS_CARGA    = list(range(2010, 2027))
CSV_BASE_PATH = Path(r"D:\DATACVM\Formulário de Referência (FRE)")
CSV_SEPARATOR = ";"
CSV_ENCODING  = "latin-1"
BATCH_SIZE    = 2000

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("load_fre_historico_emissor.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ─── MAPEAMENTO CSV → TABELA ──────────────────────────────────────────────────
COL_MAP = {
    "CNPJ_Companhia"                    : "cnpj_companhia",
    "Data_Referencia"                   : "data_referencia",
    "Versao"                            : "versao",
    "ID_Documento"                      : "id_documento",
    "Nome_Companhia"                    : "nome_companhia",
    "Data_Constituicao_Emissor"         : "data_constituicao_emissor",
    "Data_Registro_Emissor"             : "data_registro_emissor",
    "Prazo_Duracao_Emissor"             : "prazo_duracao_emissor",
    "Requisicao_Registro_Emissor"       : "requisicao_registro_emissor",
    "Pais_Constituicao_Emissor"         : "pais_constituicao_emissor",
    "Sigla_Pais_Constituicao_Emissor"   : "sigla_pais_constituicao_emissor",
    "Forma_Constituicao_Emissor"        : "forma_constituicao_emissor",
}

DATE_COLS = [
    "Data_Referencia", "Data_Constituicao_Emissor",
    "Data_Registro_Emissor", "Prazo_Duracao_Emissor",
]
NUMERIC_COLS = ["Versao", "ID_Documento"]

INSERT_COLS = list(COL_MAP.values())


# ─── FUNÇÕES ──────────────────────────────────────────────────────────────────

def safe_val(v):
    if v is None or v is pd.NaT:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    return v


def ano_ja_carregado(conn, ano: int) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(1) FROM cvm_data.fre_historico_emissor WHERE fre_ano = %s", (ano,)
        )
        count = cur.fetchone()[0]
    if count > 0:
        log.warning(f"  fre_ano={ano} já possui {count:,} registros — pulando.")
        log.warning(f"  Para reprocessar: DELETE FROM cvm_data.fre_historico_emissor WHERE fre_ano = {ano};")
        return True
    return False


def csv_path(ano: int) -> Path:
    return CSV_BASE_PATH / f"fre_cia_aberta_{ano}" / f"fre_cia_aberta_historico_emissor_{ano}.csv"


def read_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(
        path, sep=CSV_SEPARATOR, encoding=CSV_ENCODING,
        dtype=str, low_memory=False
    )
    df.columns = df.columns.str.strip()

    for col in DATE_COLS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    for col in NUMERIC_COLS:
        if col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].str.replace(",", ".", regex=False)
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for csv_col in COL_MAP.keys():
        if csv_col not in df.columns:
            df[csv_col] = None

    nao_mapeadas = [c for c in df.columns if c not in COL_MAP]
    if nao_mapeadas:
        log.info(f"    Colunas não mapeadas (ignoradas): {nao_mapeadas}")

    return df


def insert_ano(conn, df: pd.DataFrame, ano: int) -> int:
    df_ins = df[list(COL_MAP.keys())].rename(columns=COL_MAP)
    df_ins.insert(0, "fre_ano", ano)

    all_cols = ["fre_ano"] + INSERT_COLS
    cols_str = ", ".join(all_cols)
    query    = f"INSERT INTO cvm_data.fre_historico_emissor ({cols_str}) VALUES %s"

    total    = len(df_ins)
    inserted = 0

    with conn.cursor() as cur:
        for start in range(0, total, BATCH_SIZE):
            batch   = df_ins.iloc[start : start + BATCH_SIZE]
            records = [
                tuple(safe_val(v) for v in row)
                for row in batch[all_cols].itertuples(index=False, name=None)
            ]
            execute_values(cur, query, records)
            conn.commit()
            inserted += len(records)

    return inserted


def main():
    log.info("=" * 70)
    log.info("Início da carga: fre_historico_emissor")
    log.info(f"Anos a processar: {ANOS_CARGA}")
    log.info(f"Nota: disponível apenas até 2023 — anos 2024+ serão pulados.")
    log.info("=" * 70)

    try:
        conn = psycopg2.connect(DB_URL)
    except psycopg2.OperationalError as e:
        log.error(f"Falha na conexão: {e}")
        sys.exit(1)

    total_geral = 0
    try:
        for ano in ANOS_CARGA:
            path = csv_path(ano)
            log.info(f"\n[{ano}] {path.name}")

            if not path.exists():
                log.info(f"  Arquivo não encontrado — pulando (esperado para 2024+).")
                continue

            if ano_ja_carregado(conn, ano):
                continue

            df = read_csv(path)
            n  = insert_ano(conn, df, ano)
            total_geral += n
            log.info(f"  ✔ {n:,} registros inseridos.")

    except Exception as e:
        conn.rollback()
        log.error(f"Erro: {e}")
        raise
    finally:
        conn.close()
        log.info("Conexão encerrada.")

    log.info("=" * 70)
    log.info(f"✔ fre_historico_emissor: {total_geral:,} registros inseridos no total.")
    log.info("=" * 70)


if __name__ == "__main__":
    main()