"""
Script : load_dfp_status_entraga.py
Tabela : cvm_data.dfp_status_entraga
Fonte  : dfp_cia_aberta_<ANO>.csv

Fluxo:
  1. Lê o CSV e descarta DENOM_CIA
  2. Desduplicação: mantém apenas a linha com VERSAO mais alta (= DT_RECEB mais
     recente) para cada par cnpj_cia + dt_refer — versões anteriores são descartadas
  3. Lookup: verifica se TODOS os CNPJs do CSV existem em cad_cia_aberta.
             Se houver divergências → aborta e lista os CNPJs ausentes.
  4. Faz o JOIN cnpj → id_cad_cia_aberta em memória (sem query por linha)
  5. UPSERT: ON CONFLICT (cnpj_cia, dt_refer) → UPDATE (atualiza versão/dados)
  6. year_already_loaded: proteção contra reprocessamento duplo do mesmo dfp_ano

Dependências:
    pip install pandas psycopg2-binary python-dotenv

Arquivo .env (mesma pasta do script):
    DB_USER=postgres
    DB_PASSWORD=...
    DB_HOST=localhost
    DB_PORT=5432
    DB_NAME=cvm_data
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

CSV_PATH      = r"D:\DATACVM\Formulário de Demonstrações Financeiras Padronizadas (DFP)\dfp_cia_aberta_2025\dfp_cia_aberta_2025.csv"
CSV_SEPARATOR = ";"
CSV_ENCODING  = "latin-1"
BATCH_SIZE    = 500

# ─── ANO DE REFERÊNCIA ────────────────────────────────────────────────────────
# Altere este valor manualmente ao rodar cada ano (ex: 2023, 2024, 2025)
DFP_ANO = 2025

# ─── MAPEAMENTO DE COLUNAS CSV → tabela ──────────────────────────────────────
# DENOM_CIA é ignorado intencionalmente — não consta neste mapa
COLUMN_MAP = {
    "CNPJ_CIA"  : "cnpj_cia",
    "DT_REFER"  : "dt_refer",
    "VERSAO"    : "versao",
    "CD_CVM"    : "cd_cvm",
    "CATEG_DOC" : "categ_doc",
    "ID_DOC"    : "id_doc",
    "DT_RECEB"  : "dt_receb",
    "LINK_DOC"  : "link_doc",
}

# Ordem das colunas para INSERT (sem id_dfp_status_entraga que é SERIAL)
INSERT_COLS = [
    "id_cad_cia_aberta",
    "cnpj_cia",
    "dt_refer",
    "versao",
    "dfp_ano",
    "cd_cvm",
    "categ_doc",
    "id_doc",
    "dt_receb",
    "link_doc",
]

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("load_dfp_status_entraga.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# ─── FUNÇÕES ──────────────────────────────────────────────────────────────────

def read_csv() -> pd.DataFrame:
    """
    Lê o CSV, descarta DENOM_CIA, converte tipos e elimina versões
    duplicadas de cnpj_cia + dt_refer mantendo apenas a de VERSAO mais alta.
    """
    path = Path(CSV_PATH)
    if not path.exists():
        log.error(f"Arquivo não encontrado: {CSV_PATH}")
        sys.exit(1)

    log.info(f"Lendo CSV: {CSV_PATH}")
    df = pd.read_csv(
        path,
        sep=CSV_SEPARATOR,
        encoding=CSV_ENCODING,
        dtype=str,
        low_memory=False,
    )
    df.columns = df.columns.str.strip()
    log.info(f"  {len(df):,} linhas lidas. Colunas: {list(df.columns)}")

    # ── Verifica colunas obrigatórias ────────────────────────────────────────
    missing = [c for c in COLUMN_MAP.keys() if c not in df.columns]
    if missing:
        log.error(f"Colunas ausentes no CSV: {missing}")
        log.error(f"Colunas disponíveis: {list(df.columns)}")
        sys.exit(1)

    # ── Seleciona colunas necessárias (descarta DENOM_CIA e quaisquer outras) ─
    df = df[list(COLUMN_MAP.keys())].rename(columns=COLUMN_MAP)

    # ── Conversão de tipos ───────────────────────────────────────────────────
    df["cnpj_cia"]  = df["cnpj_cia"].str.strip()
    df["cd_cvm"]    = df["cd_cvm"].str.strip().replace("", None)
    df["categ_doc"] = df["categ_doc"].str.strip().replace("", None)
    df["link_doc"]  = df["link_doc"].str.strip().replace("", None)

    df["dt_refer"] = pd.to_datetime(df["dt_refer"], errors="coerce").dt.date
    df["dt_receb"] = pd.to_datetime(df["dt_receb"], errors="coerce").dt.date
    df["versao"]   = pd.to_numeric(df["versao"], errors="coerce").astype("Int16")
    df["id_doc"]   = pd.to_numeric(df["id_doc"],  errors="coerce").astype("Int64")

    # ── Remove linhas sem chave mínima ───────────────────────────────────────
    before = len(df)
    df = df.dropna(subset=["cnpj_cia", "dt_refer", "versao"])
    dropped = before - len(df)
    if dropped:
        log.warning(f"  {dropped} linha(s) removidas por ausência de cnpj_cia, dt_refer ou versao.")

    # ── Desduplicação: mantém VERSAO mais alta por cnpj_cia + dt_refer ───────
    # O CSV pode conter várias versões do mesmo documento (reapresentações).
    # A versão mais alta corresponde também à DT_RECEB mais recente.
    before_dedup = len(df)
    df = (
        df.sort_values("versao", ascending=False)
          .drop_duplicates(subset=["cnpj_cia", "dt_refer"], keep="first")
          .sort_index()
          .reset_index(drop=True)
    )
    dupes = before_dedup - len(df)
    if dupes:
        log.warning(
            f"  {dupes} linha(s) descartadas por controle de versão "
            f"(mantida a VERSAO mais alta para cada cnpj_cia + dt_refer)."
        )

    # ── Adiciona dfp_ano ─────────────────────────────────────────────────────
    df["dfp_ano"] = DFP_ANO

    log.info(f"  {len(df):,} linhas válidas após limpeza e desduplicação.")
    return df


def lookup_cnpjs(conn, df: pd.DataFrame) -> dict:
    """
    Verifica se TODOS os CNPJs do CSV existem em cad_cia_aberta.
    → OK : retorna dict {cnpj: id_cad_cia_aberta}
    → NOK: aborta e lista os CNPJs ausentes para cadastramento prévio.
    """
    cnpjs_csv = set(df["cnpj_cia"].dropna().unique())
    log.info(f"  Lookup: {len(cnpjs_csv):,} CNPJs únicos no CSV.")

    with conn.cursor() as cur:
        cur.execute(
            "SELECT cnpj_companhia, id_cad_cia_aberta FROM cvm_data.cad_cia_aberta"
        )
        rows = cur.fetchall()

    cad_map    = {row[0]: row[1] for row in rows}
    cnpjs_cad  = set(cad_map.keys())
    ausentes   = sorted(cnpjs_csv - cnpjs_cad)

    if ausentes:
        log.error("=" * 70)
        log.error("ABORTANDO — CNPJs do CSV ausentes em cad_cia_aberta:")
        log.error("=" * 70)
        for cnpj in ausentes:
            log.error(f"  ✗  {cnpj}")
        log.error("=" * 70)
        log.error(
            f"Total: {len(ausentes)} CNPJ(s) divergente(s). "
            "Cadastre-os em cad_cia_aberta antes de reprocessar."
        )
        conn.close()
        sys.exit(1)

    log.info(f"  Lookup OK — todos os {len(cnpjs_csv):,} CNPJs encontrados em cad_cia_aberta.")
    return cad_map


def year_already_loaded(conn) -> bool:
    """Evita reprocessamento acidental do mesmo dfp_ano."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(1) FROM cvm_data.dfp_status_entraga WHERE dfp_ano = %s",
            (DFP_ANO,)
        )
        count = cur.fetchone()[0]

    if count > 0:
        log.warning(
            f"dfp_ano={DFP_ANO} já possui {count:,} registros em dfp_status_entraga — "
            "abortando para evitar duplicação.\n"
            f"Para reprocessar: DELETE FROM cvm_data.dfp_status_entraga WHERE dfp_ano = {DFP_ANO};"
        )
        return True
    return False


def build_upsert_query() -> str:
    """
    ON CONFLICT na chave (cnpj_cia, dt_refer):
    Se o registro já existir (ex: reprocessamento com versão mais recente),
    atualiza todos os campos de dados.
    """
    cols_str   = ", ".join(INSERT_COLS)
    update_set = ", ".join(
        f"{c} = EXCLUDED.{c}"
        for c in INSERT_COLS
        if c not in ("cnpj_cia", "dt_refer")   # chave de conflito não atualiza
    ) + ", dt_ultima_atualizacao = NOW()"

    return f"""
        INSERT INTO cvm_data.dfp_status_entraga ({cols_str})
        VALUES %s
        ON CONFLICT ON CONSTRAINT uq_dfp_status_entraga_cnpj_dt
        DO UPDATE SET {update_set};
    """


def insert_batches(conn, df: pd.DataFrame, cad_map: dict) -> None:
    """Insere os registros em lotes via UPSERT."""

    # Mapeia id_cad_cia_aberta em memória
    df = df.copy()
    df["id_cad_cia_aberta"] = df["cnpj_cia"].map(cad_map)

    query    = build_upsert_query()
    total    = len(df)
    inserted = 0

    with conn.cursor() as cur:
        for start in range(0, total, BATCH_SIZE):
            batch   = df.iloc[start : start + BATCH_SIZE]
            records = [
                tuple(
                    None if (v is None or v is pd.NaT or (isinstance(v, float) and pd.isna(v))) else int(v) if hasattr(v, "item") else v
                    for v in row
                )
                for row in batch[INSERT_COLS].itertuples(index=False, name=None)
            ]
            execute_values(cur, query, records)
            conn.commit()
            inserted += len(records)
            log.info(f"  Progresso: {inserted:,} / {total:,} linhas processadas.")

    log.info(f"✔ Carga concluída: {inserted:,} linhas inseridas/atualizadas.")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 70)
    log.info(f"Início da carga: dfp_status_entraga | dfp_ano={DFP_ANO}")
    log.info("=" * 70)

    # 1. Lê, limpa e desduplicaz o CSV
    df = read_csv()

    # 2. Conecta ao banco
    log.info("Conectando ao PostgreSQL...")
    try:
        conn = psycopg2.connect(DB_URL)
    except psycopg2.OperationalError as e:
        log.error(f"Falha na conexão: {e}")
        sys.exit(1)

    try:
        # 3. Proteção contra reprocessamento duplo
        if year_already_loaded(conn):
            sys.exit(0)

        # 4. Lookup de CNPJs — aborta se houver divergências
        log.info("Executando lookup de CNPJs...")
        cad_map = lookup_cnpjs(conn, df)

        # 5. Insere os dados
        log.info("Iniciando inserção em lotes...")
        insert_batches(conn, df, cad_map)

    except SystemExit:
        raise
    except Exception as e:
        conn.rollback()
        log.error(f"Erro durante a carga: {e}")
        raise
    finally:
        conn.close()
        log.info("Conexão encerrada.")

    log.info("=" * 70)
    log.info("Carga dfp_status_entraga finalizada com sucesso.")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
