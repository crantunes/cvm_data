"""
Script : load_dfp_composicao_capital.py
Tabela : cvm_data.dfp_composicao_capital
Fonte  : dfp_cia_aberta_composicao_capital_<ANO>.csv

Fluxo:
  1. Lê o CSV e descarta DENOM_CIA
  2. Desduplicação: CNPJs com múltiplas versões → mantém a versão mais alta
  3. Lookup: verifica se TODOS os CNPJs do CSV existem em cad_cia_aberta.
             Se houver divergências → aborta e lista os CNPJs ausentes.
  4. Faz o JOIN cnpj → id_cad_cia_aberta (em memória, sem query por linha)
  5. Chave de unicidade: cnpj_cia + dt_refer (ON CONFLICT → UPDATE)
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

CSV_PATH      = r"D:\DATACVM\Formulário de Demonstrações Financeiras Padronizadas (DFP)\dfp_cia_aberta_2025\dfp_cia_aberta_composicao_capital_2025.csv"
CSV_SEPARATOR = ";"
CSV_ENCODING  = "latin-1"
BATCH_SIZE    = 500

# ─── ANO DE REFERÊNCIA ────────────────────────────────────────────────────────
# Altere este valor ao carregar cada ano (ex: 2024, 2025, 2026)
DFP_ANO = 2025

# ─── MAPEAMENTO DE COLUNAS CSV → tabela ──────────────────────────────────────
COLUMN_MAP = {
    "CNPJ_CIA"                 : "cnpj_cia",
    "DT_REFER"                 : "dt_refer",
    "VERSAO"                   : "versao",
    "QT_ACAO_ORDIN_CAP_INTEGR" : "qt_acao_ordin_cap_integr",
    "QT_ACAO_PREF_CAP_INTEGR"  : "qt_acao_pref_cap_integr",
    "QT_ACAO_TOTAL_CAP_INTEGR" : "qt_acao_total_cap_integr",
    "QT_ACAO_ORDIN_TESOURO"    : "qt_acao_ordin_tesouro",
    "QT_ACAO_PREF_TESOURO"     : "qt_acao_pref_tesouro",
    "QT_ACAO_TOTAL_TESOURO"    : "qt_acao_total_tesouro",
    # DENOM_CIA é ignorado intencionalmente
}

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("load_dfp_composicao_capital.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# ─── FUNÇÕES ──────────────────────────────────────────────────────────────────

def read_csv() -> pd.DataFrame:
    """Lê o CSV, ignora DENOM_CIA, renomeia colunas e converte tipos."""
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

    # Verifica colunas obrigatórias
    missing = [c for c in COLUMN_MAP.keys() if c not in df.columns]
    if missing:
        log.error(f"Colunas ausentes no CSV: {missing}")
        log.error(f"Colunas disponíveis: {list(df.columns)}")
        sys.exit(1)

    # Seleciona apenas as colunas necessárias (descarta DENOM_CIA e outras)
    df = df[list(COLUMN_MAP.keys())].rename(columns=COLUMN_MAP)

    # ── Conversão de tipos ───────────────────────────────────────────────────
    df["dt_refer"] = pd.to_datetime(df["dt_refer"], errors="coerce").dt.date
    df["versao"]   = pd.to_numeric(df["versao"], errors="coerce").astype("Int16")

    bigint_cols = [
        "qt_acao_ordin_cap_integr", "qt_acao_pref_cap_integr", "qt_acao_total_cap_integr",
        "qt_acao_ordin_tesouro",    "qt_acao_pref_tesouro",    "qt_acao_total_tesouro",
    ]
    for col in bigint_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    df["cnpj_cia"] = df["cnpj_cia"].str.strip()

    # Remove linhas sem cnpj ou dt_refer
    before = len(df)
    df = df.dropna(subset=["cnpj_cia", "dt_refer"])
    dropped = before - len(df)
    if dropped:
        log.warning(f"  {dropped} linhas removidas por ausência de cnpj_cia ou dt_refer.")

    # ── Desduplicação: mesmo CNPJ + DT_REFER com versões diferentes ──────────
    # O CSV da CVM pode conter múltiplas versões do mesmo documento.
    # Mantém apenas a linha com a VERSAO mais alta para cada cnpj_cia + dt_refer.
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
            f"  {dupes} linha(s) removidas por duplicidade cnpj_cia + dt_refer "
            f"(mantida a versão mais recente de cada)."
        )

    # Adiciona dfp_ano
    df["dfp_ano"] = DFP_ANO

    log.info(f"  {len(df):,} linhas válidas após limpeza e desduplicação.")
    return df


def lookup_cnpjs(conn, df: pd.DataFrame) -> dict[str, int]:
    """
    Verifica se TODOS os CNPJs do CSV existem em cad_cia_aberta.
    Retorna dict {cnpj: id_cad_cia_aberta} se OK.
    Aborta com lista de divergências se houver CNPJs ausentes.
    """
    cnpjs_csv = set(df["cnpj_cia"].dropna().unique())
    log.info(f"  Lookup: {len(cnpjs_csv):,} CNPJs únicos no CSV.")

    with conn.cursor() as cur:
        cur.execute("""
            SELECT cnpj_companhia, id_cad_cia_aberta
            FROM cvm_data.cad_cia_aberta
        """)
        rows = cur.fetchall()

    cad_map = {row[0]: row[1] for row in rows}
    cnpjs_cad = set(cad_map.keys())

    ausentes = cnpjs_csv - cnpjs_cad

    if ausentes:
        log.error("=" * 70)
        log.error("ABORTANDO — Os seguintes CNPJs do CSV não estão em cad_cia_aberta:")
        log.error("=" * 70)
        for cnpj in sorted(ausentes):
            log.error(f"  ✗ {cnpj}")
        log.error("=" * 70)
        log.error(
            f"Total: {len(ausentes)} CNPJ(s) ausente(s). "
            "Cadastre-os em cad_cia_aberta antes de reprocessar."
        )
        conn.close()
        sys.exit(1)

    log.info(f"  Lookup OK — todos os {len(cnpjs_csv):,} CNPJs encontrados em cad_cia_aberta.")
    return cad_map


def year_already_loaded(conn) -> bool:
    """Verifica se o dfp_ano já foi carregado para evitar duplicação."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(1) FROM cvm_data.dfp_composicao_capital WHERE dfp_ano = %s",
            (DFP_ANO,)
        )
        count = cur.fetchone()[0]

    if count > 0:
        log.warning(
            f"dfp_ano={DFP_ANO} já possui {count:,} registros em dfp_composicao_capital — "
            f"abortando para evitar duplicação.\n"
            f"Para reprocessar: DELETE FROM cvm_data.dfp_composicao_capital WHERE dfp_ano = {DFP_ANO};"
        )
        return True
    return False


def build_upsert_query() -> str:
    """
    ON CONFLICT na chave (cnpj_cia, dt_refer):
    Se já existir → atualiza todos os campos (ex: reprocessamento de versão mais recente).
    """
    return """
        INSERT INTO cvm_data.dfp_composicao_capital (
            id_cad_cia_aberta,
            cnpj_cia,
            dt_refer,
            versao,
            dfp_ano,
            qt_acao_ordin_cap_integr,
            qt_acao_pref_cap_integr,
            qt_acao_total_cap_integr,
            qt_acao_ordin_tesouro,
            qt_acao_pref_tesouro,
            qt_acao_total_tesouro
        ) VALUES %s
        ON CONFLICT ON CONSTRAINT uq_dfp_composicao_capital_cnpj_dt
        DO UPDATE SET
            id_cad_cia_aberta        = EXCLUDED.id_cad_cia_aberta,
            versao                   = EXCLUDED.versao,
            dfp_ano                  = EXCLUDED.dfp_ano,
            qt_acao_ordin_cap_integr = EXCLUDED.qt_acao_ordin_cap_integr,
            qt_acao_pref_cap_integr  = EXCLUDED.qt_acao_pref_cap_integr,
            qt_acao_total_cap_integr = EXCLUDED.qt_acao_total_cap_integr,
            qt_acao_ordin_tesouro    = EXCLUDED.qt_acao_ordin_tesouro,
            qt_acao_pref_tesouro     = EXCLUDED.qt_acao_pref_tesouro,
            qt_acao_total_tesouro    = EXCLUDED.qt_acao_total_tesouro,
            dt_ultima_atualizacao    = NOW();
    """


def insert_batches(conn, df: pd.DataFrame, cad_map: dict) -> None:
    """Insere em lotes com UPSERT."""

    cols_ordered = [
        "id_cad_cia_aberta",
        "cnpj_cia", "dt_refer", "versao", "dfp_ano",
        "qt_acao_ordin_cap_integr", "qt_acao_pref_cap_integr", "qt_acao_total_cap_integr",
        "qt_acao_ordin_tesouro",    "qt_acao_pref_tesouro",    "qt_acao_total_tesouro",
    ]

    # Adiciona id_cad_cia_aberta via lookup em memória
    df = df.copy()
    df["id_cad_cia_aberta"] = df["cnpj_cia"].map(cad_map)

    query    = build_upsert_query()
    total    = len(df)
    inserted = 0

    with conn.cursor() as cur:
        for start in range(0, total, BATCH_SIZE):
            batch = df.iloc[start : start + BATCH_SIZE]
            records = [
                tuple(
                    None if (v is None or v is pd.NaT or (isinstance(v, float) and pd.isna(v))) else int(v) if hasattr(v, "item") else v
                    for v in row
                )
                for row in batch[cols_ordered].itertuples(index=False, name=None)
            ]
            execute_values(cur, query, records)
            conn.commit()
            inserted += len(records)
            log.info(f"  Progresso: {inserted:,} / {total:,} linhas processadas.")

    log.info(f"✔ Carga concluída: {inserted:,} linhas inseridas/atualizadas.")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 70)
    log.info(f"Início da carga: dfp_composicao_capital | dfp_ano={DFP_ANO}")
    log.info("=" * 70)

    # 1. Lê e limpa o CSV
    df = read_csv()

    # 2. Conecta ao banco
    log.info(f"Conectando ao PostgreSQL...")
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
    log.info("Carga dfp_composicao_capital finalizada com sucesso.")
    log.info("=" * 70)


if __name__ == "__main__":
    main()