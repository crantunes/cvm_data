"""
Script : load_fre_administrador.py
Tabela : cvm_data.fre_administrador
Fonte  : fre_cia_aberta_administrador_membro_conselho_fiscal_<ANO>.csv
Path   : D:\\DATACVM\\Formulário de Referência (FRE)\\fre_cia_aberta_<ANO>\\

Variáveis do paper:
  CEO Age        → data_nascimento
  CEO Tenure     → data_posse
  CEO Experience → data_inicio_primeiro_mandato + experiencia_profissional
  IPO Experience → experiencia_profissional (NLP posterior)

Estratégia:
  • Loop sobre ANOS_CARGA. Ajuste a lista conforme necessário.
  • Proteção por ano: pula ano se já tiver dados (fre_ano = ANO).
    Para reprocessar um ano: DELETE FROM cvm_data.fre_administrador WHERE fre_ano = XXXX;
  • Inserção raw sem deduplicação — filtro de versão mais recente via query:
      SELECT DISTINCT ON (cnpj_companhia, data_referencia, cpf, cargo_eletivo_ocupado)
             *
      FROM cvm_data.fre_administrador
      ORDER BY cnpj_companhia, data_referencia, cpf, cargo_eletivo_ocupado, versao DESC;
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

# Ajuste os anos conforme necessário
ANOS_CARGA    = list(range(2010, 2027))   # 2010 a 2026
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
        logging.FileHandler("load_fre_administrador.log", encoding="utf-8"),
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
    "Orgao_Administracao"               : "orgao_administracao",
    "Nome"                              : "nome",
    "CPF"                               : "cpf",
    "Profissao"                         : "profissao",
    "Cargo_Eletivo_Ocupado"             : "cargo_eletivo_ocupado",
    "Complemento_Cargo_Eletivo_Ocupado" : "complemento_cargo_eletivo_ocupado",
    "Data_Eleicao"                      : "data_eleicao",
    "Data_Posse"                        : "data_posse",
    "Data_Inicio_Primeiro_Mandato"      : "data_inicio_primeiro_mandato",
    "Prazo_Mandato"                     : "prazo_mandato",
    "Eleito_Controlador"                : "eleito_controlador",
    "Numero_Mandatos_Consecutivos"      : "numero_mandatos_consecutivos",
    "Percentual_Participacao_Reunioes"  : "percentual_participacao_reunioes",
    "Outro_Cargo_Funcao"                : "outro_cargo_funcao",
    "Experiencia_Profissional"          : "experiencia_profissional",
    "Data_Nascimento"                   : "data_nascimento",
}

DATE_COLS = [
    "Data_Referencia", "Data_Eleicao", "Data_Posse",
    "Data_Inicio_Primeiro_Mandato", "Data_Nascimento",
]
NUMERIC_COLS = [
    "Versao", "ID_Documento",
    "Numero_Mandatos_Consecutivos", "Percentual_Participacao_Reunioes",
]

# Colunas INSERT na ordem da tabela (sem id_ SERIAL, sem fre_ano que é injetado)
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
            "SELECT COUNT(1) FROM cvm_data.fre_administrador WHERE fre_ano = %s", (ano,)
        )
        count = cur.fetchone()[0]
    if count > 0:
        log.warning(f"  fre_ano={ano} já possui {count:,} registros — pulando.")
        log.warning(f"  Para reprocessar: DELETE FROM cvm_data.fre_administrador WHERE fre_ano = {ano};")
        return True
    return False


def csv_path(ano: int) -> Path:
    return CSV_BASE_PATH / f"fre_cia_aberta_{ano}" / f"fre_cia_aberta_administrador_membro_conselho_fiscal_{ano}.csv"


def read_csv(path: Path, ano: int) -> pd.DataFrame:
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

    # Adiciona colunas ausentes
    for csv_col in COL_MAP.keys():
        if csv_col not in df.columns:
            df[csv_col] = None

    # Injeta fre_ano
    df["_fre_ano"] = ano

    nao_mapeadas = [c for c in df.columns if c not in COL_MAP and c != "_fre_ano"]
    if nao_mapeadas:
        log.info(f"    Colunas não mapeadas (ignoradas): {nao_mapeadas}")

    return df


def insert_ano(conn, df: pd.DataFrame, ano: int) -> int:
    df_ins = df[list(COL_MAP.keys())].rename(columns=COL_MAP)
    df_ins.insert(0, "fre_ano", ano)

    all_cols  = ["fre_ano"] + INSERT_COLS
    cols_str  = ", ".join(all_cols)
    query     = f"INSERT INTO cvm_data.fre_administrador ({cols_str}) VALUES %s"

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
    log.info("Início da carga: fre_administrador")
    log.info(f"Anos a processar: {ANOS_CARGA}")
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
                log.warning(f"  Arquivo não encontrado — pulando.")
                continue

            if ano_ja_carregado(conn, ano):
                continue

            df    = read_csv(path, ano)
            n     = insert_ano(conn, df, ano)
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
    log.info(f"✔ fre_administrador: {total_geral:,} registros inseridos no total.")
    log.info("=" * 70)


if __name__ == "__main__":
    main()