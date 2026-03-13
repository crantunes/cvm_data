"""
Script : load_dfp_financeira.py
Tabela : cvm_data.dfp_financeira
Fonte  : 16 CSVs por ano — BPA, BPP, DRE, DFC-MD, DFC-MI, DRA, DVA, DMPL (con + ind)

Fluxo:
  1. Lookup único de CNPJs (todos os 16 CSVs) contra cad_cia_aberta — aborta
     se houver divergências, listando os CNPJs ausentes
  2. Para cada CSV do ano:
     a. Lê e limpa o arquivo
     b. Aplica slug de grupo_dfp via dicionário (sem tabela auxiliar)
     c. Preenche campos ausentes com NULL (dt_ini_exerc para BPA/BPP;
        coluna_df para todos exceto DMPL_con)
     d. UPSERT em lotes com ON CONFLICT na chave composta
  3. year_already_loaded: aborta se o dfp_ano já tiver dados na tabela

Decisão grupo_dfp:
  Mapeamento via dicionário Python — 16 valores fixos definidos pela CVM,
  sem tabela auxiliar (evita JOIN em queries analíticas sem ganho real).

Dependências:
    pip install pandas psycopg2-binary python-dotenv

Arquivo .env (mesma pasta do script):
    DB_USER=postgres
    DB_PASSWORD=...
    DB_HOST=localhost
    DB_PORT=5432
    DB_NAME=cvm_data

Para carregar outro ano: altere DFP_ANO e execute novamente.
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

CSV_SEPARATOR = ";"
CSV_ENCODING  = "latin-1"
BATCH_SIZE    = 1000   # dfp_financeira é grande — lotes maiores para performance

# ─── ANO DE REFERÊNCIA ────────────────────────────────────────────────────────
# Altere este valor manualmente ao rodar cada ano (2010 a 2025)
DFP_ANO = 2025

# ─── BASE PATH DOS CSVs ───────────────────────────────────────────────────────
BASE_PATH = (
    r"D:\DATACVM\Formulário de Demonstrações Financeiras Padronizadas (DFP)"
    rf"\dfp_cia_aberta_{DFP_ANO}"
)

# ─── MAPEAMENTO: nome do arquivo → slug grupo_dfp ────────────────────────────
# Chave: sufixo do arquivo (sem ano e sem .csv)
# Valor: slug que será gravado na coluna grupo_dfp
GRUPO_DFP_MAP = {
    "BPA_con" : "BPA_CON",
    "BPA_ind" : "BPA_IND",
    "BPP_con" : "BPP_CON",
    "BPP_ind" : "BPP_IND",
    "DFC_MD_con" : "DFCD_CON",
    "DFC_MD_ind" : "DFCD_IND",
    "DFC_MI_con" : "DFCI_CON",
    "DFC_MI_ind" : "DFCI_IND",
    "DMPL_con" : "DMPL_CON",
    "DMPL_ind" : "DMPL_IND",
    "DRA_con" : "DRA_CON",
    "DRA_ind" : "DRA_IND",
    "DRE_con" : "DRE_CON",
    "DRE_ind" : "DRE_IND",
    "DVA_con" : "DVA_CON",
    "DVA_ind" : "DVA_IND",
}

# ─── MAPEAMENTO: slug → configuração de campos opcionais ─────────────────────
# tem_dt_ini  : True se o CSV possui DT_INI_EXERC
# tem_coluna_df: True se o CSV possui COLUNA_DF (apenas DMPL_con)
CSV_CONFIG = {
    "BPA_CON"  : {"tem_dt_ini": False, "tem_coluna_df": False},
    "BPA_IND"  : {"tem_dt_ini": False, "tem_coluna_df": False},
    "BPP_CON"  : {"tem_dt_ini": False, "tem_coluna_df": False},
    "BPP_IND"  : {"tem_dt_ini": False, "tem_coluna_df": False},
    "DFCD_CON" : {"tem_dt_ini": True,  "tem_coluna_df": False},
    "DFCD_IND" : {"tem_dt_ini": True,  "tem_coluna_df": False},
    "DFCI_CON" : {"tem_dt_ini": True,  "tem_coluna_df": False},
    "DFCI_IND" : {"tem_dt_ini": True,  "tem_coluna_df": False},
    "DMPL_CON" : {"tem_dt_ini": True,  "tem_coluna_df": True },  # único com COLUNA_DF
    "DMPL_IND" : {"tem_dt_ini": True,  "tem_coluna_df": False},
    "DRA_CON"  : {"tem_dt_ini": True,  "tem_coluna_df": False},
    "DRA_IND"  : {"tem_dt_ini": True,  "tem_coluna_df": False},
    "DRE_CON"  : {"tem_dt_ini": True,  "tem_coluna_df": False},
    "DRE_IND"  : {"tem_dt_ini": True,  "tem_coluna_df": False},
    "DVA_CON"  : {"tem_dt_ini": True,  "tem_coluna_df": False},
    "DVA_IND"  : {"tem_dt_ini": True,  "tem_coluna_df": False},
}

# ─── MAPEAMENTO: slug do grupo → valor longo original da CVM ─────────────────
# Usado para substituir GRUPO_DFP do CSV pelo slug padronizado
GRUPO_DFP_CSV_TO_SLUG = {
    "DF Consolidado - Balanço Patrimonial Ativo"                                              : "BPA_CON",
    "DF Individual - Balanço Patrimonial Ativo"                                               : "BPA_IND",
    "DF Consolidado - Balanço Patrimonial Passivo"                                            : "BPP_CON",
    "DF Individual - Balanço Patrimonial Passivo"                                             : "BPP_IND",
    "DF Consolidado - Demonstração do Fluxo de Caixa (Método Direto)"                        : "DFCD_CON",
    "DF Individual - Demonstração do Fluxo de Caixa (Método Direto)"                         : "DFCD_IND",
    "DF Consolidado - Demonstração do Fluxo de Caixa (Método Indireto)"                      : "DFCI_CON",
    "DF Individual - Demonstração do Fluxo de Caixa (Método Indireto)"                       : "DFCI_IND",
    "DF Consolidado - Demonstração das Mutações do Patrimônio Líquido"                       : "DMPL_CON",
    "DF Individual - Demonstração das Mutações do Patrimônio Líquido"                        : "DMPL_IND",
    "DF Consolidado - Demonstração de Resultado Abrangente"                                   : "DRA_CON",
    "DF Individual - Demonstração de Resultado Abrangente"                                    : "DRA_IND",
    "DF Consolidado - Demonstração do Resultado"                                              : "DRE_CON",
    "DF Individual - Demonstração do Resultado"                                               : "DRE_IND",
    "DF Consolidado - Demonstração de Valor Adicionado"                                       : "DVA_CON",
    "DF Individual - Demonstração de Valor Adicionado"                                        : "DVA_IND",
}

# Colunas para INSERT (sem id_dfp_financeira que é SERIAL)
INSERT_COLS = [
    "id_cad_cia_aberta",
    "cnpj_cia",
    "dt_refer",
    "versao",
    "dfp_ano",
    "cd_cvm",
    "grupo_dfp",
    "moeda",
    "escala_moeda",
    "ordem_exerc",
    "dt_ini_exerc",
    "dt_fim_exerc",
    "coluna_df",
    "cd_conta",
    "ds_conta",
    "vl_conta",
    "st_conta_fixa",
]

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f"load_dfp_financeira_{DFP_ANO}.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# ─── FUNÇÕES ──────────────────────────────────────────────────────────────────

def get_csv_path(sufixo: str) -> Path:
    """Monta o path completo do CSV a partir do sufixo e do DFP_ANO."""
    return Path(BASE_PATH) / f"dfp_cia_aberta_{sufixo}_{DFP_ANO}.csv"


def year_already_loaded(conn) -> bool:
    """Aborta se o dfp_ano já tiver dados para evitar duplicação."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(1) FROM cvm_data.dfp_financeira WHERE dfp_ano = %s",
            (DFP_ANO,)
        )
        count = cur.fetchone()[0]
    if count > 0:
        log.warning(
            f"dfp_ano={DFP_ANO} já possui {count:,} registros em dfp_financeira — "
            "abortando para evitar duplicação.\n"
            f"Para reprocessar: DELETE FROM cvm_data.dfp_financeira WHERE dfp_ano = {DFP_ANO};"
        )
        return True
    return False


def load_cad_map(conn) -> dict:
    """Carrega {cnpj_companhia: id_cad_cia_aberta} em memória."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT cnpj_companhia, id_cad_cia_aberta FROM cvm_data.cad_cia_aberta"
        )
        return {row[0]: row[1] for row in cur.fetchall()}


def collect_all_cnpjs() -> set:
    """
    Lê apenas a coluna CNPJ_CIA de todos os 16 CSVs para fazer o
    lookup global antes de iniciar qualquer INSERT.
    CSVs ausentes são avisados mas não abortam (ano pode não ter todos).
    """
    todos_cnpjs = set()
    for sufixo in GRUPO_DFP_MAP.keys():
        path = get_csv_path(sufixo)
        if not path.exists():
            log.warning(f"  CSV não encontrado (ignorado): {path.name}")
            continue
        try:
            df = pd.read_csv(
                path, sep=CSV_SEPARATOR, encoding=CSV_ENCODING,
                usecols=["CNPJ_CIA"], dtype=str, low_memory=False
            )
            cnpjs = set(df["CNPJ_CIA"].str.strip().dropna().unique())
            todos_cnpjs.update(cnpjs)
        except Exception as e:
            log.warning(f"  Erro ao ler CNPJs de {path.name}: {e}")
    return todos_cnpjs


def lookup_cnpjs_global(conn, cad_map: dict) -> None:
    """
    Verifica se TODOS os CNPJs de todos os CSVs existem em cad_cia_aberta.
    Aborta com lista de divergências se houver ausências.
    """
    log.info("Coletando CNPJs de todos os CSVs para lookup global...")
    cnpjs_csv = collect_all_cnpjs()
    log.info(f"  {len(cnpjs_csv):,} CNPJs únicos coletados nos CSVs.")

    ausentes = sorted(cnpjs_csv - set(cad_map.keys()))
    if ausentes:
        log.error("=" * 70)
        log.error("ABORTANDO — CNPJs dos CSVs ausentes em cad_cia_aberta:")
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

    log.info(f"  Lookup global OK — todos os {len(cnpjs_csv):,} CNPJs encontrados.")


def read_and_clean_csv(sufixo: str, slug: str, config: dict) -> pd.DataFrame | None:
    """
    Lê um CSV, aplica limpeza, converte tipos e preenche NULLs
    conforme a configuração do tipo de demonstração.
    Retorna None se o arquivo não existir.
    """
    path = get_csv_path(sufixo)
    if not path.exists():
        log.warning(f"  Arquivo não encontrado — pulando: {path.name}")
        return None

    log.info(f"  Lendo: {path.name}")
    df = pd.read_csv(
        path, sep=CSV_SEPARATOR, encoding=CSV_ENCODING,
        dtype=str, low_memory=False
    )
    df.columns = df.columns.str.strip()
    log.info(f"    {len(df):,} linhas lidas.")

    # ── Descarta DENOM_CIA e quaisquer colunas extras ────────────────────────
    colunas_base = [
        "CNPJ_CIA", "DT_REFER", "VERSAO", "CD_CVM", "GRUPO_DFP",
        "MOEDA", "ESCALA_MOEDA", "ORDEM_EXERC", "DT_FIM_EXERC",
        "CD_CONTA", "DS_CONTA", "VL_CONTA", "ST_CONTA_FIXA",
    ]
    colunas_opcionais = []
    if config["tem_dt_ini"]:
        colunas_opcionais.append("DT_INI_EXERC")
    if config["tem_coluna_df"]:
        colunas_opcionais.append("COLUNA_DF")

    colunas_ler = [c for c in colunas_base + colunas_opcionais if c in df.columns]
    df = df[colunas_ler].copy()

    # ── Adiciona colunas ausentes como NULL ──────────────────────────────────
    if "DT_INI_EXERC" not in df.columns:
        df["DT_INI_EXERC"] = None
    if "COLUNA_DF" not in df.columns:
        df["COLUNA_DF"] = None

    # ── Substitui GRUPO_DFP longo pelo slug padronizado ──────────────────────
    if "GRUPO_DFP" in df.columns:
        df["GRUPO_DFP"] = df["GRUPO_DFP"].str.strip().map(GRUPO_DFP_CSV_TO_SLUG)
        nao_mapeados = df["GRUPO_DFP"].isna().sum()
        if nao_mapeados:
            log.warning(
                f"    {nao_mapeados} linha(s) com GRUPO_DFP não mapeado — serão removidas."
            )
            df = df.dropna(subset=["GRUPO_DFP"])
    else:
        # CSV não tem GRUPO_DFP — usa o slug do arquivo diretamente
        df["GRUPO_DFP"] = slug

    # ── Conversão de tipos ───────────────────────────────────────────────────
    df["CNPJ_CIA"]     = df["CNPJ_CIA"].str.strip()
    df["CD_CVM"]       = df["CD_CVM"].str.strip().replace("", None) if "CD_CVM" in df.columns else None
    df["ORDEM_EXERC"]  = df["ORDEM_EXERC"].str.strip() if "ORDEM_EXERC" in df.columns else None
    df["ST_CONTA_FIXA"]= df["ST_CONTA_FIXA"].str.strip() if "ST_CONTA_FIXA" in df.columns else None
    df["COLUNA_DF"]    = df["COLUNA_DF"].str.strip() if df["COLUNA_DF"].notna().any() else None

    df["DT_REFER"]     = pd.to_datetime(df["DT_REFER"],     errors="coerce").dt.date
    df["DT_FIM_EXERC"] = pd.to_datetime(df["DT_FIM_EXERC"], errors="coerce").dt.date
    df["DT_INI_EXERC"] = pd.to_datetime(df["DT_INI_EXERC"], errors="coerce").dt.date

    df["VERSAO"]   = pd.to_numeric(df["VERSAO"],   errors="coerce").astype("Int16")
    df["VL_CONTA"] = pd.to_numeric(df["VL_CONTA"], errors="coerce")

    # ── Remove linhas sem chave mínima ───────────────────────────────────────
    before = len(df)
    df = df.dropna(subset=["CNPJ_CIA", "DT_REFER", "VERSAO", "CD_CONTA"])
    dropped = before - len(df)
    if dropped:
        log.warning(f"    {dropped} linha(s) removidas por campos chave nulos.")

    # ── Adiciona dfp_ano ─────────────────────────────────────────────────────
    df["DFP_ANO"] = DFP_ANO

    # ── Desduplicação na chave UNIQUE da tabela ──────────────────────────
    # CSVs podem conter linhas repetidas ou múltiplas versões do mesmo registro.
    # ON CONFLICT DO UPDATE falha se a mesma chave aparece 2x no mesmo batch.
    # Espelha a CONSTRAINT uq_dfp_financeira da tabela.
    dedup_key = ["CNPJ_CIA", "DT_REFER", "VERSAO", "GRUPO_DFP",
                 "ORDEM_EXERC", "CD_CONTA", "COLUNA_DF"]
    before_dedup = len(df)
    df = (
        df.sort_values("VERSAO", ascending=False)
          .drop_duplicates(subset=dedup_key, keep="first")
          .reset_index(drop=True)
    )
    dupes = before_dedup - len(df)
    if dupes:
        log.warning(
            f"    {dupes} linha(s) removidas por duplicidade na chave única "
            f"(mantida a VERSAO mais alta de cada combinação)."
        )

    log.info(f"    {len(df):,} linhas válidas após limpeza e desduplicação.")
    return df


def build_upsert_query() -> str:
    cols_str   = ", ".join(INSERT_COLS)
    update_set = ",\n            ".join(
        f"{c} = EXCLUDED.{c}"
        for c in INSERT_COLS
        if c not in ("cnpj_cia", "dt_refer", "versao", "grupo_dfp",
                     "ordem_exerc", "cd_conta", "coluna_df")
    ) + ",\n            dt_ultima_atualizacao = NOW()"

    return f"""
        INSERT INTO cvm_data.dfp_financeira ({cols_str})
        VALUES %s
        ON CONFLICT ON CONSTRAINT uq_dfp_financeira
        DO UPDATE SET
            {update_set};
    """


def insert_batches(conn, df: pd.DataFrame, cad_map: dict, nome_csv: str) -> int:
    """Insere registros de um CSV em lotes. Retorna total inserido/atualizado."""

    # Mapeia id_cad_cia_aberta em memória
    df = df.copy()
    df["id_cad_cia_aberta"] = df["CNPJ_CIA"].map(cad_map)

    # Renomeia para snake_case conforme INSERT_COLS
    col_rename = {
        "CNPJ_CIA": "cnpj_cia", "DT_REFER": "dt_refer", "VERSAO": "versao",
        "DFP_ANO": "dfp_ano", "CD_CVM": "cd_cvm", "GRUPO_DFP": "grupo_dfp",
        "MOEDA": "moeda", "ESCALA_MOEDA": "escala_moeda", "ORDEM_EXERC": "ordem_exerc",
        "DT_INI_EXERC": "dt_ini_exerc", "DT_FIM_EXERC": "dt_fim_exerc",
        "COLUNA_DF": "coluna_df", "CD_CONTA": "cd_conta", "DS_CONTA": "ds_conta",
        "VL_CONTA": "vl_conta", "ST_CONTA_FIXA": "st_conta_fixa",
    }
    df = df.rename(columns=col_rename)

    query    = build_upsert_query()
    total    = len(df)
    inserted = 0

    with conn.cursor() as cur:
        for start in range(0, total, BATCH_SIZE):
            batch = df.iloc[start : start + BATCH_SIZE]
            records = [
                tuple(
                    None if (
                        v is None
                        or v is pd.NaT
                        or (isinstance(v, float) and pd.isna(v))
                    )
                    else int(v) if hasattr(v, "item") and isinstance(v.item(), int)
                    else v
                    for v in row
                )
                for row in batch[INSERT_COLS].itertuples(index=False, name=None)
            ]
            execute_values(cur, query, records)
            conn.commit()
            inserted += len(records)

    log.info(f"    ✔ {nome_csv}: {inserted:,} registros inseridos/atualizados.")
    return inserted


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 70)
    log.info(f"Início da carga: dfp_financeira | dfp_ano={DFP_ANO}")
    log.info(f"Base path: {BASE_PATH}")
    log.info("=" * 70)

    # 1. Conecta ao banco
    log.info("Conectando ao PostgreSQL...")
    try:
        conn = psycopg2.connect(DB_URL)
    except psycopg2.OperationalError as e:
        log.error(f"Falha na conexão: {e}")
        sys.exit(1)

    try:
        # 2. Proteção contra reprocessamento duplo
        if year_already_loaded(conn):
            sys.exit(0)

        # 3. Carrega mapa de CNPJs em memória
        log.info("Carregando mapa cad_cia_aberta...")
        cad_map = load_cad_map(conn)
        log.info(f"  {len(cad_map):,} empresas carregadas.")

        # 4. Lookup global de CNPJs (todos os CSVs de uma vez) — aborta se divergir
        lookup_cnpjs_global(conn, cad_map)

        # 5. Processa cada CSV
        total_geral = 0
        csvs_processados = 0
        csvs_ausentes    = 0

        for sufixo, slug in GRUPO_DFP_MAP.items():
            config = CSV_CONFIG[slug]
            log.info(f"--- [{slug}] Processando {sufixo}_{DFP_ANO}.csv ---")

            df = read_and_clean_csv(sufixo, slug, config)
            if df is None:
                csvs_ausentes += 1
                continue

            qtd = insert_batches(conn, df, cad_map, f"{sufixo}_{DFP_ANO}")
            total_geral += qtd
            csvs_processados += 1

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
    log.info(f"Carga dfp_financeira concluída — dfp_ano={DFP_ANO}")
    log.info(f"  CSVs processados : {csvs_processados}")
    log.info(f"  CSVs ausentes    : {csvs_ausentes}")
    log.info(f"  Total registros  : {total_geral:,}")
    log.info("=" * 70)


if __name__ == "__main__":
    main()