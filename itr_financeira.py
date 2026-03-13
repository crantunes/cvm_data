"""
Script : load_itr_financeira.py
Tabela : cvm_data.itr_financeira
Fonte  : 16 CSVs por ano — BPA, BPP, DRE, DFC-MD, DFC-MI, DRA, DVA, DMPL (con + ind)
Período: 2011 – 2025

Diferenças em relação ao load_dfp_financeira.py:
  • itr_ano  : ano do arquivo (configurado manualmente em ITR_ANO)
  • itr_trim : derivado de dt_refer no padrão de mercado XTnn
                - mês 01-03 → 1Tnn  (ex: 2022-03-31 → "1T22")
                - mês 04-06 → 2Tnn  (ex: 2022-06-30 → "2T22")
                - mês 07-09 → 3Tnn  (ex: 2022-09-30 → "3T22")
                - mês 10-12 → 4Tnn  (ex: 2022-12-31 → "4T22")
                YY = dois últimos dígitos do ano de dt_refer (não do ITR_ANO,
                pois ITRs de Jan/2012 referenciam trimestres de 2011)
  • Prefixo dos CSVs: "itr_cia_aberta_" em vez de "dfp_cia_aberta_"
  • Base path aponta para pasta ITR
  • Todos os fixes da v2 da DFP já incorporados:
      - NaT → None
      - Deduplicação espelhando a UNIQUE constraint
      - GRUPO_DFP: aceita prefixo "IT " além de "DF " (robustez ITR)

Dependências:
    pip install pandas psycopg2-binary python-dotenv

Arquivo .env (mesma pasta do script):
    DB_USER=postgres
    DB_PASSWORD=...
    DB_HOST=localhost
    DB_PORT=5432
    DB_NAME=cvm_data

Para carregar outro ano: altere ITR_ANO e execute novamente.
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
BATCH_SIZE    = 1000

# ─── ANO DE REFERÊNCIA ────────────────────────────────────────────────────────
# Altere este valor manualmente ao rodar cada ano (2011 a 2025)
ITR_ANO = 2025

# ─── BASE PATH DOS CSVs ───────────────────────────────────────────────────────
BASE_PATH = (
    r"D:\DATACVM\Formulário de Informações Trimestrais (ITR)"
    rf"\itr_cia_aberta_{ITR_ANO}"
)

# ─── MAPEAMENTO: sufixo do arquivo → slug grupo_dfp ──────────────────────────
GRUPO_DFP_MAP = {
    "BPA_con"    : "BPA_CON",
    "BPA_ind"    : "BPA_IND",
    "BPP_con"    : "BPP_CON",
    "BPP_ind"    : "BPP_IND",
    "DFC_MD_con" : "DFCD_CON",
    "DFC_MD_ind" : "DFCD_IND",
    "DFC_MI_con" : "DFCI_CON",
    "DFC_MI_ind" : "DFCI_IND",
    "DMPL_con"   : "DMPL_CON",
    "DMPL_ind"   : "DMPL_IND",
    "DRA_con"    : "DRA_CON",
    "DRA_ind"    : "DRA_IND",
    "DRE_con"    : "DRE_CON",
    "DRE_ind"    : "DRE_IND",
    "DVA_con"    : "DVA_CON",
    "DVA_ind"    : "DVA_IND",
}

# ─── CONFIGURAÇÃO DE CAMPOS OPCIONAIS POR TIPO ───────────────────────────────
# tem_dt_ini   : True se o CSV possui DT_INI_EXERC
# tem_coluna_df: True se o CSV possui COLUNA_DF (apenas DMPL_con, igual à DFP)
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

# ─── MAPEAMENTO: texto longo do CSV → slug ────────────────────────────────────
# ITR usa prefixo "IT " nos CSVs — a DFP usa "DF ".
# Mapeamos ambos os prefixos para máxima robustez (alguns anos podem variar).
GRUPO_CSV_TO_SLUG = {
    # Prefixo "IT " — padrão ITR
    "IT Consolidado - Balanço Patrimonial Ativo"                                              : "BPA_CON",
    "IT Individual - Balanço Patrimonial Ativo"                                               : "BPA_IND",
    "IT Consolidado - Balanço Patrimonial Passivo"                                            : "BPP_CON",
    "IT Individual - Balanço Patrimonial Passivo"                                             : "BPP_IND",
    "IT Consolidado - Demonstração do Fluxo de Caixa (Método Direto)"                        : "DFCD_CON",
    "IT Individual - Demonstração do Fluxo de Caixa (Método Direto)"                         : "DFCD_IND",
    "IT Consolidado - Demonstração do Fluxo de Caixa (Método Indireto)"                      : "DFCI_CON",
    "IT Individual - Demonstração do Fluxo de Caixa (Método Indireto)"                       : "DFCI_IND",
    "IT Consolidado - Demonstração das Mutações do Patrimônio Líquido"                       : "DMPL_CON",
    "IT Individual - Demonstração das Mutações do Patrimônio Líquido"                        : "DMPL_IND",
    "IT Consolidado - Demonstração de Resultado Abrangente"                                   : "DRA_CON",
    "IT Individual - Demonstração de Resultado Abrangente"                                    : "DRA_IND",
    "IT Consolidado - Demonstração do Resultado"                                              : "DRE_CON",
    "IT Individual - Demonstração do Resultado"                                               : "DRE_IND",
    "IT Consolidado - Demonstração de Valor Adicionado"                                       : "DVA_CON",
    "IT Individual - Demonstração de Valor Adicionado"                                        : "DVA_IND",
    # Prefixo "DF " — fallback caso algum ano use o mesmo padrão da DFP
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

# ─── COLUNAS PARA INSERT ──────────────────────────────────────────────────────
INSERT_COLS = [
    "id_cad_cia_aberta",
    "cnpj_cia",
    "dt_refer",
    "versao",
    "itr_ano",
    "itr_trim",
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
        logging.FileHandler(f"load_itr_financeira_{ITR_ANO}.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# ─── FUNÇÕES ──────────────────────────────────────────────────────────────────

def get_csv_path(sufixo: str) -> Path:
    """Monta o path completo do CSV ITR a partir do sufixo e do ITR_ANO."""
    return Path(BASE_PATH) / f"itr_cia_aberta_{sufixo}_{ITR_ANO}.csv"


def derivar_itr_trim(dt_refer_series: pd.Series) -> pd.Series:
    """
    Deriva o trimestre no padrão de mercado XTnn a partir de dt_refer.

    Lógica:
      Mês 01-03 → trimestre 1
      Mês 04-06 → trimestre 2
      Mês 07-09 → trimestre 3
      Mês 10-12 → trimestre 4

    YY = dois últimos dígitos do ano de dt_refer (não de ITR_ANO,
    pois ITRs entregues em janeiro referenciam o tri anterior).

    Exemplos:
      2022-03-31 → "1T22"
      2022-06-30 → "2T22"
      2022-09-30 → "3T22"
      2022-12-31 → "4T22"
    """
    dt = pd.to_datetime(dt_refer_series, errors="coerce")
    mes = dt.dt.month
    ano_yy = dt.dt.year % 100   # dois últimos dígitos do ano

    trimestre = pd.cut(
        mes,
        bins=[0, 3, 6, 9, 12],
        labels=["1", "2", "3", "4"],
        right=True
    ).astype(str)

    # Formata: "{trimestre}T{ano_yy:02d}"
    itr_trim = trimestre + "T" + ano_yy.apply(lambda y: f"{int(y):02d}" if pd.notna(y) else "")

    return itr_trim


def year_already_loaded(conn) -> bool:
    """Aborta se o itr_ano já tiver dados para evitar duplicação."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(1) FROM cvm_data.itr_financeira WHERE itr_ano = %s",
            (ITR_ANO,)
        )
        count = cur.fetchone()[0]
    if count > 0:
        log.warning(
            f"itr_ano={ITR_ANO} já possui {count:,} registros em itr_financeira — "
            "abortando para evitar duplicação.\n"
            f"Para reprocessar: DELETE FROM cvm_data.itr_financeira WHERE itr_ano = {ITR_ANO};"
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
    Lê apenas CNPJ_CIA de todos os 16 CSVs para o lookup global.
    CSVs ausentes são avisados mas não abortam.
    """
    todos_cnpjs = set()
    for sufixo in GRUPO_DFP_MAP.keys():
        path = get_csv_path(sufixo)
        if not path.exists():
            log.warning(f"  CSV não encontrado (ignorado no lookup): {path.name}")
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
    Aborta com lista completa de divergências se houver ausências.
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
    Lê um CSV ITR, aplica limpeza, converte tipos, deriva itr_trim e
    deduplica na chave UNIQUE antes do INSERT.
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

    # ── Seleciona colunas necessárias — descarta DENOM_CIA e extras ──────────
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

    # ── Mapeia GRUPO_DFP: texto longo CVM → slug (aceita "IT " e "DF ") ─────
    if "GRUPO_DFP" in df.columns:
        df["GRUPO_DFP"] = df["GRUPO_DFP"].str.strip().map(GRUPO_CSV_TO_SLUG)
        nao_mapeados = df["GRUPO_DFP"].isna().sum()
        if nao_mapeados:
            # Loga os valores únicos não mapeados para diagnóstico
            valores_raw = df.loc[df["GRUPO_DFP"].isna(), "GRUPO_DFP"].unique()
            log.warning(
                f"    {nao_mapeados} linha(s) com GRUPO_DFP não mapeado — removidas. "
                f"Valores: {list(valores_raw[:5])}"
            )
            df = df.dropna(subset=["GRUPO_DFP"])
    else:
        df["GRUPO_DFP"] = slug

    # ── Conversão de tipos ───────────────────────────────────────────────────
    df["CNPJ_CIA"]      = df["CNPJ_CIA"].str.strip()
    df["CD_CVM"]        = df["CD_CVM"].str.strip().replace("", None) if "CD_CVM" in df.columns else None
    df["ORDEM_EXERC"]   = df["ORDEM_EXERC"].str.strip() if "ORDEM_EXERC" in df.columns else None
    df["ST_CONTA_FIXA"] = df["ST_CONTA_FIXA"].str.strip() if "ST_CONTA_FIXA" in df.columns else None
    df["COLUNA_DF"]     = df["COLUNA_DF"].str.strip() if df["COLUNA_DF"].notna().any() else None

    # ── Diagnóstico de comprimento: detecta campos TEXT que excedem 100 chars ─
    # ds_conta, escala_moeda e moeda são TEXT no banco — sem limite fixo.
    # Este bloco loga os casos extremos para rastreabilidade mas NÃO remove linhas.
    campos_texto = {
        "DS_CONTA": "ds_conta", "ESCALA_MOEDA": "escala_moeda", "MOEDA": "moeda",
        "COLUNA_DF": "coluna_df",
    }
    for col_csv, col_db in campos_texto.items():
        if col_csv in df.columns and df[col_csv].notna().any():
            comprimentos = df[col_csv].dropna().str.len()
            max_len = int(comprimentos.max())
            if max_len > 100:
                n_longa = int((comprimentos > 100).sum())
                exemplo = df.loc[comprimentos.idxmax(), col_csv][:120]
                log.warning(
                    f"    ⚠ {col_csv}: {n_longa} valor(es) com >{100} chars "
                    f"(máx={max_len}). Exemplo: '{exemplo}...'"
                )

    df["DT_REFER"]      = pd.to_datetime(df["DT_REFER"],     errors="coerce").dt.date
    df["DT_FIM_EXERC"]  = pd.to_datetime(df["DT_FIM_EXERC"], errors="coerce").dt.date
    df["DT_INI_EXERC"]  = pd.to_datetime(df["DT_INI_EXERC"], errors="coerce").dt.date

    df["VERSAO"]    = pd.to_numeric(df["VERSAO"],   errors="coerce").astype("Int16")
    df["VL_CONTA"]  = pd.to_numeric(df["VL_CONTA"], errors="coerce")

    # ── Remove linhas sem chave mínima ───────────────────────────────────────
    before = len(df)
    df = df.dropna(subset=["CNPJ_CIA", "DT_REFER", "VERSAO", "CD_CONTA"])
    dropped = before - len(df)
    if dropped:
        log.warning(f"    {dropped} linha(s) removidas por campos chave nulos.")

    # ── Deriva itr_trim a partir de DT_REFER ─────────────────────────────────
    df["ITR_TRIM"] = derivar_itr_trim(pd.Series([str(d) for d in df["DT_REFER"]]))

    # Valida: trimestres que não geraram código válido
    invalidos = df["ITR_TRIM"].str.match(r'^[1-4]T\d{2}$') == False
    if invalidos.any():
        log.warning(f"    {invalidos.sum()} linha(s) com ITR_TRIM inválido — removidas.")
        df = df[~invalidos]

    # ── Adiciona itr_ano ─────────────────────────────────────────────────────
    df["ITR_ANO"] = ITR_ANO

    # ── Desduplicação espelhando CONSTRAINT uq_itr_financeira ────────────────
    # CSVs ITR contêm exercícios anteriores para geração dos PDFs comparativos.
    # A dedup mantém apenas a linha com VERSAO mais alta por chave única.
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

    # ── Log de distribuição de trimestres para auditoria ────────────────────
    dist = df["ITR_TRIM"].value_counts().sort_index()
    log.info(f"    Distribuição ITR_TRIM: {dict(dist)}")

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
        INSERT INTO cvm_data.itr_financeira ({cols_str})
        VALUES %s
        ON CONFLICT ON CONSTRAINT uq_itr_financeira
        DO UPDATE SET
            {update_set};
    """


def insert_batches(conn, df: pd.DataFrame, cad_map: dict, nome_csv: str) -> int:
    """Insere registros de um CSV ITR em lotes. Retorna total inserido/atualizado."""

    df = df.copy()
    df["id_cad_cia_aberta"] = df["CNPJ_CIA"].map(cad_map)

    col_rename = {
        "CNPJ_CIA": "cnpj_cia", "DT_REFER": "dt_refer", "VERSAO": "versao",
        "ITR_ANO": "itr_ano", "ITR_TRIM": "itr_trim",
        "CD_CVM": "cd_cvm", "GRUPO_DFP": "grupo_dfp",
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
    log.info(f"Início da carga: itr_financeira | itr_ano={ITR_ANO}")
    log.info(f"Base path: {BASE_PATH}")
    log.info("=" * 70)

    log.info("Conectando ao PostgreSQL...")
    try:
        conn = psycopg2.connect(DB_URL)
    except psycopg2.OperationalError as e:
        log.error(f"Falha na conexão: {e}")
        sys.exit(1)

    try:
        # 1. Proteção contra reprocessamento duplo
        if year_already_loaded(conn):
            sys.exit(0)

        # 2. Carrega mapa de CNPJs em memória
        log.info("Carregando mapa cad_cia_aberta...")
        cad_map = load_cad_map(conn)
        log.info(f"  {len(cad_map):,} empresas carregadas.")

        # 3. Lookup global de CNPJs — aborta se houver divergências
        lookup_cnpjs_global(conn, cad_map)

        # 4. Processa cada um dos 16 CSVs
        total_geral      = 0
        csvs_processados = 0
        csvs_ausentes    = 0

        for sufixo, slug in GRUPO_DFP_MAP.items():
            config = CSV_CONFIG[slug]
            log.info(f"--- [{slug}] Processando itr_cia_aberta_{sufixo}_{ITR_ANO}.csv ---")

            df = read_and_clean_csv(sufixo, slug, config)
            if df is None:
                csvs_ausentes += 1
                continue

            qtd = insert_batches(conn, df, cad_map, f"itr_{sufixo}_{ITR_ANO}")
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
    log.info(f"Carga itr_financeira concluída — itr_ano={ITR_ANO}")
    log.info(f"  CSVs processados : {csvs_processados}")
    log.info(f"  CSVs ausentes    : {csvs_ausentes}")
    log.info(f"  Total registros  : {total_geral:,}")
    log.info("=" * 70)


if __name__ == "__main__":
    main()