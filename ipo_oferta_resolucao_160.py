"""
Script : load_ipo_oferta_resolucao_160.py
Tabela : cvm_data.ipo_oferta_resolucao_160
Fonte  : D:\\DATACVM\\Ofertas Públicas\\oferta_distribuicao\\oferta_resolucao_160.csv

Características do CSV (confirmadas na análise do arquivo real):
  • 12.218 linhas  |  71 colunas  |  separador ';'  |  encoding latin-1
  • Numero_Requerimento: zero nulos, zero duplicatas → PK natural BIGINT
  • CNPJ_Emissor e CNPJ_Lider: 100% preenchidos
  • Status_Requerimento: 7 valores únicos (ex: Encerrado, Em análise...)
  • Vigência: a partir de outubro/2023 (Resolução CVM 160 — rito automático)

Estratégia de INSERT: ON CONFLICT (numero_requerimento) DO UPDATE
  O arquivo é atualizado periodicamente pela CVM com novos requerimentos
  e mudanças de status. O UPSERT permite reprocessar sem DELETE.
  Para forçar recarga completa: DELETE FROM cvm_data.ipo_oferta_resolucao_160;
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

CSV_PATH      = Path(r"D:\DATACVM\Ofertas Públicas\oferta_distribuicao\oferta_resolucao_160.csv")
CSV_SEPARATOR = ";"
CSV_ENCODING  = "latin-1"
BATCH_SIZE    = 2000

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("load_ipo_oferta_resolucao_160.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ─── MAPEAMENTO CSV → TABELA (ordem do prompt) ────────────────────────────────
COL_MAP = {
    "Numero_Requerimento"               : "numero_requerimento",
    "Rito_Requerimento"                 : "rito_requerimento",
    "Numero_Processo"                   : "numero_processo",
    "Data_requerimento"                 : "data_requerimento",
    "Data_Registro"                     : "data_registro",
    "Data_Encerramento"                 : "data_encerramento",
    "Status_Requerimento"               : "status_requerimento",
    "Valor_Mobiliario"                  : "valor_mobiliario",
    "Tipo_requerimento"                 : "tipo_requerimento",
    "Bookbuilding"                      : "bookbuilding",
    "CNPJ_Emissor"                      : "cnpj_emissor",
    "Nome_Emissor"                      : "nome_emissor",
    "CNPJ_Lider"                        : "cnpj_lider",
    "Nome_Lider"                        : "nome_lider",
    "Grupo_Coordenador"                 : "grupo_coordenador",
    "Tipo_Oferta"                       : "tipo_oferta",
    "Emissao"                           : "emissao",
    "Qtde_Total_Registrada"             : "qtde_total_registrada",
    "Valor_Total_Registrado"            : "valor_total_registrado",
    "Oferta_inicial"                    : "oferta_inicial",
    "Oferta_vasos_comunicantes"         : "oferta_vasos_comunicantes",
    "Publico_alvo"                      : "publico_alvo",
    "Reabertura_serie"                  : "reabertura_serie",
    "Titulo_classificado_como_sustentavel": "titulo_classificado_como_sustentavel",
    "Titulo_padronizado"                : "titulo_padronizado",
    "Destinacao_recursos"               : "destinacao_recursos",
    "Data_deliberacao_aprovou_oferta"   : "data_deliberacao_aprovou_oferta",
    "Mercado_negociacao"                : "mercado_negociacao",
    "Tipo_lastro"                       : "tipo_lastro",
    "Regime_fiduciario"                 : "regime_fiduciario",
    "Ativos_alvo"                       : "ativos_alvo",
    "Descricao_garantias"               : "descricao_garantias",
    "Descricao_lastro"                  : "descricao_lastro",
    "Identificacao_devedores_coobrigados": "identificacao_devedores_coobrigados",
    "Possibilidade_revolvencia"         : "possibilidade_revolvencia",
    "FIDC_nao_padronizado"              : "fidc_nao_padronizado",
    "Titulo_incentivado"                : "titulo_incentivado",
    "Regime_distribuicao"               : "regime_distribuicao",
    "Tipo_societario"                   : "tipo_societario",
    "Administrador"                     : "administrador",
    "Gestor"                            : "gestor",
    "Agente_fiduciario"                 : "agente_fiduciario",
    "Escriturador"                      : "escriturador",
    "Custodiante"                       : "custodiante",
    "Avaliador_Risco"                   : "avaliador_risco",
    "Processo_SEI"                      : "processo_sei",
    "Endereco_emissor_rede_mundial_computadores": "endereco_emissor_rede_mundial_computadores",
    "Num_Invest_Pessoa_Natural"                             : "num_invest_pessoa_natural",
    "Qtde_VM_Pessoa_Natural"                                : "qtde_vm_pessoa_natural",
    "Num_Invest_Clube_Investimento"                         : "num_invest_clube_investimento",
    "Qtde_VM_Clube_Investimento"                            : "qtde_vm_clube_investimento",
    "Num_Invest_Fundos_Investimento"                        : "num_invest_fundos_investimento",
    "Qtde_VM_Fundos_Investimento"                           : "qtde_vm_fundos_investimento",
    "Num_Invest_Entidade_Previdencia_Privada"               : "num_invest_entidade_previdencia_privada",
    "Qtde_VM_Entidade_Previdencia_Privada"                  : "qtde_vm_entidade_previdencia_privada",
    "Num_Invest_Companhia_Seguradora"                       : "num_invest_companhia_seguradora",
    "Qtde_VM_Companhia_Seguradora"                          : "qtde_vm_companhia_seguradora",
    "Num_Invest_Investidor_Estrangeiro"                     : "num_invest_investidor_estrangeiro",
    "Qtde_VM_Investidor_Estrangeiro"                        : "qtde_vm_investidor_estrangeiro",
    "Num_Invest_Instit_Intermed_Partic_Consorcio_Distrib"   : "num_invest_instit_intermed_partic_consorcio_distrib",
    "Qtde_VM_Instit_Intermed_Partic_Consorcio_Distrib"      : "qtde_vm_instit_intermed_partic_consorcio_distrib",
    "Num_Invest_Instit_Financ_Emissora_Partic_Consorcio"    : "num_invest_instit_financ_emissora_partic_consorcio",
    "Qtde_VM_Instit_Financ_Emissora_Partic_Consorcio"       : "qtde_vm_instit_financ_emissora_partic_consorcio",
    "Num_Invest_Demais_Instit_Financ"                       : "num_invest_demais_instit_financ",
    "Qtde_VM_Demais_Instit_Financ"                          : "qtde_vm_demais_instit_financ",
    "Num_Invest_Demais_Pessoa_Juridica_Emissora_Partic_Consorcio" : "num_invest_demais_pessoa_juridica_emissora_partic_consorcio",
    "Qtde_VM_Demais_Pessoa_Juridica_Emissora_Partic_Consorcio"    : "qtde_vm_demais_pessoa_juridica_emissora_partic_consorcio",
    "Num_Invest_Demais_Pessoa_Juridica"                     : "num_invest_demais_pessoa_juridica",
    "Qtde_VM_Demais_Pessoa_Juridica"                        : "qtde_vm_demais_pessoa_juridica",
    "Num_Invest_Soc_Adm_Emp_Prop_Demais_Pess_Jurid_Emiss_Partic_Consorcio" : "num_invest_soc_adm",   # nome curto — original (68 chars) truncado pelo PostgreSQL
    "Qdte_VM_Soc_Adm_Emp_Prop_Demais_Pess_Jurid_Emiss_Partic_Consorcio"    : "qdte_vm_soc_adm",      # nome curto — original (65 chars) truncado pelo PostgreSQL
}

DATE_COLS = [
    "Data_requerimento", "Data_Registro", "Data_Encerramento",
]

NUMERIC_COLS = [
    "Numero_Requerimento", "Qtde_Total_Registrada", "Valor_Total_Registrado",
    "Num_Invest_Pessoa_Natural", "Qtde_VM_Pessoa_Natural",
    "Num_Invest_Clube_Investimento", "Qtde_VM_Clube_Investimento",
    "Num_Invest_Fundos_Investimento", "Qtde_VM_Fundos_Investimento",
    "Num_Invest_Entidade_Previdencia_Privada", "Qtde_VM_Entidade_Previdencia_Privada",
    "Num_Invest_Companhia_Seguradora", "Qtde_VM_Companhia_Seguradora",
    "Num_Invest_Investidor_Estrangeiro", "Qtde_VM_Investidor_Estrangeiro",
    "Num_Invest_Instit_Intermed_Partic_Consorcio_Distrib", "Qtde_VM_Instit_Intermed_Partic_Consorcio_Distrib",
    "Num_Invest_Instit_Financ_Emissora_Partic_Consorcio", "Qtde_VM_Instit_Financ_Emissora_Partic_Consorcio",
    "Num_Invest_Demais_Instit_Financ", "Qtde_VM_Demais_Instit_Financ",
    "Num_Invest_Demais_Pessoa_Juridica_Emissora_Partic_Consorcio", "Qtde_VM_Demais_Pessoa_Juridica_Emissora_Partic_Consorcio",
    "Num_Invest_Demais_Pessoa_Juridica", "Qtde_VM_Demais_Pessoa_Juridica",
    "Num_Invest_Soc_Adm_Emp_Prop_Demais_Pess_Jurid_Emiss_Partic_Consorcio",
    "Qdte_VM_Soc_Adm_Emp_Prop_Demais_Pess_Jurid_Emiss_Partic_Consorcio",
]

# Colunas para INSERT/UPDATE (número_requerimento é PK — vai no ON CONFLICT)
INSERT_COLS = list(COL_MAP.values())  # inclui numero_requerimento como primeiro campo

# Campos que NÃO entram no UPDATE SET (apenas a PK)
NO_UPDATE = {"numero_requerimento"}


# ─── FUNÇÕES ──────────────────────────────────────────────────────────────────

def safe_val(v):
    if v is None or v is pd.NaT:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    return v


def read_csv() -> pd.DataFrame:
    log.info(f"Lendo CSV: {CSV_PATH.name}")
    df = pd.read_csv(
        CSV_PATH, sep=CSV_SEPARATOR, encoding=CSV_ENCODING,
        dtype=str, low_memory=False
    )
    df.columns = df.columns.str.strip()
    log.info(f"  {len(df):,} linhas lidas. {len(df.columns)} colunas.")

    # Converte datas
    for col in DATE_COLS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    # Converte numéricos
    for col in NUMERIC_COLS:
        if col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].str.replace(",", ".", regex=False)
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Adiciona colunas ausentes como None
    ausentes = []
    for csv_col in COL_MAP.keys():
        if csv_col not in df.columns:
            df[csv_col] = None
            ausentes.append(csv_col)
    if ausentes:
        log.info(f"  Colunas ausentes no CSV (inseridas como NULL): {ausentes}")

    nao_mapeadas = [c for c in df.columns if c not in COL_MAP]
    if nao_mapeadas:
        log.info(f"  Colunas do CSV não mapeadas (ignoradas): {nao_mapeadas}")

    return df


def build_upsert_query() -> str:
    """
    UPSERT em numero_requerimento: permite reprocessar o arquivo sem DELETE.
    A CVM atualiza status_requerimento e datas ao longo do tempo.
    """
    cols_str   = ", ".join(INSERT_COLS)
    update_set = ",\n            ".join(
        f"{c} = EXCLUDED.{c}"
        for c in INSERT_COLS
        if c not in NO_UPDATE
    ) + ",\n            dt_carga = NOW()"

    return f"""
        INSERT INTO cvm_data.ipo_oferta_resolucao_160 ({cols_str})
        VALUES %s
        ON CONFLICT (numero_requerimento)
        DO UPDATE SET
            {update_set};
    """


def insert(conn, df: pd.DataFrame) -> int:
    df_ins = df[list(COL_MAP.keys())].rename(columns=COL_MAP)

    for col in INSERT_COLS:
        if col not in df_ins.columns:
            df_ins[col] = None

    query    = build_upsert_query()
    total    = len(df_ins)
    inserted = 0

    with conn.cursor() as cur:
        for start in range(0, total, BATCH_SIZE):
            batch   = df_ins.iloc[start : start + BATCH_SIZE]
            records = [
                tuple(safe_val(v) for v in row)
                for row in batch[INSERT_COLS].itertuples(index=False, name=None)
            ]
            execute_values(cur, query, records)
            conn.commit()
            inserted += len(records)
            log.info(f"  {inserted:,}/{total:,} registros inseridos/atualizados...")

    return inserted


def main():
    log.info("=" * 70)
    log.info("Início da carga: ipo_oferta_resolucao_160")
    log.info(f"CSV: {CSV_PATH}")
    log.info("=" * 70)

    if not CSV_PATH.exists():
        log.error(f"Arquivo não encontrado: {CSV_PATH}")
        sys.exit(1)

    log.info("Conectando ao PostgreSQL...")
    try:
        conn = psycopg2.connect(DB_URL)
    except psycopg2.OperationalError as e:
        log.error(f"Falha na conexão: {e}")
        sys.exit(1)

    try:
        insert_count = insert(conn, read_csv())
    except Exception as e:
        conn.rollback()
        log.error(f"Erro durante a carga: {e}")
        raise
    finally:
        conn.close()
        log.info("Conexão encerrada.")

    log.info("=" * 70)
    log.info(f"✔ ipo_oferta_resolucao_160: {insert_count:,} registros inseridos/atualizados.")
    log.info("=" * 70)


if __name__ == "__main__":
    main()