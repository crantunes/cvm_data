"""
Script : load_ipo_oferta_distribuicao.py
Tabela : cvm_data.ipo_oferta_distribuicao
Fonte  : D:\\DATACVM\\Ofertas Públicas\\oferta_distribuicao\\oferta_distribuicao.csv

Características do CSV (confirmadas na análise do arquivo real):
  • 48.942 linhas  |  76 colunas  |  separador ';'  |  encoding latin-1
  • Numero_Processo    : NULL em 57% das linhas (rito ICVM 476 não tem processo)
  • Numero_Registro_Oferta: NULL em 58% das linhas
  • Sem PK combinada confiável — inserção raw com SERIAL
  • Colunas Nr_/Qtd_ de colocação por investidor: presentes no CSV mas NULL
    para registros anteriores a março/2024 (inclusão progressiva pela CVM)
  • Qtd_* podem ser fracionários (ex: 8436.9674) — NUMERIC(15,4) na tabela

Proteção idempotente: aborta se a tabela já tiver dados.
Para reprocessar: DELETE FROM cvm_data.ipo_oferta_distribuicao;
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

CSV_PATH      = Path(r"D:\DATACVM\Ofertas Públicas\oferta_distribuicao\oferta_distribuicao.csv")
CSV_SEPARATOR = ";"
CSV_ENCODING  = "latin-1"
BATCH_SIZE    = 2000

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("load_ipo_oferta_distribuicao.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ─── MAPEAMENTO CSV → TABELA (ordem do prompt) ────────────────────────────────
# Chave: nome exato da coluna no cabeçalho do CSV
# Valor: nome da coluna na tabela PostgreSQL
COL_MAP = {
    "Numero_Processo"                   : "numero_processo",
    "Numero_Registro_Oferta"            : "numero_registro_oferta",
    "Tipo_Oferta"                       : "tipo_oferta",
    "Tipo_Componente_Oferta_Mista"      : "tipo_componente_oferta_mista",
    "Tipo_Ativo"                        : "tipo_ativo",
    "CNPJ_Emissor"                      : "cnpj_emissor",
    "Nome_Emissor"                      : "nome_emissor",
    "CNPJ_Lider"                        : "cnpj_lider",
    "Nome_Lider"                        : "nome_lider",
    "Nome_Vendedor"                     : "nome_vendedor",
    "CNPJ_Ofertante"                    : "cnpj_ofertante",
    "Nome_Ofertante"                    : "nome_ofertante",
    "Rito_Oferta"                       : "rito_oferta",
    "Modalidade_Oferta"                 : "modalidade_oferta",
    "Modalidade_Registro"               : "modalidade_registro",
    "Modalidade_Dispensa_Registro"      : "modalidade_dispensa_registro",
    "Data_Abertura_Processo"            : "data_abertura_processo",
    "Data_Protocolo"                    : "data_protocolo",
    "Data_Dispensa_Oferta"              : "data_dispensa_oferta",
    "Data_Registro_Oferta"              : "data_registro_oferta",
    "Data_Inicio_Oferta"                : "data_inicio_oferta",
    "Data_Encerramento_Oferta"          : "data_encerramento_oferta",
    "Emissao"                           : "emissao",
    "Classe_Ativo"                      : "classe_ativo",
    "Serie"                             : "serie",
    "Especie_Ativo"                     : "especie_ativo",
    "Forma_Ativo"                       : "forma_ativo",
    "Data_Emissao"                      : "data_emissao",
    "Data_Vencimento"                   : "data_vencimento",
    "Quantidade_Sem_Lote_Suplementar"   : "quantidade_sem_lote_suplementar",
    "Quantidade_No_Lote_Suplementar"    : "quantidade_no_lote_suplementar",
    "Quantidade_Total"                  : "quantidade_total",
    "Preco_Unitario"                    : "preco_unitario",
    "Valor_Total"                       : "valor_total",
    "Oferta_Inicial"                    : "oferta_inicial",
    "Oferta_Incentivo_Fiscal"           : "oferta_incentivo_fiscal",
    "Oferta_Regime_Fiduciario"          : "oferta_regime_fiduciario",
    "Atualizacao_Monetaria"             : "atualizacao_monetaria",
    "Juros"                             : "juros",
    "Projeto_Audiovisual"               : "projeto_audiovisual",
    "Tipo_Societario_Emissor"           : "tipo_societario_emissor",
    "Tipo_Fundo_Investimento"           : "tipo_fundo_investimento",
    "Ultimo_Comunicado"                 : "ultimo_comunicado",
    "Data_Comunicado"                   : "data_comunicado",
    "Nr_Pessoa_Fisica"                  : "nr_pessoa_fisica",
    "Qtd_Pessoa_Fisica"                 : "qtd_pessoa_fisica",
    "Nr_Clube_Investimento"             : "nr_clube_investimento",
    "Qtd_Clube_Investimento"            : "qtd_clube_investimento",
    "Nr_Fundos_Investimento"            : "nr_fundos_investimento",
    "Qtd_Fundos_Investimento"           : "qtd_fundos_investimento",
    "Nr_Entidade_Previdencia_Privada"   : "nr_entidade_previdencia_privada",
    "Qtd_Entidade_Previdencia_Privada"  : "qtd_entidade_previdencia_privada",
    "Nr_Companhia_Seguradora"           : "nr_companhia_seguradora",
    "Qtd_Companhia_Seguradora"          : "qtd_companhia_seguradora",
    "Nr_Investidor_Estrangeiro"         : "nr_investidor_estrangeiro",
    "Qtd_Investidor_Estrangeiro"        : "qtd_investidor_estrangeiro",
    "Nr_Instit_Intermed_Partic_Consorcio_Distrib"            : "nr_instit_intermed_partic_consorcio_distrib",
    "Qtd_Instit_Intermed_Partic_Consorcio_Distrib"           : "qtd_instit_intermed_partic_consorcio_distrib",
    "Nr_Instit_Financ_Emissora_Partic_Consorcio"             : "nr_instit_financ_emissora_partic_consorcio",
    "Qtd_Instit_Financ_Emissora_Partic_Consorcio"            : "qtd_instit_financ_emissora_partic_consorcio",
    "Nr_Demais_Instit_Financ"                                : "nr_demais_instit_financ",
    "Qtd_Demais_Instit_Financ"                               : "qtd_demais_instit_financ",
    "Nr_Demais_Pessoa_Juridica_Emissora_Partic_Consorcio"    : "nr_demais_pessoa_juridica_emissora_partic_consorcio",
    "Qtd_Demais_Pessoa_Juridica_Emissora_Partic_Consorcio"   : "qtd_demais_pessoa_juridica_emissora_partic_consorcio",
    "Nr_Demais_Pessoa_Juridica"                              : "nr_demais_pessoa_juridica",
    "Qtd_Demais_Pessoa_Juridica"                             : "qtd_demais_pessoa_juridica",
    "Nr_Soc_Adm_Emp_Prop_Demais_Pess_Jurid_Emiss_Partic_Consorcio"  : "nr_soc_adm_emp_prop_demais_pess_jurid_emiss_partic_consorcio",
    "Qdt_Soc_Adm_Emp_Prop_Demais_Pess_Jurid_Emiss_Partic_Consorcio" : "qdt_soc_adm_emp_prop_demais_pess_jurid_emiss_partic_consorcio",
    "Nr_Outros"                                              : "nr_outros",
    "Qtd_Outros"                                             : "qtd_outros",
    "Qtd_Cli_Pessoa_Fisica"                                  : "qtd_cli_pessoa_fisica",
    "Qtd_Cli_Pessoa_Juridica"                                : "qtd_cli_pessoa_juridica",
    "Qtd_Cli_Pessoa_Juridica_Ligada_Adm"                     : "qtd_cli_pessoa_juridica_ligada_adm",
    "QtD_Cli_Demais_Pessoa_Juridica"                         : "qtd_cli_demais_pessoa_juridica",
    "Qtd_Cli_Investidor_Estrangeiro"                         : "qtd_cli_investidor_estrangeiro",
    "Qtd_Cli_Soc_Adm_Emp_Prop_Demais_Pess_Jurid_Emiss_Partic_Consorcio": "qtd_cli_soc_adm",  # nome curto — original (65 chars) truncado pelo PostgreSQL
}

DATE_COLS = [
    "Data_Abertura_Processo", "Data_Protocolo", "Data_Dispensa_Oferta",
    "Data_Registro_Oferta", "Data_Inicio_Oferta", "Data_Encerramento_Oferta",
    "Data_Emissao", "Data_Vencimento", "Data_Comunicado",
]

NUMERIC_COLS = [
    "Quantidade_Sem_Lote_Suplementar", "Quantidade_No_Lote_Suplementar",
    "Quantidade_Total", "Preco_Unitario", "Valor_Total",
    "Nr_Pessoa_Fisica", "Qtd_Pessoa_Fisica",
    "Nr_Clube_Investimento", "Qtd_Clube_Investimento",
    "Nr_Fundos_Investimento", "Qtd_Fundos_Investimento",
    "Nr_Entidade_Previdencia_Privada", "Qtd_Entidade_Previdencia_Privada",
    "Nr_Companhia_Seguradora", "Qtd_Companhia_Seguradora",
    "Nr_Investidor_Estrangeiro", "Qtd_Investidor_Estrangeiro",
    "Nr_Instit_Intermed_Partic_Consorcio_Distrib", "Qtd_Instit_Intermed_Partic_Consorcio_Distrib",
    "Nr_Instit_Financ_Emissora_Partic_Consorcio", "Qtd_Instit_Financ_Emissora_Partic_Consorcio",
    "Nr_Demais_Instit_Financ", "Qtd_Demais_Instit_Financ",
    "Nr_Demais_Pessoa_Juridica_Emissora_Partic_Consorcio", "Qtd_Demais_Pessoa_Juridica_Emissora_Partic_Consorcio",
    "Nr_Demais_Pessoa_Juridica", "Qtd_Demais_Pessoa_Juridica",
    "Nr_Soc_Adm_Emp_Prop_Demais_Pess_Jurid_Emiss_Partic_Consorcio",
    "Qdt_Soc_Adm_Emp_Prop_Demais_Pess_Jurid_Emiss_Partic_Consorcio",
    "Nr_Outros", "Qtd_Outros",
    "Qtd_Cli_Pessoa_Fisica", "Qtd_Cli_Pessoa_Juridica",
    "Qtd_Cli_Pessoa_Juridica_Ligada_Adm", "QtD_Cli_Demais_Pessoa_Juridica",
    "Qtd_Cli_Investidor_Estrangeiro",
    "Qtd_Cli_Soc_Adm_Emp_Prop_Demais_Pess_Jurid_Emiss_Partic_Consorcio",
]

# Colunas na ordem exata do INSERT (sem id_ que é SERIAL)
INSERT_COLS = list(COL_MAP.values())


# ─── FUNÇÕES ──────────────────────────────────────────────────────────────────

def safe_val(v):
    """NaT, NA, nan → None."""
    if v is None or v is pd.NaT:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    return v


def already_loaded(conn) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(1) FROM cvm_data.ipo_oferta_distribuicao")
        count = cur.fetchone()[0]
    if count > 0:
        log.warning(
            f"ipo_oferta_distribuicao já possui {count:,} registros — abortando.\n"
            "Para reprocessar: DELETE FROM cvm_data.ipo_oferta_distribuicao;"
        )
        return True
    return False


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

    # Converte numéricos — substitui vírgula decimal se necessário
    for col in NUMERIC_COLS:
        if col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].str.replace(",", ".", regex=False)
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Adiciona colunas ausentes do mapeamento como None
    # (ex: campos Nr_/Qtd_ ausentes em CSVs anteriores a mar/2024)
    ausentes = []
    for csv_col in COL_MAP.keys():
        if csv_col not in df.columns:
            df[csv_col] = None
            ausentes.append(csv_col)
    if ausentes:
        log.info(f"  Colunas ausentes no CSV (inseridas como NULL): {ausentes}")

    # Log de colunas do CSV não mapeadas
    nao_mapeadas = [c for c in df.columns if c not in COL_MAP]
    if nao_mapeadas:
        log.info(f"  Colunas do CSV não mapeadas (ignoradas): {nao_mapeadas}")

    return df


def insert(conn, df: pd.DataFrame) -> int:
    # Renomeia para snake_case da tabela
    df_ins = df[list(COL_MAP.keys())].rename(columns=COL_MAP)

    cols_str = ", ".join(INSERT_COLS)
    query    = f"INSERT INTO cvm_data.ipo_oferta_distribuicao ({cols_str}) VALUES %s"

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
            log.info(f"  {inserted:,}/{total:,} registros inseridos...")

    return inserted


def main():
    log.info("=" * 70)
    log.info("Início da carga: ipo_oferta_distribuicao")
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
        if already_loaded(conn):
            sys.exit(0)

        df    = read_csv()
        total = insert(conn, df)

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
    log.info(f"✔ ipo_oferta_distribuicao: {total:,} registros inseridos.")
    log.info("=" * 70)


if __name__ == "__main__":
    main()