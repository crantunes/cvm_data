"""
Script : dfp_parecer.py

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

CSV_PATH      = r"D:\DATACVM\Formulário de Demonstrações Financeiras Padronizadas (DFP)\dfp_cia_aberta_2025\dfp_cia_aberta_parecer_2025.csv"
CSV_SEPARATOR = ";"
CSV_ENCODING  = "latin-1"
BATCH_SIZE    = 300   # menor que outros scripts — campos TEXT são mais pesados

# ─── ANO DE REFERÊNCIA ────────────────────────────────────────────────────────
# Altere este valor manualmente ao rodar cada ano (ex: 2023, 2024, 2025)
DFP_ANO = 2025

# ─── COLUNAS LIDAS DO CSV ─────────────────────────────────────────────────────
# TP_PARECER_DECL é lido apenas para o lookup/mapeamento — NÃO é inserido
CSV_COLS_NEEDED = [
    "CNPJ_CIA",
    "DT_REFER",
    "VERSAO",
    "TP_RELAT_AUD",
    "TP_PARECER_DECL",       # usado apenas para mapear id_dfp_parecer_aux
    "NUM_ITEM_PARECER_DECL",
    "TXT_PARECER_DECL",
    # DENOM_CIA ignorado
]

# Ordem das colunas para INSERT na tabela
INSERT_COLS = [
    "id_cad_cia_aberta",
    "cnpj_cia",
    "dt_refer",
    "versao",
    "dfp_ano",
    "tp_relat_aud",
    "id_dfp_parecer_aux",
    "slug_parecer",
    "num_item_parecer_decl",
    "txt_parecer_decl",
]

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("load_dfp_parecer.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# ─── FUNÇÕES ──────────────────────────────────────────────────────────────────

def read_csv() -> pd.DataFrame:
    """Lê o CSV, valida colunas, converte tipos básicos."""
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
    missing = [c for c in CSV_COLS_NEEDED if c not in df.columns]
    if missing:
        log.error(f"Colunas ausentes no CSV: {missing}")
        log.error(f"Colunas disponíveis: {list(df.columns)}")
        sys.exit(1)

    # Seleciona apenas colunas necessárias (descarta DENOM_CIA e outras extras)
    df = df[CSV_COLS_NEEDED].copy()

    # ── Limpeza e conversão de tipos ─────────────────────────────────────────
    df["CNPJ_CIA"]            = df["CNPJ_CIA"].str.strip()
    df["TP_PARECER_DECL"]     = df["TP_PARECER_DECL"].str.strip()
    df["TP_RELAT_AUD"]        = df["TP_RELAT_AUD"].str.strip().replace("", None)
    df["TXT_PARECER_DECL"]    = df["TXT_PARECER_DECL"].fillna("")

    df["DT_REFER"] = pd.to_datetime(df["DT_REFER"], errors="coerce").dt.date
    df["VERSAO"]   = pd.to_numeric(df["VERSAO"], errors="coerce").astype("Int16")
    df["NUM_ITEM_PARECER_DECL"] = pd.to_numeric(
        df["NUM_ITEM_PARECER_DECL"], errors="coerce"
    ).astype("Int16")

    # ── Remove linhas sem chave mínima ───────────────────────────────────────
    before = len(df)
    df = df.dropna(subset=["CNPJ_CIA", "DT_REFER", "VERSAO", "TP_PARECER_DECL"])
    dropped = before - len(df)
    if dropped:
        log.warning(f"  {dropped} linha(s) removidas por campos chave nulos.")

    log.info(f"  {len(df):,} linhas válidas após limpeza inicial.")
    return df


def lookup_cnpjs(conn, df: pd.DataFrame) -> dict:
    """
    Verifica se TODOS os CNPJs do CSV existem em cad_cia_aberta.
    → OK : retorna dict {cnpj: id_cad_cia_aberta}
    → NOK: aborta e lista os CNPJs ausentes.
    """
    cnpjs_csv = set(df["CNPJ_CIA"].dropna().unique())
    log.info(f"  Lookup CNPJs: {len(cnpjs_csv):,} CNPJs únicos no CSV.")

    with conn.cursor() as cur:
        cur.execute(
            "SELECT cnpj_companhia, id_cad_cia_aberta FROM cvm_data.cad_cia_aberta"
        )
        rows = cur.fetchall()

    cad_map  = {row[0]: row[1] for row in rows}
    ausentes = sorted(set(cnpjs_csv) - set(cad_map.keys()))

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

    log.info(f"  Lookup CNPJs OK — todos os {len(cnpjs_csv):,} encontrados.")
    return cad_map


def lookup_parecer_aux(conn) -> tuple[dict, dict]:
    """
    Carrega a tabela dfp_parecer_aux em memória.
    Retorna dois dicts:
      nome_to_id   : {nome_original_cvm: id_dfp_parecer_aux}
      nome_to_slug : {nome_original_cvm: slug_parecer}
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT nome_original_cvm, id_dfp_parecer_aux, slug_parecer "
            "FROM cvm_data.dfp_parecer_aux"
        )
        rows = cur.fetchall()

    nome_to_id   = {row[0]: row[1] for row in rows}
    nome_to_slug = {row[0]: row[2] for row in rows}

    log.info(f"  Lookup dfp_parecer_aux: {len(rows)} tipos de parecer carregados.")

    # Exibe o mapa para rastreabilidade no log
    for nome, id_aux in nome_to_id.items():
        log.info(f"    [{id_aux}] {nome_to_slug[nome]!r:20s} ← {nome!r}")

    return nome_to_id, nome_to_slug


def validate_parecer_types(df: pd.DataFrame, nome_to_id: dict) -> None:
    """
    Verifica se todos os valores de TP_PARECER_DECL têm correspondência
    em dfp_parecer_aux.nome_original_cvm. Aborta se houver tipos desconhecidos.
    """
    tipos_csv      = set(df["TP_PARECER_DECL"].dropna().unique())
    tipos_conhecidos = set(nome_to_id.keys())
    desconhecidos  = sorted(tipos_csv - tipos_conhecidos)

    if desconhecidos:
        log.error("=" * 70)
        log.error("ABORTANDO — Tipos de TP_PARECER_DECL sem correspondência em dfp_parecer_aux:")
        log.error("=" * 70)
        for tp in desconhecidos:
            log.error(f"  ✗  {tp!r}")
        log.error("=" * 70)
        log.error(
            "Adicione os tipos acima em cvm_data.dfp_parecer_aux antes de reprocessar."
        )
        sys.exit(1)

    log.info(f"  Validação de tipos OK — {len(tipos_csv)} tipos distintos mapeados.")


def aggregate_texts(df: pd.DataFrame, nome_to_id: dict, nome_to_slug: dict) -> pd.DataFrame:
    """
    Reconstrução semântica dos textos fragmentados.

    O sistema legado da CVM quebra textos longos em N registros sequenciais
    numerados por NUM_ITEM_PARECER_DECL. Aqui:
      1. Ordena por NUM_ITEM_PARECER_DECL dentro de cada grupo
      2. Concatena TXT_PARECER_DECL com espaço simples (string_agg equivalente)
      3. Registra o MAX(NUM_ITEM_PARECER_DECL) — indica quantos fragmentos havia

    Chave de grupo: CNPJ_CIA + DT_REFER + VERSAO + TP_PARECER_DECL
    (um mesmo CNPJ pode ter múltiplos tipos de parecer na mesma data)
    """
    log.info("  Iniciando agregação de textos fragmentados...")
    linhas_antes = len(df)

    # Ordena para garantir sequência correta antes do groupby
    df_sorted = df.sort_values(
        ["CNPJ_CIA", "DT_REFER", "VERSAO", "TP_PARECER_DECL", "NUM_ITEM_PARECER_DECL"],
        ascending=True,
        na_position="last",
    )

    agg = (
        df_sorted
        .groupby(
            ["CNPJ_CIA", "DT_REFER", "VERSAO", "TP_PARECER_DECL", "TP_RELAT_AUD"],
            dropna=False,
            sort=False,
        )
        .agg(
            num_item_parecer_decl=("NUM_ITEM_PARECER_DECL", "max"),
            txt_parecer_decl     =("TXT_PARECER_DECL",      lambda x: " ".join(
                str(v) for v in x if v and str(v).strip()
            )),
        )
        .reset_index()
    )

    linhas_depois = len(agg)
    fragmentos    = linhas_antes - linhas_depois
    log.info(
        f"  Agregação concluída: {linhas_antes:,} fragmentos → "
        f"{linhas_depois:,} registros ({fragmentos:,} fragmentos reunidos)."
    )

    # ── Mapeia id_dfp_parecer_aux e slug_parecer ─────────────────────────────
    agg["id_dfp_parecer_aux"] = agg["TP_PARECER_DECL"].map(nome_to_id)
    agg["slug_parecer"]       = agg["TP_PARECER_DECL"].map(nome_to_slug)

    # ── Renomeia para padrão snake_case da tabela ────────────────────────────
    agg = agg.rename(columns={
        "CNPJ_CIA"     : "cnpj_cia",
        "DT_REFER"     : "dt_refer",
        "VERSAO"       : "versao",
        "TP_RELAT_AUD" : "tp_relat_aud",
    })

    # ── Adiciona dfp_ano ─────────────────────────────────────────────────────
    agg["dfp_ano"] = DFP_ANO

    # ── Substitui strings vazias por None em txt_parecer_decl ────────────────
    agg["txt_parecer_decl"] = agg["txt_parecer_decl"].replace("", None)

    return agg


def year_already_loaded(conn) -> bool:
    """Evita reprocessamento acidental do mesmo dfp_ano."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(1) FROM cvm_data.dfp_parecer WHERE dfp_ano = %s",
            (DFP_ANO,)
        )
        count = cur.fetchone()[0]

    if count > 0:
        log.warning(
            f"dfp_ano={DFP_ANO} já possui {count:,} registros em dfp_parecer — "
            "abortando para evitar duplicação.\n"
            f"Para reprocessar: DELETE FROM cvm_data.dfp_parecer WHERE dfp_ano = {DFP_ANO};"
        )
        return True
    return False


def insert_batches(conn, df: pd.DataFrame, cad_map: dict) -> None:
    """Insere os registros agregados em lotes."""

    df = df.copy()
    df["id_cad_cia_aberta"] = df["cnpj_cia"].map(cad_map)

    query = f"""
        INSERT INTO cvm_data.dfp_parecer ({", ".join(INSERT_COLS)})
        VALUES %s
    """

    total    = len(df)
    inserted = 0

    with conn.cursor() as cur:
        for start in range(0, total, BATCH_SIZE):
            batch = df.iloc[start : start + BATCH_SIZE]
            records = [
                tuple(
                    None if (v is None or (isinstance(v, float) and pd.isna(v))) else
                    int(v) if hasattr(v, "item") else v
                    for v in row
                )
                for row in batch[INSERT_COLS].itertuples(index=False, name=None)
            ]
            execute_values(cur, query, records)
            conn.commit()
            inserted += len(records)
            log.info(f"  Progresso: {inserted:,} / {total:,} registros inseridos.")

    log.info(f"✔ Carga concluída: {inserted:,} registros inseridos em dfp_parecer.")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 70)
    log.info(f"Início da carga: dfp_parecer | dfp_ano={DFP_ANO}")
    log.info("=" * 70)

    # 1. Lê e limpa o CSV
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

        # 5. Carrega tabela de tipos de parecer em memória
        log.info("Carregando mapa dfp_parecer_aux...")
        nome_to_id, nome_to_slug = lookup_parecer_aux(conn)

        # 6. Valida se todos os tipos do CSV têm correspondência
        log.info("Validando tipos de TP_PARECER_DECL...")
        validate_parecer_types(df, nome_to_id)

        # 7. Agrega fragmentos de texto por grupo
        log.info("Agregando fragmentos de TXT_PARECER_DECL...")
        df_agg = aggregate_texts(df, nome_to_id, nome_to_slug)

        # 8. Insere os dados agregados
        log.info("Iniciando inserção em lotes...")
        insert_batches(conn, df_agg, cad_map)

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
    log.info("Carga dfp_parecer finalizada com sucesso.")
    log.info("=" * 70)


if __name__ == "__main__":
    main()