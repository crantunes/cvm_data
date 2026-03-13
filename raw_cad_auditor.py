import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os

# ─── CONFIGURAÇÕES DE CONEXÃO (via .env) ─────────────────────────────────────
# Crie um arquivo .env na mesma pasta do script com o conteúdo:
#   DB_USER=pesquisador
#   DB_PASSWORD=sua_senha
#   DB_HOST=localhost
#   DB_PORT=5432
#   DB_NAME=cvm_data
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_URL  = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# ─── CAMINHO DO ARQUIVO CSV ───────────────────────────────────────────────────
CSV_PATH = r"D:\DATACVM\Outros Cadastros\Auditor\cad_auditor_pj.csv"

# ─── COLUNAS QUE SERÃO INSERIDAS (ordem = ordem da tabela, sem id_auditor) ───
COLUNAS_INSERT = ["CD_CVM", "CNPJ", "DENOM_SOCIAL", "SIT", "DT_INI_SIT"]
COLUNAS_DB     = [c.lower() for c in COLUNAS_INSERT]

# ─── CAMPOS DE DATA ───────────────────────────────────────────────────────────
CAMPOS_DATA = ["DT_INI_SIT"]


def carregar_csv(caminho: str) -> pd.DataFrame:
    print(f"Lendo arquivo: {caminho}")
    df = pd.read_csv(
        caminho,
        sep=";",
        encoding="latin-1",
        dtype=str,
        low_memory=False
    )
    print(f"  -> {len(df):,} linhas carregadas | {len(df.columns)} colunas no CSV")
    return df


def selecionar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    colunas_ausentes = [c for c in COLUNAS_INSERT if c not in df.columns]
    if colunas_ausentes:
        raise ValueError(f"Colunas nao encontradas no CSV: {colunas_ausentes}")
    df = df[COLUNAS_INSERT].copy()
    print(f"  -> Colunas selecionadas: {COLUNAS_INSERT}")
    return df


def tratar_duplicatas(df: pd.DataFrame) -> pd.DataFrame:
    col   = "CNPJ"
    antes = len(df)
    duplicatas = df[df.duplicated(subset=[col], keep=False)]
    if not duplicatas.empty:
        qtd_cnpjs = duplicatas[col].nunique()
        print(f"\n  AVISO: Duplicatas em CNPJ: {len(duplicatas)} linhas | {qtd_cnpjs} CNPJs repetidos")
        print(duplicatas[[col, "DENOM_SOCIAL"]].drop_duplicates(subset=[col]).to_string(index=False))
    df = df.drop_duplicates(subset=[col], keep="first").copy()
    removidas = antes - len(df)
    if removidas:
        print(f"  -> {removidas} linha(s) duplicada(s) removida(s). Restam {len(df):,} linhas.")
    else:
        print(f"  -> Nenhuma duplicata encontrada. Total: {len(df):,} linhas.")
    return df


def converter_tipos(df: pd.DataFrame) -> pd.DataFrame:
    for campo in CAMPOS_DATA:
        df[campo] = pd.to_datetime(df[campo], format="%Y-%m-%d", errors="coerce")
        df[campo] = df[campo].apply(lambda x: None if pd.isnull(x) else x.date())
    df = df.where(pd.notnull(df), None)
    df.columns = COLUNAS_DB
    print(f"  -> DataFrame pronto: {len(df):,} linhas | {len(df.columns)} colunas")
    return df


def inserir_no_banco(df: pd.DataFrame):
    print(f"\nConectando ao banco {DB_NAME} em {DB_HOST}...")
    conn = psycopg2.connect(DB_URL)
    cur  = conn.cursor()

    cur.execute("""
        SELECT table_schema FROM information_schema.tables
        WHERE table_name = 'cadauditor'
    """)
    resultado = cur.fetchone()
    if not resultado:
        raise Exception("Tabela 'cadauditor' nao encontrada! Verifique se foi criada no banco.")
    schema = resultado[0]
    print(f"  -> Tabela encontrada: {schema}.cadauditor")

    colunas_str = ", ".join(COLUNAS_DB)
    sql = f"INSERT INTO {schema}.cadauditor ({colunas_str}) VALUES %s ON CONFLICT (cnpj) DO NOTHING"

    registros  = [tuple(row) for row in df.itertuples(index=False, name=None)]
    BATCH_SIZE = 1000
    total      = 0

    print(f"Inserindo {len(registros):,} registros em lotes de {BATCH_SIZE}...")
    for i in range(0, len(registros), BATCH_SIZE):
        lote = registros[i:i + BATCH_SIZE]
        execute_values(cur, sql, lote)
        total += len(lote)
        print(f"  -> {min(total, len(registros)):,} / {len(registros):,}", end="\r")

    conn.commit()
    cur.close()
    conn.close()
    print(f"\nImportacao concluida! {total:,} registros processados.")


def main():
    print("=" * 60)
    print("  IMPORTACAO: cad_auditor_pj -> cadauditor")
    print("=" * 60)

    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"Arquivo nao encontrado: {CSV_PATH}")

    df = carregar_csv(CSV_PATH)

    print("\n[1/4] Selecionando colunas...")
    df = selecionar_colunas(df)

    print("\n[2/4] Removendo duplicatas em CNPJ...")
    df = tratar_duplicatas(df)

    print("\n[3/4] Convertendo tipos de dados...")
    df = converter_tipos(df)

    print("\n[4/4] Inserindo no banco de dados...")
    inserir_no_banco(df)

    print("\n" + "=" * 60)
    print("  PROCESSO FINALIZADO COM SUCESSO!")
    print("=" * 60)


if __name__ == "__main__":
    main()