import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os

# ─── CONFIGURAÇÕES DE CONEXÃO (via .env) ─────────────────────────────────────
load_dotenv()
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_URL  = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# ─── CAMINHOS DOS CSVs ────────────────────────────────────────────────────────
CSV_PRINCIPAL = r"D:\DATACVM\Outros Cadastros\Coordenador de Oferta UnderWriters\cad_coord_oferta.csv"
CSV_RESP      = r"D:\DATACVM\Outros Cadastros\Coordenador de Oferta UnderWriters\cad_coord_oferta_resp.csv"

# ─── FILTRO TP_RESP ───────────────────────────────────────────────────────────
TP_RESP_VALIDO = "DIRETOR RESPONSÁVEL PELA INTERMEDIAÇÃO DE OFERTAS PÚBLICAS DE DISTRIBUIÇÃO"

# ─── COLUNAS USADAS DE CADA CSV ───────────────────────────────────────────────
COLUNAS_PRINCIPAL = [
    "CD_CVM", "CNPJ_UNDERWRITER", "DENOM_SOCIAL", "DENOM_COMERC",
    "DT_REG", "DT_CANCEL", "MOTIVO_CANCEL", "SIT", "DT_INI_SIT",
    "SETOR_ATIV", "VL_PATRIM_LIQ", "DT_PATRIM_LIQ", "UF", "EMAIL"
]

COLUNAS_RESP = ["CNPJ", "TP_RESP", "RESP", "DT_INI_RESP"]

# Colunas finais na ordem da tabela (sem id_underwriter — gerado pelo SERIAL)
COLUNAS_DB = [
    "cd_cvm", "cnpj_underwriter", "denom_social", "denom_comerc",
    "dt_reg", "dt_cancel", "motivo_cancel", "sit", "dt_ini_sit",
    "setor_ativ", "vl_patrim_liq", "dt_patrim_liq", "uf", "email",
    "tp_resp", "resp", "dt_ini_resp"
]

CAMPOS_DATA = ["DT_REG", "DT_CANCEL", "DT_INI_SIT", "DT_PATRIM_LIQ", "DT_INI_RESP"]


def ler_csv(caminho, descricao):
    print(f"Lendo {descricao}: {caminho}")
    df = pd.read_csv(caminho, sep=";", encoding="latin-1", dtype=str, low_memory=False)
    print(f"  -> {len(df):,} linhas | {len(df.columns)} colunas")
    return df


def preparar_principal(df: pd.DataFrame) -> pd.DataFrame:
    print("\n[PRINCIPAL] Verificando colunas...")
    ausentes = [c for c in COLUNAS_PRINCIPAL if c not in df.columns]
    if ausentes:
        raise ValueError(f"Colunas ausentes no CSV principal: {ausentes}")

    df = df[COLUNAS_PRINCIPAL].copy()

    # Remove duplicatas em CNPJ_UNDERWRITER (Unique Key)
    antes = len(df)
    dupl  = df[df.duplicated(subset=["CNPJ_UNDERWRITER"], keep=False)]
    if not dupl.empty:
        print(f"  AVISO: {dupl['CNPJ_UNDERWRITER'].nunique()} CNPJs duplicados em cad_coord_oferta — mantendo primeira ocorrencia")
    df = df.drop_duplicates(subset=["CNPJ_UNDERWRITER"], keep="first")
    print(f"  -> {antes - len(df)} duplicatas removidas. Restam {len(df):,} registros.")
    return df


def preparar_resp(df: pd.DataFrame) -> pd.DataFrame:
    print("\n[RESP] Verificando colunas...")
    ausentes = [c for c in COLUNAS_RESP if c not in df.columns]
    if ausentes:
        raise ValueError(f"Colunas ausentes no CSV de responsaveis: {ausentes}")

    df = df[COLUNAS_RESP].copy()

    # Filtra apenas o TP_RESP válido
    antes = len(df)
    df["TP_RESP"] = df["TP_RESP"].str.strip()
    df = df[df["TP_RESP"] == TP_RESP_VALIDO].copy()
    print(f"  -> Filtro TP_RESP: {antes:,} -> {len(df):,} linhas mantidas")

    # Se houver mais de um diretor por CNPJ, mantém o primeiro
    dupl = df[df.duplicated(subset=["CNPJ"], keep=False)]
    if not dupl.empty:
        print(f"  AVISO: {dupl['CNPJ'].nunique()} CNPJs com mais de um diretor — mantendo primeiro")
    df = df.drop_duplicates(subset=["CNPJ"], keep="first")
    print(f"  -> {len(df):,} responsaveis unicos apos deduplicacao")
    return df


def fazer_join(df_princ: pd.DataFrame, df_resp: pd.DataFrame) -> pd.DataFrame:
    print("\n[JOIN] Unindo tabelas por CNPJ_UNDERWRITER = CNPJ...")

    df = df_princ.merge(
        df_resp,
        left_on="CNPJ_UNDERWRITER",
        right_on="CNPJ",
        how="left"           # LEFT JOIN: underwriters sem diretor ficam com NULL
    )

    # Remove coluna CNPJ duplicada vinda do resp
    if "CNPJ" in df.columns:
        df = df.drop(columns=["CNPJ"])

    com_resp  = df["TP_RESP"].notna().sum()
    sem_resp  = df["TP_RESP"].isna().sum()
    print(f"  -> {com_resp:,} underwriters COM diretor | {sem_resp:,} SEM diretor (campos resp serao NULL)")
    print(f"  -> Total apos join: {len(df):,} linhas")
    return df


def converter_tipos(df: pd.DataFrame) -> pd.DataFrame:
    print("\n[TIPOS] Convertendo datas e normalizando nulos...")
    for campo in CAMPOS_DATA:
        if campo in df.columns:
            df[campo] = pd.to_datetime(df[campo], format="%Y-%m-%d", errors="coerce")
            df[campo] = df[campo].apply(lambda x: None if pd.isnull(x) else x.date())

    # VL_PATRIM_LIQ: converte para float
    if "VL_PATRIM_LIQ" in df.columns:
        df["VL_PATRIM_LIQ"] = pd.to_numeric(df["VL_PATRIM_LIQ"].str.replace(",", "."), errors="coerce")

    df = df.where(pd.notnull(df), None)

    # Seleciona e renomeia para colunas da tabela
    colunas_orig = [c.upper() if c != "TP_RESP" else "TP_RESP" for c in COLUNAS_DB]
    # Monta mapeamento: nome_original -> nome_db
    mapa = {orig: db for orig, db in zip(
        ["CD_CVM","CNPJ_UNDERWRITER","DENOM_SOCIAL","DENOM_COMERC",
         "DT_REG","DT_CANCEL","MOTIVO_CANCEL","SIT","DT_INI_SIT",
         "SETOR_ATIV","VL_PATRIM_LIQ","DT_PATRIM_LIQ","UF","EMAIL",
         "TP_RESP","RESP","DT_INI_RESP"],
        COLUNAS_DB
    )}
    df = df.rename(columns=mapa)[COLUNAS_DB]
    print(f"  -> DataFrame final: {len(df):,} linhas | {len(df.columns)} colunas")
    return df


def inserir_no_banco(df: pd.DataFrame):
    print(f"\nConectando ao banco {DB_NAME} em {DB_HOST}...")
    conn = psycopg2.connect(DB_URL)
    cur  = conn.cursor()

    cur.execute("""
        SELECT table_schema FROM information_schema.tables
        WHERE table_name = 'cadunderwriter'
    """)
    resultado = cur.fetchone()
    if not resultado:
        raise Exception("Tabela 'cadunderwriter' nao encontrada! Verifique se foi criada no banco.")
    schema = resultado[0]
    print(f"  -> Tabela encontrada: {schema}.cadunderwriter")

    colunas_str = ", ".join(COLUNAS_DB)
    sql = f"""
        INSERT INTO {schema}.cadunderwriter ({colunas_str})
        VALUES %s
        ON CONFLICT (cnpj_underwriter) DO NOTHING
    """

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
    print("=" * 65)
    print("  IMPORTACAO: cad_coord_oferta + resp -> cadunderwriter")
    print("=" * 65)

    for path in [CSV_PRINCIPAL, CSV_RESP]:
        if not os.path.exists(path):
            raise FileNotFoundError(f"Arquivo nao encontrado: {path}")

    print("\n[1/6] Lendo CSVs...")
    df_princ = ler_csv(CSV_PRINCIPAL, "cad_coord_oferta")
    df_resp  = ler_csv(CSV_RESP,      "cad_coord_oferta_resp")

    print("\n[2/6] Preparando CSV principal...")
    df_princ = preparar_principal(df_princ)

    print("\n[3/6] Preparando CSV de responsaveis...")
    df_resp = preparar_resp(df_resp)

    print("\n[4/6] Realizando JOIN...")
    df = fazer_join(df_princ, df_resp)

    print("\n[5/6] Convertendo tipos de dados...")
    df = converter_tipos(df)

    print("\n[6/6] Inserindo no banco de dados...")
    inserir_no_banco(df)

    print("\n" + "=" * 65)
    print("  PROCESSO FINALIZADO COM SUCESSO!")
    print("=" * 65)


if __name__ == "__main__":
    main()