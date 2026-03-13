import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os

# ─── CONFIGURAÇÕES DE CONEXÃO ────────────────────────────────────────────────
DB_CONFIG = {
    "host":     "localhost",
    "database": "cvm_data",
    "user":     "pesquisador",
    "password": "sua_senha_forte",    # ajuste se necessário
    "port":     5432
}

# ─── CAMINHO DO ARQUIVO CSV ───────────────────────────────────────────────────
CSV_PATH = r"D:\DATACVM\Outros Cadastros\Companhia Aberta\cad_cia_aberta.csv"

# ─── COLUNAS QUE EXISTEM NA TABELA (meta_cad_cia_aberta2.txt) ────────────────
COLUNAS_TABELA = [
    "CNPJ_CIA", "DENOM_SOCIAL", "DENOM_COMERC", "DT_REG", "DT_CONST",
    "DT_CANCEL", "MOTIVO_CANCEL", "SIT", "DT_INI_SIT", "CD_CVM",
    "SETOR_ATIV", "TP_MERC", "CATEG_REG", "DT_INI_CATEG", "SIT_EMISSOR",
    "DT_INI_SIT_EMISSOR", "CONTROLE_ACIONARIO", "TP_ENDER", "LOGRADOURO",
    "COMPL", "BAIRRO", "MUN", "UF", "PAIS", "CEP", "DDD_TEL", "TEL",
    "DDD_FAX", "FAX", "EMAIL", "CNPJ_AUDITOR", "AUDITOR"
]

# Mapeamento para nomes em minúsculo (como estão na tabela PostgreSQL)
COLUNAS_DB = [c.lower() for c in COLUNAS_TABELA]

# ─── CAMPOS DE DATA ───────────────────────────────────────────────────────────
CAMPOS_DATA = [
    "DT_REG", "DT_CONST", "DT_CANCEL", "DT_INI_SIT",
    "DT_INI_CATEG", "DT_INI_SIT_EMISSOR"
]


def carregar_csv(caminho: str) -> pd.DataFrame:
    """Lê o CSV com separador ';' e encoding latin-1 (padrão CVM)."""
    print(f"Lendo arquivo: {caminho}")
    df = pd.read_csv(
        caminho,
        sep=";",
        encoding="latin-1",
        dtype=str,          # lê tudo como string para evitar conversões automáticas
        low_memory=False
    )
    print(f"  → {len(df):,} linhas carregadas | {len(df.columns)} colunas no CSV")
    return df


def filtrar_tp_merc(df: pd.DataFrame) -> pd.DataFrame:
    """Mantém apenas linhas onde TP_MERC == 'BOLSA' ou TP_MERC está vazio/nulo."""
    col = "TP_MERC"
    if col not in df.columns:
        raise ValueError(f"Coluna '{col}' não encontrada no CSV.")

    mask = df[col].str.strip().str.upper().eq("BOLSA") | df[col].isna() | df[col].str.strip().eq("")
    filtrado = df[mask].copy()
    print(f"  → {len(filtrado):,} linhas após filtro TP_MERC = 'BOLSA' ou vazio")
    return filtrado


def tratar_duplicatas(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicatas na chave primária CNPJ_CIA, mantendo a primeira ocorrência."""
    col = "CNPJ_CIA"
    antes = len(df)
    duplicatas = df[df.duplicated(subset=[col], keep=False)]

    if not duplicatas.empty:
        print(f"\n⚠️  Duplicatas encontradas em CNPJ_CIA ({len(duplicatas)} linhas, {duplicatas[col].nunique()} CNPJs únicos):")
        print(duplicatas[[col, "DENOM_SOCIAL", "TP_MERC"]].to_string(index=False))

    df = df.drop_duplicates(subset=[col], keep="first").copy()
    removidas = antes - len(df)
    if removidas:
        print(f"  → {removidas} linha(s) duplicada(s) removida(s). Restam {len(df):,} linhas.")
    else:
        print(f"  → Nenhuma duplicata em CNPJ_CIA. Total: {len(df):,} linhas.")
    return df


def preparar_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Seleciona apenas as colunas da tabela e converte campos de data."""
    # Verifica quais colunas do CSV estão ausentes
    colunas_existentes = [c for c in COLUNAS_TABELA if c in df.columns]
    colunas_ausentes   = [c for c in COLUNAS_TABELA if c not in df.columns]

    if colunas_ausentes:
        print(f"\n⚠️  Colunas do CSV ausentes (serão preenchidas com NULL): {colunas_ausentes}")
        for c in colunas_ausentes:
            df[c] = None

    df = df[COLUNAS_TABELA].copy()

    # Converte datas — NaT deve virar None (psycopg2 não aceita NaT)
    for campo in CAMPOS_DATA:
        df[campo] = pd.to_datetime(df[campo], format="%Y-%m-%d", errors="coerce")
        df[campo] = df[campo].apply(lambda x: None if pd.isnull(x) else x.date())

    # Substitui NaN/None nos demais campos
    df = df.where(pd.notnull(df), None)

    # Renomeia colunas para minúsculo
    df.columns = COLUNAS_DB

    print(f"  → DataFrame preparado: {len(df):,} linhas | {len(df.columns)} colunas")
    return df


def inserir_no_banco(df: pd.DataFrame):
    """Insere os dados na tabela usando INSERT ... ON CONFLICT DO NOTHING."""

    # ── Ajuste o schema abaixo se a tabela não estiver em 'public' ──
    # Para descobrir: no pgAdmin, clique com botão direito na tabela → Properties → Schema
    SCHEMA = "public"
    TABELA = "cad_cia_aberta"

    print(f"\nConectando ao banco {DB_CONFIG['database']} em {DB_CONFIG['host']}...")
    conn = psycopg2.connect(**DB_CONFIG)
    cur  = conn.cursor()

    # Verifica se a tabela existe e mostra o schema real caso não encontre
    cur.execute("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_name = %s
    """, (TABELA,))
    resultados = cur.fetchall()
    if not resultados:
        raise Exception(f"Tabela '{TABELA}' não encontrada em nenhum schema! Verifique o nome no pgAdmin.")
    elif len(resultados) > 1:
        print(f"⚠️  Tabela '{TABELA}' encontrada em múltiplos schemas: {resultados}")
        print(f"   Usando schema configurado: '{SCHEMA}'")
    else:
        SCHEMA = resultados[0][0]
        print(f"  → Tabela encontrada: {SCHEMA}.{TABELA}")

    colunas_str = ", ".join(COLUNAS_DB)
    sql = f"""
        INSERT INTO {SCHEMA}.{TABELA} ({colunas_str})
        VALUES %s
        ON CONFLICT (cnpj_cia) DO NOTHING
    """

    # Converte DataFrame para lista de tuplas
    registros = [tuple(row) for row in df.itertuples(index=False, name=None)]

    BATCH_SIZE = 1000
    total_inserido = 0

    print(f"Inserindo {len(registros):,} registros em lotes de {BATCH_SIZE}...")
    for i in range(0, len(registros), BATCH_SIZE):
        lote = registros[i:i + BATCH_SIZE]
        execute_values(cur, sql, lote)
        total_inserido += len(lote)
        print(f"  → Inseridos {min(total_inserido, len(registros)):,} / {len(registros):,}", end="\r")

    conn.commit()
    cur.close()
    conn.close()
    print(f"\n✅ Importação concluída! {total_inserido:,} registros processados (duplicatas ignoradas pelo banco).")


def main():
    print("=" * 60)
    print("  IMPORTAÇÃO: cad_cia_aberta → PostgreSQL")
    print("=" * 60)

    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"Arquivo não encontrado: {CSV_PATH}")

    df = carregar_csv(CSV_PATH)

    print("\n[1/4] Filtrando por TP_MERC...")
    df = filtrar_tp_merc(df)

    print("\n[2/4] Verificando e tratando duplicatas em CNPJ_CIA...")
    df = tratar_duplicatas(df)

    print("\n[3/4] Preparando colunas e tipos de dados...")
    df = preparar_dataframe(df)

    print("\n[4/4] Inserindo no banco de dados...")
    inserir_no_banco(df)

    print("\n" + "=" * 60)
    print("  PROCESSO FINALIZADO COM SUCESSO!")
    print("=" * 60)


if __name__ == "__main__":
    main()