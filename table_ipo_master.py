import pandas as pd
import requests
from sqlalchemy import create_engine
import io
import zipfile

engine = create_engine('postgresql://pesquisador:sua_senha_forte@localhost:5432/cvm_data')

def load_cvm_robust():
    # URL CORRIGIDA: agora é .zip, não .csv
    url = "https://dados.cvm.gov.br/dados/OFERTA/DISTRIB/DADOS/oferta_distribuicao.zip"
    print("Iniciando captura definitiva da base CVM...")

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': '*/*',
        'Accept-Language': 'pt-BR,pt;q=0.9',
        'Connection': 'keep-alive',
    }

    session = requests.Session()
    session.headers.update(headers)

    response = session.get(url, timeout=120)
    response.raise_for_status()

    print(f"Download concluído. Tamanho: {len(response.content) / 1024:.1f} KB")

    # Extrai o CSV de dentro do ZIP em memória
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        # Lista arquivos no ZIP para debug
        print(f"Arquivos no ZIP: {z.namelist()}")
        
        # Pega o primeiro .csv encontrado
        csv_files = [f for f in z.namelist() if f.endswith('.csv')]
        if not csv_files:
            raise ValueError(f"Nenhum CSV encontrado no ZIP. Conteúdo: {z.namelist()}")
        
        csv_filename = csv_files[0]
        print(f"Lendo arquivo: {csv_filename}")
        
        with z.open(csv_filename) as f:
            content = f.read().decode('iso-8859-1')

    df = pd.read_csv(io.StringIO(content), sep=';', on_bad_lines='skip', engine='python')

    if len(df.columns) < 5:
        df = pd.read_csv(io.StringIO(content), sep=',', on_bad_lines='skip', engine='python')

    df.columns = (
        df.columns
        .str.strip()
        .str.upper()
        .str.replace(' ', '_', regex=False)
        .str.replace('Ç', 'C', regex=False)
        .str.replace('Ã', 'A', regex=False)
        .str.replace('Õ', 'O', regex=False)
        .str.replace('É', 'E', regex=False)
        .str.replace('Ê', 'E', regex=False)
        .str.replace('Á', 'A', regex=False)
        .str.replace('Í', 'I', regex=False)
        .str.replace('Ó', 'O', regex=False)
        .str.replace('Ú', 'U', regex=False)
    )
    return df

df_raw = load_cvm_robust()

print("Colunas disponíveis:", df_raw.columns.tolist())
print(f"Total de colunas: {len(df_raw.columns)}")

def get_col(keywords, col_name_hint=""):
    for kw in keywords:
        cols = [c for c in df_raw.columns if kw in c]
        if cols:
            return cols[0]
    raise ValueError(
        f"Coluna '{col_name_hint}' não encontrada. "
        f"Palavras-chave buscadas: {keywords}. "
        f"Colunas disponíveis: {df_raw.columns.tolist()}"
    )

try:
    c_cnpj  = get_col(['CNPJ_EMISSOR', 'CNPJ'],              'CNPJ')
    c_tipo  = get_col(['TIPO_ATIVO', 'ATIVO', 'TIPO'],        'TIPO_ATIVO')
    c_data  = get_col(['DATA_REGISTRO', 'DATA'],              'DATA')
    c_valor = get_col(['VALOR_TOTAL', 'VALOR'],               'VALOR')
    c_nome  = get_col(['NOME_EMISSOR', 'NOME_CIA', 'NOME'],   'NOME')
except ValueError as e:
    print(f"\nERRO DE MAPEAMENTO: {e}")
    raise

print(f"Mapeamento identificado: {c_cnpj}, {c_tipo}, {c_data}, {c_valor}, {c_nome}")

df_acoes = df_raw[
    df_raw[c_tipo].str.contains('A.*[CÇ]', na=False, case=False, regex=True) &
    df_raw[c_tipo].str.contains('O', na=False, case=False)
].copy()

df_acoes[c_data] = pd.to_datetime(df_acoes[c_data], errors='coerce')
df_acoes = df_acoes.dropna(subset=[c_cnpj, c_data])
df_acoes = df_acoes.sort_values(by=[c_cnpj, c_data])

df_acoes['rank'] = df_acoes.groupby(c_cnpj).cumcount() + 1

ipo_base = df_acoes[
    (ipo_base = df_acoes[df_acoes['rank'] == 1])
].copy()
ipo_base = ipo_base[[c_cnpj, c_nome, c_data, c_valor]]
ipo_base.columns = ['cnpj_cia', 'nome_empresarial', 'data_registro_ipo', 'valor_total_ipo']

last_follow = df_acoes[df_acoes['rank'] > 1].groupby(c_cnpj).last().reset_index()
if not last_follow.empty:
    last_follow = last_follow[[c_cnpj, c_data, c_valor]]
    last_follow.columns = ['cnpj_cia', 'data_registro_reoferta', 'valor_total_reoferta']
    final_df = pd.merge(ipo_base, last_follow, on='cnpj_cia', how='left')
else:
    final_df = ipo_base.copy()
    final_df['data_registro_reoferta'] = None
    final_df['valor_total_reoferta'] = None

df_status = pd.read_sql(
    "SELECT cnpj_cia, situacao_registro as status_registro FROM public.empresas",
    engine
)
final_df = pd.merge(final_df, df_status, on='cnpj_cia', how='left')

final_df.to_sql('ipo_master', engine, if_exists='replace', index=False)
print(f"Sucesso! Tabela ipo_master populada com {len(final_df)} registros.")

# ===== DIAGNÓSTICO - remova após confirmar =====
print("\n--- Valores únicos em TIPO_ATIVO ---")
print(df_raw['TIPO_ATIVO'].value_counts().head(30))

print("\n--- Valores únicos em TIPO_OFERTA ---")
print(df_raw['TIPO_OFERTA'].value_counts().head(20))

print("\n--- Valores únicos em CLASSE_ATIVO ---")
print(df_raw['CLASSE_ATIVO'].value_counts().head(20) if 'CLASSE_ATIVO' in df_raw.columns else "coluna inexistente")
# ===============================================

# ===== DIAGNÓSTICO =====
print(f"\nTotal linhas df_raw: {len(df_raw)}")
print(f"Total após filtro AÇÕES: {len(df_acoes)}")
print("\nAmostra TIPO_ATIVO filtrado:")
print(df_acoes[c_tipo].value_counts())
print(f"\nTotal rank==1: {len(df_acoes[df_acoes['rank']==1])}")
print(f"Total rank==1 e ano>=2010: {len(ipo_base)}")
print(f"\nAmostra de datas (c_data):")
print(df_acoes[c_data].dt.year.value_counts().sort_index())
# =======================