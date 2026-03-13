import pandas as pd
import requests
import zipfile
import io
from sqlalchemy import create_engine

# Configuração da conexão
DB_URL = "postgresql://pesquisador:sua_senha_forte@localhost:5432/cvm_data"
engine = create_engine(DB_URL)

def popular_tabela_empresas():
    url = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/FCA/DADOS/fca_cia_aberta_2025.zip"
    print("--- Baixando dados cadastrais da CVM (FCA 2025) ---")
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            file_name = "fca_cia_aberta_geral_2025.csv"
            
            with z.open(file_name) as f:
                # Lendo com separador ';' e tratando encoding
                df = pd.read_csv(f, sep=';', encoding='ISO-8859-1')
                
                # TRUQUE TÉCNICO: Normaliza os nomes das colunas para minúsculas
                df.columns = [c.strip().lower() for c in df.columns]
                
                # Mapeamento com os nomes REAIS que apareceram no seu terminal
                df_db = pd.DataFrame()
                df_db['cnpj_cia'] = df['cnpj_companhia'] # Mudou de cnpj_cia para cnpj_companhia
                df_db['denom_social'] = df['nome_empresarial'] # Mudou de denom_social para nome_empresarial
                df_db['denom_comerc'] = df['nome_empresarial'] # Usando o mesmo como fallback
                df_db['setor_ativ'] = df['setor_atividade'] # Mudou de setor_ativ para setor_atividade
                df_db['data_reg_cvm'] = pd.to_datetime(df['data_registro_cvm'], errors='coerce')
                df_db['situacao_registro'] = df['situacao_registro_cvm'] # Mudou para situacao_registro_cvm
                
                # Limpeza de duplicatas para não quebrar a Primary Key do Postgres
                df_db = df_db.drop_duplicates(subset=['cnpj_cia'])
                
                print(f"Encontradas {len(df_db)} empresas únicas. Carregando no Postgres...")
                
                df_db.to_sql('empresas', engine, if_exists='append', index=False, method='multi', chunksize=5000)
                
        print("Sucesso! Tabela 'empresas' populada.")
        
    except Exception as e:
        print(f"Erro corrigido na carga: {e}")
        # Se o erro persistir, vamos imprimir as colunas para depurar:
        if 'df' in locals():
            print(f"Colunas encontradas no CSV: {df.columns.tolist()}")

if __name__ == "__main__":
    popular_tabela_empresas()