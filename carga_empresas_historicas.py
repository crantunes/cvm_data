import pandas as pd
import requests
import zipfile
import io
from sqlalchemy import create_engine, text

# Configuração da conexão
DB_URL = "postgresql://pesquisador:sua_senha_forte@localhost:5432/cvm_data"
engine = create_engine(DB_URL)

def popular_empresas_historicas(anos):
    for ano in anos:
        url = f"https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/FCA/DADOS/fca_cia_aberta_{ano}.zip"
        print(f"\n--- Buscando Cadastro de Empresas: Ano {ano} ---")
        
        try:
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                # Localiza o arquivo 'geral' dentro do ZIP (o nome pode variar levemente por ano)
                file_name = [f for f in z.namelist() if 'geral' in f.lower()][0]
                
                with z.open(file_name) as f:
                    df = pd.read_csv(f, sep=';', encoding='ISO-8859-1')
                    df.columns = [c.strip().lower() for c in df.columns]
                    
                    # Mapeamento Resiliente (Trata variações de nomes de colunas ao longo dos anos)
                    df_db = pd.DataFrame()
                    df_db['cnpj_cia'] = df.get('cnpj_companhia', df.get('cnpj_cia'))
                    df_db['denom_social'] = df.get('nome_empresarial', df.get('denom_social'))
                    df_db['denom_comerc'] = df.get('nome_empresarial', df.get('denom_comerc'))
                    df_db['setor_ativ'] = df.get('setor_atividade', df.get('setor_ativ'))
                    df_db['data_reg_cvm'] = pd.to_datetime(df.get('data_registro_cvm', df.get('dt_reg_cvm')), errors='coerce')
                    df_db['situacao_registro'] = df.get('situacao_registro_cvm', df.get('sit_reg'))
                    
                    # Limpeza básica
                    df_db = df_db.dropna(subset=['cnpj_cia'])
                    df_db = df_db.drop_duplicates(subset=['cnpj_cia'])
                    
                    print(f"   -> Processando {len(df_db)} empresas potenciais...")

                    # Inserção com tratamento de conflito (UPSERT "leve")
                    # Criamos uma tabela temporária para fazer o merge e evitar erro de PK
                    df_db.to_sql('temp_empresas', engine, if_exists='replace', index=False)
                    
                    with engine.begin() as conn:
                        query = text("""
                            INSERT INTO empresas (cnpj_cia, denom_social, denom_comerc, setor_ativ, data_reg_cvm, situacao_registro)
                            SELECT cnpj_cia, denom_social, denom_comerc, setor_ativ, data_reg_cvm, situacao_registro
                            FROM temp_empresas
                            ON CONFLICT (cnpj_cia) DO NOTHING;
                        """)
                        conn.execute(query)
                        
            print(f"   OK: Ano {ano} integrado ao cadastro mestre.")
            
        except Exception as e:
            print(f"   Erro no ano {ano}: {e}")

if __name__ == "__main__":
    # Range histórico para cobrir todas as empresas que passaram pela CVM
    anos_historicos = range(2010, 2025)
    popular_empresas_historicas(anos_historicos)